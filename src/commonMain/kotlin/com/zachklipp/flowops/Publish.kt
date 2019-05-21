@file:Suppress("BooleanLiteralArgument")

package com.zachklipp.flowops

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/**
 * Broadcasts this [Flow].
 *
 * The returned [Flow] is a [FlowTap], which means it won't collect from
 * the upstream [Flow] until it is [connected][FlowTap.connectIn]. After being connected, it will
 * collect only once from the upstream [Flow] for every downstream collector. Every element emitted by
 * the upstream [Flow] will be emitted to each downstream [Flow]. This is similar to [BroadcastChannel], but
 * won't start broadcasting until it is explicitly connected. The returned [FlowTap] can also be
 * disconnected by cancelling the connector coroutine, which will stop collecting from upstream. Disconnecting
 * will not cause downstream [Flow]s to complete.
 *
 * @param downstreamBufferSize The capacity of the buffers used to send elements to downstream flows. If any
 * downstream collector is slow and exerts backpressure by letting its buffer fill up, the backpressure will
 * be propagated to the upstream [Flow]. Use a larger value here to prevent that from happening, or a smaller
 * value to increase backpressure sensitivity.
 *
 * @see replayMostRecent
 */
@FlowPreview
fun <T> Flow<T>.publish(downstreamBufferSize: Int = 16): FlowTap<T> =
    PublishFlowTap(this, downstreamBufferSize)

@FlowPreview
internal open class PublishFlowTap<T>(
    private val upstream: Flow<T>,
    private val downstreamBufferSize: Int
) : FlowTap<T>, Flow<T> {

    private var isConnected = false

    /**
     * Set to true when the upstream has either completed successfully or failed, to indicate to
     * any subsequent collectors that they should immediately get the completion signal.
     * Guarded by [lock].
     */
    private var isComplete = false
    private var failureCause: Throwable? = null

    /**
     * Guards both [isComplete], [downstreamFlows], and [replayBuffer].
     */
    private val lock = Mutex()

    /**
     * Use a copy-on-write list so snapshotting is fast and doesn't require copying the whole list.
     * Note that this list is guarded by [lock] so it doesn't _need_ to be thread-safe, although
     * platform-specific implementations may be.
     */
    private var downstreamFlows: Set<SendChannel<T>> = emptySet()

    override val flow: Flow<T> get() = this

    /**
     * Called whenever the upstream [Flow] emits a new element.
     * Invoked from within a critical section exclusive with all the other `*Synchronized` methods.
     */
    protected open fun onNewElementFromUpstreamSynchronized(element: T) {}

    /**
     * Called when the connection coroutine is cancelled.
     */
    protected open fun onDisconnectedSynchronized() {}

    /**
     * Called when from [collect] to allow custom work to be done to initialize the [FlowCollector].
     * This method will be invoked from within a critical section exclusive with all the other `*Synchronized`
     * methods, and so should not do anything suspending. Any asynchronous work should be done in the
     * returned function, which will be invoked _outside_ the critical section.
     * */
    protected open fun prepareForNewCollectorSynchronized(collector: FlowCollector<T>): (suspend () -> Unit)? = null

    /**
     * Called when the upstream [Flow] is completed, either successfully or with a failure.
     * Invoked from within a critical section exclusive with all the other `*Synchronized` methods.
     */
    protected open fun onCompletedSynchronized() {}

    final override suspend fun connectAndJoin() {
        lock.withLock {
            check(!isConnected) { "ConnectableFlow is already connected." }
            isConnected = true
        }

        try {
            forwardUpstreamItemsToDownstream()
            completeDownstream(null)
        } catch (e: CancellationException) {
            withContext(NonCancellable) {
                // Disconnect so we can re-connect upstream after completion is handled.
                check(isConnected)
                isConnected = false
                onDisconnectedSynchronized()
            }
            // Don't propagate cancellation downstream, since we may be re-connected later.
            throw e
        } catch (e: Throwable) {
            // Actual errors should go downstream.
            completeDownstream(e)
            throw e
        }
    }

    final override suspend fun collect(collector: FlowCollector<T>) {
        val channel = Channel<T>(capacity = downstreamBufferSize)
        val initializer = lock.withLock {
            // The upstream already completed, we should immediately notify downstream.
            // If the completion happens any time after leaving this critical section,
            // downstream will be notified through the channel.
            if (isComplete) {
                failureCause?.let { throw it } ?: return
            }

            downstreamFlows = downstreamFlows + channel
            return@withLock prepareForNewCollectorSynchronized(collector)
        }

        try {
            @Suppress("EXPERIMENTAL_API_USAGE")
            channel.consume {
                // First emit the cached element…
                initializer?.invoke()

                // …and then forward everything from upstream until completion.
                for (element in channel) {
                    collector.emit(element)
                }
            }
        } catch (e: Throwable) {
            // The downstream is no longer collecting.
            // If there's no contention on the lock, we can help out and remove our channel from the list.
            // If the channel closes normally, it meant upstream completed, and the list will be cleared
            // all at once in completeDownstream so we don't need to help.
            if (lock.tryLock()) {
                try {
                    removeCancelledFlows()
                } finally {
                    lock.unlock()
                }
            }
            throw e
        }
    }

    private suspend fun forwardUpstreamItemsToDownstream() {
        upstream.collect { element ->
            // Keep this critical section small. Just get a snapshot of the list of downstream flows and
            // save the last item.
            val flowsSnapshot = lock.withLock {
                onNewElementFromUpstreamSynchronized(element)
                downstreamFlows
            }

            var somethingWasCancelled = false
            flowsSnapshot.forEach { downstream ->
                try {
                    downstream.send(element)
                } catch (e: ClosedSendChannelException) {
                    // This at least one downstream flow was cancelled, so we should clean them up later.
                    somethingWasCancelled = true
                }
            }

            if (somethingWasCancelled) {
                // Help remove cancelled flows from the list.
                lock.withLock {
                    removeCancelledFlows()
                }
            }
        }
    }

    /**
     * Removes all channels from [downstreamFlows] that are closed.
     * Assumes the caller has acquired [lock].
     */
    private fun removeCancelledFlows() {
        downstreamFlows = downstreamFlows.filterNotTo(HashSet()) {
            @Suppress("EXPERIMENTAL_API_USAGE")
            it.isClosedForSend
        }
    }

    /**
     * Marks this instance as "complete" (any future collectors will immediately complete),
     * and signals completion (either success or failure) to all currently-subscribed flows
     * in [downstreamFlows]. Clears state as well so the last value is no longer cached and
     * [downstreamFlows] is cleared.
     */
    private suspend fun completeDownstream(cause: Throwable?) {
        withContext(NonCancellable) {
            val flowsToClose = lock.withLock {
                check(!isComplete)
                isComplete = true
                failureCause = cause

                onCompletedSynchronized()

                // Return the snapshot of flows to outside the critical section and also
                // clear the property.
                return@withLock downstreamFlows.also {
                    downstreamFlows = emptySet()
                }
            }
            flowsToClose.forEach { downstream ->
                downstream.close(cause)
            }
        }
    }
}
