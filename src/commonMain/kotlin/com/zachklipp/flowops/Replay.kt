package com.zachklipp.flowops

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector

/**
 * Broadcasts this [Flow], immediately emitting the last-seen value to new collectors.
 *
 * The returned [Flow] is a [ConnectableFlow], which means it won't collect from
 * the upstream [Flow] until it is [connected][ConnectableFlow.connectIn]. After being connected, it will
 * collect only once from the upstream [Flow] for every downstream collector. Every element emitted by
 * the upstream [Flow] will be emitted to each downstream [Flow]. This is similar to `BroadcastChannel`, but
 * won't start broadcasting until it is explicitly connected. The returned [ConnectableFlow] can also be
 * disconnected by cancelling the connector coroutine, which will stop collecting from upstream. Disconnecting
 * will not cause downstream [Flow]s to complete.
 *
 * Upstream values will not start getting cached until the [ConnectableFlow] is connected.
 *
 * @param downstreamBufferSize The capacity of the buffers used to send elements to downstream flows. If any
 * downstream collector is slow and exerts backpressure by letting its buffer fill up, the backpressure will
 * be propagated to the upstream [Flow]. Use a larger value here to prevent that from happening, or a smaller
 * value to increase backpressure sensitivity.
 * @param preserveCacheBetweenConnections
 * If true (the default), the last-seen value will be saved after the [ConnectableFlow] is disconnected, and
 * will be immediately emitted to new collectors.
 * If false, the cache will be cleared on disconnection, and new collectors won't receive a value until the
 * [ConnectableFlow] is reconnected and the upstream [Flow] emits an element.
 */
@FlowPreview
fun <T> Flow<T>.replayMostRecent(
    downstreamBufferSize: Int = 16,
    preserveCacheBetweenConnections: Boolean = true
): ConnectableFlow<T> =
    ReplayMostRecentConnectableFlow(this, downstreamBufferSize, preserveCacheBetweenConnections)

/**
 * Consumes this [ReceiveChannel] by broadcasting its elements to downstream collectors, starting with the
 * last-seen value. See [Flow.replayMostRecent] for full documentation.
 */
@FlowPreview
fun <T> ReceiveChannel<T>.replayMostRecentAsFlow(
    downstreamBufferSize: Int = 16,
    preserveCacheBetweenConnections: Boolean = true
): ConnectableFlow<T> = consumeAsFlow().replayMostRecent(downstreamBufferSize, preserveCacheBetweenConnections)

@FlowPreview
private class ReplayMostRecentConnectableFlow<T>(
    upstream: Flow<T>,
    downstreamBufferSize: Int,
    private val preserveCacheBetweenConnections: Boolean
) : PublishConnectableFlow<T>(upstream, downstreamBufferSize) {

    /**
     * Used to distinguish a missing element from a null element in [lastElement].
     */
    private object NullSentinal {
        fun <T> box(value: T): Any? = value ?: this
        fun <T : Any?> unbox(value: Any): T {
            @Suppress("UNCHECKED_CAST")
            return value.takeUnless { it === NullSentinal } as T
        }
    }

    /**
     * Null means no element has been seen yet. Null elements are represented by [NullSentinal].
     */
    private var lastElement: Any? = null

    override fun onNewElementFromUpstreamSynchronized(element: T) {
        lastElement = NullSentinal.box(element)
    }

    override fun onDisconnectedSynchronized() {
        if (!preserveCacheBetweenConnections) {
            lastElement = null
        }
    }

    override fun prepareForNewCollectorSynchronized(collector: FlowCollector<T>): (suspend () -> Unit)? {
        val snapshot = lastElement
        return snapshot?.let {
            { collector.emit(NullSentinal.unbox(snapshot)) }
        }
    }

    override fun onCompletedSynchronized() {
        // No new downstream collectors should get the cached value,
        // since it is pre-upstream-completion.
        lastElement = null
    }
}
