package com.zachklipp.flowops

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import java.util.concurrent.atomic.AtomicLong

/**
 * Returns a [Flow] that is useful for representing a "property" that always immediately emits its
 * current value on collection, and then emits any updates to that value.
 *
 * The returned [Flow] will:
 *
 *  1. **Multicast:** Collect the upstream (receiver) [Flow] only once for all downstream collectors.
 *
 *  2. **Cache:** Replay the most recently-emitted item to each new collector.
 *     Note that the first value will only be cached after the first downstream collector starts collecting.
 *     Values emitted while no downstream collectors are active will not be cached.
 *
 *  3. **Absorb backpressure:** Slow downstream collectors will never slow down the upstream [Flow].
 *
 * Alias for [replayMostRecent] with a downstream channel capacity of [Channel.CONFLATED] (to avoid
 * backpressure) and connected with [refCountIn].
 *
 * @param connectionScope The scope used to collect the upstream (receiver) [Flow].
 */
@FlowPreview
fun <T> Flow<T>.asPropertyFlow(connectionScope: CoroutineScope): Flow<T> =
    replayMostRecent(downstreamBufferSize = Channel.CONFLATED)
        .refCountIn(connectionScope)

/**
 * [Connects][ConnectableFlow.connectAndJoin] to this [ConnectableFlow] when the first downstream collector
 * begins collecting, and disconnects once the last downstream collector stops collecting.
 *
 * @param connectionScope The scope passed to [ConnectableFlow.connectIn].
 */
@FlowPreview
fun <T> ConnectableFlow<T>.refCountIn(connectionScope: CoroutineScope): Flow<T> {
    // TODO(zachklipp) This should use atomicfu.
    val collectors = AtomicLong(0)
    val lock = Mutex()
    var connection: Job? = null

    return flow {
        if (collectors.getAndIncrement() == 0L) {
            // Need this mutex to avoid race with the previous connector winning the decrement and
            // actually cancelling/joining the previous connection.
            lock.lock()
            connection = connectIn(connectionScope)
        }
        try {
            collect { emit(it) }
        } finally {
            if (collectors.decrementAndGet() == 0L) {
                connection!!.cancelAndJoin()
                connection = null
                lock.unlock()
            }
        }
    }
}
