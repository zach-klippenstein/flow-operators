package com.zachklipp.flowops

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

/**
 * Returns a [Flow] that will consume this [ReceiveChannel]. Note that channels are not broadcast
 * so if the returned [Flow] is collected by multiple collectors concurrently, they will receive different
 * values. See [ReceiveChannel.replayMostRecentAsFlow] for a more manageable way to create [Flow]s.
 *
 * The returned [Flow] will throw an [IllegalStateException] if it is collected concurrently.
 */
@FlowPreview
@Suppress("EXPERIMENTAL_API_USAGE", "NOTHING_TO_INLINE")
internal inline fun <T> ReceiveChannel<T>.consumeAsFlow(): Flow<T> {
    // This should be an atomic but I don't want to deal with atomicfu.
    val lock = Mutex()
    var collectingContext: CoroutineContext? = null
    return flow {
        // This error message's access of collectingContext has a race, but it's just a debug string.
        check(lock.tryLock()) { "consumeAsFlow is already being collected in $collectingContext" }
        collectingContext = coroutineContext
        try {
            consumeEach { emit(it) }
        } finally {
            collectingContext = null
            lock.unlock()
        }
    }
}
