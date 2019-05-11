@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.zachklipp.flowops

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart.UNDISPATCHED
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.launch

/**
 * Alternative to [produceIn] that eagerly starts collecting.
 */
@FlowPreview
internal fun <T> Flow<T>.collectIn(scope: CoroutineScope): ReceiveChannel<T> {
    val channel = Channel<T>(0)
    val collector = scope.launch(start = UNDISPATCHED) {
        collect { channel.send(it) }
    }
    channel.invokeOnClose { collector.cancel() }
    return channel
}
