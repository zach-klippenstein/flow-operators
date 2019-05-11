package com.zachklipp.flowops

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * Returns a [Flow] that will consume this [ReceiveChannel]. Note that channels are not broadcast
 * so if the returned [Flow] is collected by multiple collectors concurrently, they will receive different
 * values. See [ReceiveChannel.replayMostRecentAsFlow] for a more manageable way to create [Flow]s.
 */
@FlowPreview
@Suppress("EXPERIMENTAL_API_USAGE", "NOTHING_TO_INLINE")
internal inline fun <T> ReceiveChannel<T>.consumeAsFlow(): Flow<T> = flow { consumeEach { emit(it) } }
