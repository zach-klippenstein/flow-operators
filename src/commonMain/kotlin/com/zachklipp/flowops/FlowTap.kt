package com.zachklipp.flowops

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch

/**
 * Represents a [Flow] that won't collect the upstream flow until it is explicitly [connected][connectAndJoin].
 */
@FlowPreview
interface FlowTap<out T> {

    /**
     * The actual [Flow] instance that won't start emitting until this tap is [connected][connectAndJoin].
     */
    val flow: Flow<T>

    /**
     * Connects to the upstream [Flow] and suspends until it completes.
     *
     * @see connectIn
     */
    suspend fun connectAndJoin()

    /**
     * Connects to the upstream [Flow] and returns a [Job] that represents the connection.
     *
     * Cancelling the [Job] will cancel the upstream [Flow].
     *
     * @see connectAndJoin
     */
    fun connectIn(scope: CoroutineScope): Job = scope.launch {
        this@FlowTap.connectAndJoin()
    }
}
