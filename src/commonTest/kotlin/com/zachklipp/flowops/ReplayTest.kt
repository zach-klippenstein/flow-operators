@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.zachklipp.flowops

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.yield
import kotlin.test.*

class ReplayTest {

    @Test
    fun doesntEmitUntilConnected() = runTest {
        val source = flowOf(0)
        val transformed = source.replayMostRecent()
        val collector = transformed.collectIn(this)

        yield()
        assertNull(collector.poll())

        // Don't leak the coroutine.
        collector.cancel()
    }

    @Test
    fun emitsAfterConnected() = runTest {
        val source = flowOf(0)
        val transformed = source.replayMostRecent()
        val collector = transformed.collectIn(this)
        val connection = transformed.connectIn(this)

        assertEquals(0, collector.receive())

        // Don't leak.
        connection.cancel()
    }

    @Test
    fun replaysToLateCollectors() = runTest {
        val emitted = CompletableDeferred<Unit>()
        val source = flow {
            emit(0)
            emitted.complete(Unit)
            suspendCancellableCoroutine<Nothing> { }
        }
        val transformed = source.replayMostRecent()
        val connection = transformed.connectIn(this)

        emitted.await()

        val collector = transformed.collectIn(this)
        assertEquals(0, collector.receive())

        // Don't leak.
        collector.cancel()
        connection.cancel()
    }

    @Test
    fun stopsReplayingAfterCompletion() = runTest {
        val emitted = CompletableDeferred<Unit>()
        val source = flow {
            emit(0)
            emitted.complete(Unit)
        }
        val transformed = source.replayMostRecent()
        val connection = transformed.connectIn(this)

        emitted.await()

        transformed.collect {
            fail("Expected no elements.")
        }
        // Success!

        // Don't leak.
        connection.cancel()
    }

    @Test
    fun dropsStaleElements() = runTest {
        val emitted = CompletableDeferred<Unit>()
        val source = flow {
            emit(0)
            emit(1)
            emitted.complete(Unit)
            suspendCancellableCoroutine<Nothing> { }
        }
        val transformed = source.replayMostRecent()
        val connection = transformed.connectIn(this)

        emitted.await()

        val collector = transformed.collectIn(this)
        assertEquals(1, collector.receive())

        // Don't leak.
        collector.cancel()
        connection.cancel()
    }

    @Test
    fun emitsNewElementsAfterReplaying() = runTest {
        val emitted = CompletableDeferred<Unit>()
        val emitNext = CompletableDeferred<Unit>()
        val source = flow {
            emit(0)
            emitted.complete(Unit)
            emitNext.await()
            emit(1)
        }
        val transformed = source.replayMostRecent()
        val connection = transformed.connectIn(this)

        emitted.await()

        val collector = transformed.collectIn(this)
        assertEquals(0, collector.receive())

        emitNext.complete(Unit)
        assertEquals(1, collector.receive())

        // Don't leak.
        collector.cancel()
        connection.cancel()
    }

    @Test
    fun stopsEmittingAfterDisconnected() = runTest {
        val source = Channel<Int>(capacity = 0)
        val transformed = source.replayMostRecentAsFlow()
        val connection = transformed.connectIn(this)
        val collector = transformed.collectIn(this)

        assertNull(collector.poll())
        assertTrue(source.offer(0))
        assertEquals(0, collector.receive())

        connection.cancel()

        assertFailsWith<CancellationException> { source.offer(1) }
        assertNull(collector.poll())

        // Don't leak.
        collector.cancel()
    }

    @Test
    fun cachePreservedBetweenDisconnections() = runTest {
        val source = Channel<Int>(capacity = 0)
        val transformed = source.replayMostRecentAsFlow()
        val connection = transformed.connectIn(this)

        source.send(0)
        connection.cancel()

        val collector = transformed.collectIn(this)

        assertEquals(0, collector.receive())

        // Don't leak.
        collector.cancel()
    }

    @Test
    fun cacheDiscardedBetweenDisconnections() = runTest {
        val source = Channel<Int>(capacity = 0)
        val transformed = source.replayMostRecentAsFlow(preserveCacheBetweenConnections = false)
        val connection = transformed.connectIn(this)

        source.send(0)
        connection.cancel()

        val collector = transformed.collectIn(this)

        assertNull(collector.poll())

        // Don't leak.
        collector.cancel()
    }
}
