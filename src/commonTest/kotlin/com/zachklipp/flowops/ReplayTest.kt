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
    fun `doesn't emit until connected`() = runTest {
        val source = flowOf(0)
        val transformed = source.replayMostRecent()
        val collector = transformed.collectIn(this)

        yield()
        assertNull(collector.poll())

        // Don't leak the coroutine.
        collector.cancel()
    }

    @Test
    fun `emits after connected`() = runTest {
        val source = flowOf(0)
        val transformed = source.replayMostRecent()
        val collector = transformed.collectIn(this)
        val connection = transformed.connectIn(this)

        assertEquals(0, collector.receive())

        // Don't leak.
        connection.cancel()
    }

    @Test
    fun `replays to late collectors`() = runTest {
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
    fun `stops replaying after completion`() = runTest {
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
    fun `drops stale elements`() = runTest {
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
    fun `emits new elements after replaying`() = runTest {
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
    fun `stops emitting after disconnected`() = runTest {
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
    fun `cache preserved between disconnections`() = runTest {
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
    fun `cache discarded between disconnections`() = runTest {
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
