@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.zachklipp.flowops

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.yield
import kotlin.test.*

class PublishTest {

    @Test
    fun `doesn't emit until connected`() = runTest {
        val source = flowOf(0)
        val transformed = source.publish()
        val collector = transformed.collectIn(this)

        yield()
        assertNull(collector.poll())

        // Don't leak the coroutine.
        collector.cancel()
    }

    @Test
    fun `emits after connected`() = runTest {
        val source = flowOf(0)
        val transformed = source.publish()
        val collector = transformed.collectIn(this)
        val connection = transformed.connectIn(this)

        assertEquals(0, collector.receive())

        // Don't leak.
        connection.cancel()
    }

    @Test
    fun `emits new elements after connecting`() = runTest {
        val emitted = CompletableDeferred<Unit>()
        val emitNext = CompletableDeferred<Unit>()
        val source = flow {
            emit(0)
            emitted.complete(Unit)
            emitNext.await()
            emit(1)
        }
        val transformed = source.publish()
        val connection = transformed.connectIn(this)

        emitted.await()

        val collector = transformed.collectIn(this)
        assertNull(collector.poll())

        emitNext.complete(Unit)
        assertEquals(1, collector.receive())

        // Don't leak.
        collector.cancel()
        connection.cancel()
    }

    @Test
    fun `stops emitting after disconnected`() = runTest {
        val source = Channel<Int>(capacity = 0)
        val transformed = source.consumeAsFlow()
            .publish()
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
}
