@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.zachklipp.flowops

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineStart.UNDISPATCHED
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

class ConsumeAsFlowTest {

    @Test
    fun throwsWhenCollectedConcurrently() = runTest {
        val source = Channel<Unit>(1).apply { send(Unit) }
        val flow = source.consumeAsFlow()
        val collecting = CompletableDeferred<Unit>()

        val collectorJob = launch(CoroutineName("collector"), start = UNDISPATCHED) {
            flow.collect {
                collecting.complete(Unit)
            }
        }

        collecting.await()

        // TODO(zachklipp) Use assertFailsWith. See https://youtrack.jetbrains.com/issue/KT-31194
        try {
            flow.collect { }
            fail("Expected exception to be thrown.")
        } catch (e: IllegalStateException) {
            assertTrue("consumeAsFlow is already being collected in" in e.message!!)
            // Message should include coroutine context.
            assertTrue("collector" in e.message!!)
            collectorJob.cancel()
        }
    }
}
