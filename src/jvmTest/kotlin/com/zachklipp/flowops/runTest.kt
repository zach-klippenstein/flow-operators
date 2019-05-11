@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.zachklipp.flowops

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.test.runBlockingTest

/**
 * TODO write documentation.
 */
actual fun runTest(block: suspend CoroutineScope.() -> Unit) {
    runBlockingTest(testBody = block)
}
