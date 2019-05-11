package com.zachklipp.flowops

import kotlinx.coroutines.CoroutineScope

/**
 * TODO write documentation.
 */
expect fun runTest(block: suspend CoroutineScope.() -> Unit)
