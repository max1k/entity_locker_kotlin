package ru.mxk.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

internal class ReentrantEntityLockerTest {
    private val DEFAULT_SLEEP_TIME_MS = Duration.ofMillis(1000)
    private val THREAD_LIMIT = 1000
    private val FIRST_ENTITY_ID = 1

    private val EXPECTED_LIST = (1 .. THREAD_LIMIT).toList()

    private fun startThreads(latch: CountDownLatch, threadRunnable: () -> Unit) {
        val threads = (1..THREAD_LIMIT)
            .map { Thread(threadRunnable) }
            .onEach { obj: Thread -> obj.start() }

        latch.countDown()

        threads.forEach { it.join() }
    }

    @Test
    fun testAtMostOneExecutionOnSameEntity() {
        val latch = CountDownLatch(1)
        val mutualList: MutableList<Int> = mutableListOf()
        val entityLocker = ReentrantEntityLocker<Int>()

        startThreads(latch) {
            latch.await()
            entityLocker.lockAndRun(FIRST_ENTITY_ID) { mutualList.add(mutualList.size + 1) }
        }

        assertTrue(entityLocker.isClean())
        assertEquals(EXPECTED_LIST, mutualList)
    }

    @Test
    fun testReentrantLocking() {
        val latch = CountDownLatch(1)
        val firstMutualList= mutableListOf<Int>()
        val secondMutualList= mutableListOf<Int>()
        val entityLocker = ReentrantEntityLocker<Int>()

        startThreads(latch) {
            latch.await()
            entityLocker.lockAndRun(FIRST_ENTITY_ID) {
                firstMutualList.add(firstMutualList.size + 1)
                entityLocker.lockAndRun(FIRST_ENTITY_ID) {
                    secondMutualList.add(secondMutualList.size + 1)
                }
            }
        }

        assertTrue(entityLocker.isClean())
        assertEquals(firstMutualList, secondMutualList)
    }

    @Test
    fun testConcurrentExecutionOnDifferentEntities() {
        val latch = CountDownLatch(1)
        val entityLocker = ReentrantEntityLocker<UUID>()
        val counter = AtomicInteger()
        val startTime: Instant = Instant.now()

        startThreads(latch) {
            latch.await()
            entityLocker.lockAndRun(UUID.randomUUID()) {
                counter.incrementAndGet()
                Thread.sleep(DEFAULT_SLEEP_TIME_MS.toMillis())
            }
        }

        val runDuration = Duration.between(startTime, Instant.now())

        assertTrue(entityLocker.isClean())
        assertEquals(THREAD_LIMIT, counter.get())
        assertTrue(runDuration >= DEFAULT_SLEEP_TIME_MS)
        assertTrue(runDuration <  DEFAULT_SLEEP_TIME_MS.multipliedBy(THREAD_LIMIT.toLong()))
    }

    @Test
    fun testLockWithTimeoutWaitingTimeElapses() {
        val latch = CountDownLatch(1)
        val counter = AtomicInteger(0)
        val protectedCodeExecutions: MutableList<Boolean> = CopyOnWriteArrayList()
        val entityLocker = ReentrantEntityLocker<Int>()

        startThreads(latch) {
            latch.await()
            val executed: Boolean =
                entityLocker.lockAndRun(FIRST_ENTITY_ID, DEFAULT_SLEEP_TIME_MS.dividedBy(2)) {
                    counter.incrementAndGet()
                    Thread.sleep(DEFAULT_SLEEP_TIME_MS.toMillis())
                }

            protectedCodeExecutions.add(executed)
        }


        assertTrue(entityLocker.isClean())
        assertEquals(1, counter.get())

        assertEquals(THREAD_LIMIT, protectedCodeExecutions.size)
        val executionsCount = protectedCodeExecutions.count { it }
        assertEquals(1, executionsCount)
    }

    @Test
    fun testAtMostOneExecutionOnGlobalLock() {
        val latch = CountDownLatch(1)
        val mutualList: MutableList<Int> = mutableListOf()

        startThreads(latch) {
            latch.await()
            EntityLocker.runGlobal { mutualList.add(mutualList.size + 1) }
        }

        assertEquals(EXPECTED_LIST, mutualList)
    }

}