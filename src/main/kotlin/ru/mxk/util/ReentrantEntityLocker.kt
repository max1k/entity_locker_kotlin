package ru.mxk.util

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

class ReentrantEntityLocker<T : Any> : EntityLocker<T> {
    private val lockByEntityId = ConcurrentHashMap<T, Lock>()
    private val cleanUpLock: ReadWriteLock = ReentrantReadWriteLock()

    override fun lockAndRun(entityId: T, runnable: () -> Unit) {
        runWithLock(entityId, null, runnable)
    }

    override fun lockAndRun(entityId: T, timeout: Duration, runnable: () -> Unit): Boolean {
        return runWithLock(entityId, timeout, runnable)
    }

    private fun runWithLock(entityId: T, timeout: Duration?, runnable: () -> Unit): Boolean {
        var cleanup = true

        try {
            cleanUpLock.readLock().withLock {
                cleanup = !lockByEntityId.containsKey(entityId)
                return runExclusive(entityId, timeout, runnable)
            }
        } finally {
            if (cleanup) {
                cleanUpLock.writeLock().withLock {
                    lockByEntityId.remove(entityId)
                }
            }
        }
    }

    private fun runExclusive(entityId: T, timeout: Duration?, runnable: () -> Unit): Boolean {
        val entityLock: Lock = lockByEntityId.getOrPut(entityId) { ReentrantLock() }

        if (timeout == null) {
            entityLock.withLock(runnable)
            return true
        }

        val locked = entityLock.tryLock(timeout.toMillis(), TimeUnit.MILLISECONDS)

        if (locked) {
            try {
                runnable()
            } finally {
                entityLock.unlock()
            }
        }

        return locked
    }

    fun isClean(): Boolean {
        return lockByEntityId.isEmpty()
    }

}