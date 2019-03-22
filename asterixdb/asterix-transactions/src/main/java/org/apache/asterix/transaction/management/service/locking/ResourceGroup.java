/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.transaction.management.service.locking;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.asterix.common.transactions.ITransactionContext;

/**
 * A ResourceGroup represents a group of resources that are manged by a ConcurrentLockManager.
 * All resources in a group share a common latch. I.e. all modifications of lock requests for any resource in a group
 * are protected by the same latch.
 *
 * @see ConcurrentLockManager
 */
class ResourceGroup {
    private ReentrantReadWriteLock latch;
    private Condition condition;
    AtomicLong firstResourceIndex;

    ResourceGroup() {
        latch = new ReentrantReadWriteLock();
        condition = latch.writeLock().newCondition();
        firstResourceIndex = new AtomicLong(-1);
    }

    void getLatch() {
        log("latch");
        latch.writeLock().lock();
    }

    boolean tryLatch(long timeout, TimeUnit unit) throws InterruptedException {
        log("tryLatch");
        try {
            return latch.writeLock().tryLock(timeout, unit);
        } catch (InterruptedException e) {
            ConcurrentLockManager.LOGGER.trace("interrupted while wating on ResourceGroup");
            throw e;
        }
    }

    void releaseLatch() {
        log("release");
        latch.writeLock().unlock();
    }

    boolean hasWaiters() {
        return latch.hasQueuedThreads();
    }

    void await(ITransactionContext txnContext) throws InterruptedException {
        log("wait for");
        try {
            condition.await();
        } catch (InterruptedException e) {
            ConcurrentLockManager.LOGGER.trace("interrupted while waiting on ResourceGroup");
            throw e;
        }
    }

    void wakeUp() {
        log("notify");
        condition.signalAll();
    }

    void log(String s) {
        if (ConcurrentLockManager.LOGGER.isEnabled(ConcurrentLockManager.LVL)) {
            ConcurrentLockManager.LOGGER.log(ConcurrentLockManager.LVL, s + " " + toString());
        }
    }

    public String toString() {
        return "{ id : " + hashCode() + ", first : " + TypeUtil.Global.toString(firstResourceIndex.get()) + ", "
                + "waiters : " + (hasWaiters() ? "true" : "false") + " }";
    }
}
