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
package org.apache.hyracks.control.common.work;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkQueue {
    private static final Logger LOGGER = LogManager.getLogger();
    //to be fixed when application vs. hyracks log level issues are sorted
    private static final boolean DEBUG = false;

    private final LinkedBlockingQueue<AbstractWork> queue;
    private final WorkerThread thread;
    private boolean stopped;
    private AtomicInteger enqueueCount;
    private AtomicInteger dequeueCount;
    private int threadPriority = Thread.MAX_PRIORITY;
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    public WorkQueue(String id, int threadPriority) {
        if (threadPriority != Thread.MAX_PRIORITY && threadPriority != Thread.NORM_PRIORITY
                && threadPriority != Thread.MIN_PRIORITY) {
            throw new IllegalArgumentException("Illegal thread priority number.");
        }
        this.threadPriority = threadPriority;
        queue = new LinkedBlockingQueue<>();
        thread = new WorkerThread(id);
        stopped = true;
        if (DEBUG) {
            enqueueCount = new AtomicInteger(0);
            dequeueCount = new AtomicInteger(0);
        }
    }

    public void start() throws HyracksException {
        if (DEBUG) {
            enqueueCount.set(0);
            dequeueCount.set(0);
        }
        stopped = false;
        thread.start();
    }

    public void stop() throws HyracksException {
        synchronized (this) {
            stopped = true;
        }
        thread.interrupt();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksException.create(e);
        }
    }

    public void schedule(AbstractWork event) {
        if (DEBUG) {
            LOGGER.log(Level.DEBUG, "Enqueue (" + hashCode() + "): " + enqueueCount.incrementAndGet());
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Scheduling: " + event);
        }
        queue.offer(event);
    }

    public void scheduleAndSync(SynchronizableWork sRunnable) throws Exception {
        schedule(sRunnable);
        sRunnable.sync();
    }

    private class WorkerThread extends Thread {
        WorkerThread(String id) {
            setName("Worker:" + id);
            setDaemon(true);
            setPriority(threadPriority);
        }

        @Override
        public void run() {
            AbstractWork r;
            while (true) {
                synchronized (WorkQueue.this) {
                    if (stopped) {
                        return;
                    }
                }
                try {
                    r = queue.take();
                } catch (InterruptedException e) { // NOSONAR: aborting the thread
                    break;
                }
                if (DEBUG) {
                    LOGGER.log(Level.TRACE, "Dequeue (" + WorkQueue.this.hashCode() + "): "
                            + dequeueCount.incrementAndGet() + "/" + enqueueCount);
                }
                if (LOGGER.isEnabled(r.logLevel())) {
                    LOGGER.log(r.logLevel(), "Executing: " + r);
                }
                ThreadInfo before = threadMXBean.getThreadInfo(thread.getId());
                try {
                    r.run();
                } catch (Exception e) {
                    LOGGER.log(Level.WARN, "Exception while executing " + r, e);
                } finally {
                    if (LOGGER.isTraceEnabled()) {
                        traceWaitsAndBlocks(r, before);
                    }
                }
            }
        }

        protected void traceWaitsAndBlocks(AbstractWork r, ThreadInfo before) {
            ThreadInfo after = threadMXBean.getThreadInfo(thread.getId());
            final long waitedDelta = after.getWaitedCount() - before.getWaitedCount();
            final long blockedDelta = after.getBlockedCount() - before.getBlockedCount();
            if (waitedDelta > 0 || blockedDelta > 0) {
                LOGGER.trace("Work {} waited {} times (~{}ms), blocked {} times (~{}ms)", r, waitedDelta,
                        after.getWaitedTime() - before.getWaitedTime(), blockedDelta,
                        after.getBlockedTime() - before.getBlockedTime());
            }
        }
    }
}
