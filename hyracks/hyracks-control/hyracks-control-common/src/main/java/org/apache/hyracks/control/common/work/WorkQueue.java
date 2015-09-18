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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksException;

public class WorkQueue {
    private static final Logger LOGGER = Logger.getLogger(WorkQueue.class.getName());
    private static final Level COUNT_LOGGING_LEVEL = Level.FINEST;

    private final LinkedBlockingQueue<AbstractWork> queue;
    private final WorkerThread thread;
    private final Semaphore stopSemaphore;
    private boolean stopped;
    private AtomicInteger enqueueCount;
    private AtomicInteger dequeueCount;
    private int threadPriority = Thread.MAX_PRIORITY;

    public WorkQueue(int threadPriority) {
        if (threadPriority != Thread.MAX_PRIORITY && threadPriority != Thread.NORM_PRIORITY
                && threadPriority != Thread.MIN_PRIORITY) {
            throw new IllegalArgumentException("Illegal thread priority number.");
        }
        this.threadPriority = threadPriority;
        queue = new LinkedBlockingQueue<AbstractWork>();
        thread = new WorkerThread();
        stopSemaphore = new Semaphore(1);
        stopped = true;
        if (LOGGER.isLoggable(COUNT_LOGGING_LEVEL)) {
            enqueueCount = new AtomicInteger(0);
            dequeueCount = new AtomicInteger(0);
        }
    }

    public void start() throws HyracksException {
        try {
            stopSemaphore.acquire();
        } catch (InterruptedException e) {
            throw new HyracksException(e);
        }
        if (LOGGER.isLoggable(COUNT_LOGGING_LEVEL)) {
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
        schedule(new AbstractWork() {
            @Override
            public void run() {
            }
        });
        try {
            stopSemaphore.acquire();
        } catch (InterruptedException e) {
            throw new HyracksException(e);
        }
    }

    public void schedule(AbstractWork event) {
        if (LOGGER.isLoggable(COUNT_LOGGING_LEVEL)) {
            LOGGER.log(COUNT_LOGGING_LEVEL, "Enqueue (" + hashCode() + "): " + enqueueCount.incrementAndGet());
        }
        if (LOGGER.isLoggable(Level.FINER)) {
            LOGGER.finer("Scheduling: " + event);
        }
        queue.offer(event);
    }

    public void scheduleAndSync(SynchronizableWork sRunnable) throws Exception {
        schedule(sRunnable);
        sRunnable.sync();
    }

    private class WorkerThread extends Thread {
        WorkerThread() {
            setDaemon(true);
            setPriority(threadPriority);
        }

        @Override
        public void run() {
            try {
                AbstractWork r;
                while (true) {
                    synchronized (WorkQueue.this) {
                        if (stopped) {
                            return;
                        }
                    }
                    try {
                        r = queue.take();
                    } catch (InterruptedException e) {
                        continue;
                    }
                    if (LOGGER.isLoggable(COUNT_LOGGING_LEVEL)) {
                        LOGGER.log(COUNT_LOGGING_LEVEL,
                                "Dequeue (" + WorkQueue.this.hashCode() + "): " + dequeueCount.incrementAndGet() + "/"
                                        + enqueueCount);
                    }
                    try {
                        if (LOGGER.isLoggable(r.logLevel())) {
                            LOGGER.log(r.logLevel(), "Executing: " + r);
                        }
                        r.run();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } finally {
                stopSemaphore.release();
            }
        }
    }
}