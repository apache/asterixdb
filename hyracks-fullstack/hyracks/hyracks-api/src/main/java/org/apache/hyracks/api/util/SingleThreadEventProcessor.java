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
package org.apache.hyracks.api.util;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class SingleThreadEventProcessor<T> implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger();
    protected final String name;
    private final LinkedBlockingQueue<T> eventInbox;
    private volatile Thread executorThread;
    private volatile boolean stopped = false;

    public SingleThreadEventProcessor(String threadName) {
        this.name = threadName;
        eventInbox = new LinkedBlockingQueue<>();
        executorThread = new Thread(this, threadName);
        executorThread.start();
    }

    @Override
    public final void run() {
        LOGGER.log(Level.INFO, "Started " + Thread.currentThread().getName());
        while (!stopped) {
            try {
                T event = eventInbox.take();
                handle(event);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.log(Level.ERROR, "Error handling an event", e);
            }
        }
        LOGGER.log(Level.WARN, "Stopped " + Thread.currentThread().getName());
    }

    protected abstract void handle(T event) throws Exception; //NOSONAR

    public void add(T event) {
        if (!eventInbox.add(event)) {
            throw new IllegalStateException();
        }
    }

    public void stop() throws HyracksDataException {
        stopped = true;
        executorThread.interrupt();
        InvokeUtil.doUninterruptibly(() -> executorThread.join(1000));
        int attempt = 0;
        while (executorThread.isAlive()) {
            attempt++;
            LOGGER.log(Level.WARN, "Failed to stop event processor after {} attempts. Interrupted exception swallowed?",
                    attempt, ExceptionUtils.fromThreadStack(executorThread));
            if (attempt == 10) {
                throw HyracksDataException.create(ErrorCode.FAILED_TO_SHUTDOWN_EVENT_PROCESSOR, name);
            }
            executorThread.interrupt();
            InvokeUtil.doUninterruptibly(() -> executorThread.join(1000));
        }
    }
}
