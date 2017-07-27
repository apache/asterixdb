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
package org.apache.asterix.active;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class SingleThreadEventProcessor<T> implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(SingleThreadEventProcessor.class.getName());
    private final String name;
    private final LinkedBlockingQueue<T> eventInbox;
    private final ExecutorService executorService;
    private final Future<?> future;

    public SingleThreadEventProcessor(String threadName) {
        this.name = threadName;
        eventInbox = new LinkedBlockingQueue<>();
        executorService = Executors.newSingleThreadExecutor(r -> new Thread(r, threadName));
        future = executorService.submit(this);
    }

    @Override
    public final void run() {
        LOGGER.log(Level.INFO, "Started " + Thread.currentThread().getName());
        while (!Thread.currentThread().isInterrupted()) {
            try {
                T event = eventInbox.take();
                handle(event);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error handling an event", e);
            }
        }
        LOGGER.log(Level.WARNING, "Stopped " + Thread.currentThread().getName());
    }

    protected abstract void handle(T event) throws Exception; //NOSONAR

    public void add(T event) {
        if (!eventInbox.add(event)) {
            throw new IllegalStateException();
        }
    }

    public void stop() throws HyracksDataException, InterruptedException {
        future.cancel(true);
        executorService.shutdown();
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
            throw HyracksDataException.create(ErrorCode.FAILED_TO_SHUTDOWN_EVENT_PROCESSOR, name);
        }
    }
}
