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
package org.apache.asterix.external.feed.message;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.external.feed.api.IMessageReceiver;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class MessageReceiver<T> implements IMessageReceiver<T> {

    protected static final Logger LOGGER = Logger.getLogger(MessageReceiver.class.getName());

    protected final ArrayBlockingQueue<T> inbox;
    protected ExecutorService executor;

    public MessageReceiver() {
        inbox = new ArrayBlockingQueue<T>(2);
    }

    public abstract void processMessage(T message) throws Exception;

    @Override
    public void start() {
        executor = Executors.newSingleThreadExecutor();
        executor.execute(new MessageReceiverRunnable<T>(this));
    }

    @Override
    public synchronized void sendMessage(T message) throws InterruptedException {
        inbox.put(message);
    }

    @Override
    public void close(boolean processPending) {
        if (executor != null) {
            executor.shutdown();
            executor = null;
            if (processPending) {
                flushPendingMessages();
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Will discard the pending frames " + inbox.size());
                }
            }
        }
    }

    private static class MessageReceiverRunnable<T> implements Runnable {

        private final ArrayBlockingQueue<T> inbox;
        private final MessageReceiver<T> messageReceiver;

        public MessageReceiverRunnable(MessageReceiver<T> messageReceiver) {
            this.inbox = messageReceiver.inbox;
            this.messageReceiver = messageReceiver;
        }
        // TODO: this should handle exceptions better

        @Override
        public void run() {
            while (true) {
                try {
                    T message = inbox.poll();
                    if (message == null) {
                        messageReceiver.emptyInbox();
                        message = inbox.take();
                    }
                    messageReceiver.processMessage(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected void flushPendingMessages() {
        while (!inbox.isEmpty()) {
            T message = null;
            try {
                message = inbox.take();
                processMessage(message);
            } catch (InterruptedException ie) {
                // ignore exception but break from the loop
                break;
            } catch (Exception e) {
                e.printStackTrace();
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Exception " + e + " in processing message " + message);
                }
            }
        }
    }

    public abstract void emptyInbox() throws HyracksDataException;

}
