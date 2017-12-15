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
package org.apache.asterix.replication.logging;

import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class is responsible for sending transactions logs to remote replicas.
 */
public class TxnLogReplicator implements Callable<Boolean> {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final ReplicationLogBuffer POISON_PILL = new ReplicationLogBuffer(null, 0, 0);
    private final LinkedBlockingQueue<ReplicationLogBuffer> emptyQ;
    private final LinkedBlockingQueue<ReplicationLogBuffer> flushQ;
    private ReplicationLogBuffer flushPage;
    private final AtomicBoolean isStarted;
    private final AtomicBoolean terminateFlag;

    public TxnLogReplicator(LinkedBlockingQueue<ReplicationLogBuffer> emptyQ,
            LinkedBlockingQueue<ReplicationLogBuffer> flushQ) {
        this.emptyQ = emptyQ;
        this.flushQ = flushQ;
        flushPage = null;
        isStarted = new AtomicBoolean(false);
        terminateFlag = new AtomicBoolean(false);
    }

    public void terminate() {
        //make sure the LogFlusher thread started before terminating it.
        synchronized (isStarted) {
            while (!isStarted.get()) {
                try {
                    isStarted.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        terminateFlag.set(true);
        if (flushPage != null) {
            synchronized (flushPage) {
                flushPage.isStop(true);
                flushPage.notify();
            }
        }
        //[Notice]
        //The return value doesn't need to be checked
        //since terminateFlag will trigger termination if the flushQ is full.
        flushQ.offer(POISON_PILL);
    }

    @Override
    public Boolean call() {
        Thread.currentThread().setName("TxnLog Replicator");
        synchronized (isStarted) {
            isStarted.set(true);
            isStarted.notify();
        }

        while (true) {
            try {
                if (terminateFlag.get()) {
                    return true;
                }

                flushPage = null;
                flushPage = flushQ.take();
                if (flushPage == POISON_PILL) {
                    continue;
                }
                flushPage.flush();
                // TODO: pool large pages
                if (flushPage.getLogBufferSize() == flushPage.getReplicationManager().getLogPageSize()) {
                    emptyQ.offer(flushPage);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.error("TxnLogReplicator is terminating abnormally. Logs Replication Stopped.", e);
                throw e;
            }
        }
    }
}
