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
package org.apache.hyracks.control.nc.heartbeat;

import java.net.InetSocketAddress;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.control.common.base.IClusterController;
import org.apache.hyracks.control.common.heartbeat.HeartbeatData;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HeartbeatTask implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final long ONE_SECOND_NANOS = TimeUnit.SECONDS.toNanos(1);
    private final String ncId;
    private final HeartbeatData hbData;
    private final Semaphore delayBlock = new Semaphore(0);
    private final IClusterController cc;
    private final long heartbeatPeriodNanos;
    private final InetSocketAddress ncAddress;

    public HeartbeatTask(String ncId, HeartbeatData hbData, IClusterController cc, long heartbeatPeriod,
            InetSocketAddress ncAddress) {
        this.ncId = ncId;
        this.hbData = hbData;
        this.cc = cc;
        this.heartbeatPeriodNanos = TimeUnit.MILLISECONDS.toNanos(heartbeatPeriod);
        this.ncAddress = ncAddress;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                long nextFireNanoTime = System.nanoTime() + heartbeatPeriodNanos;
                final boolean success = execute();
                long delayNanos = success ? nextFireNanoTime - System.nanoTime() : ONE_SECOND_NANOS;
                if (delayNanos > 0) {
                    delayBlock.tryAcquire(delayNanos, TimeUnit.NANOSECONDS); //NOSONAR - ignore result of tryAcquire
                } else {
                    LOGGER.warn("After sending heartbeat, next one is already late by "
                            + TimeUnit.NANOSECONDS.toMillis(-delayNanos) + "ms; sending without delay");
                }
            } catch (InterruptedException e) { // NOSONAR
                break;
            }
        }
        LOGGER.log(Level.INFO, "Heartbeat task interrupted; shutting down");
    }

    private boolean execute() throws InterruptedException {
        try {
            synchronized (hbData) {
                cc.nodeHeartbeat(ncId, hbData, ncAddress);
            }
            LOGGER.trace("Successfully sent heartbeat");
            return true;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.log(Level.DEBUG, "Exception sending heartbeat; will retry after 1s", e);
            LOGGER.log(Level.WARN, "Exception sending heartbeat; will retry after 1s: " + e.toString());
            return false;
        }
    }
}
