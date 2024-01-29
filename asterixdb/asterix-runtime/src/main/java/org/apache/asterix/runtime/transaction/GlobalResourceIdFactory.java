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
package org.apache.asterix.runtime.transaction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.runtime.message.ResourceIdRequestMessage;
import org.apache.asterix.runtime.message.ResourceIdRequestResponseMessage;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongPriorityQueue;

/**
 * A resource id factory that generates unique resource ids across all NCs by requesting
 * unique ids from the cluster controller.
 */
public class GlobalResourceIdFactory implements IResourceIdFactory {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long INVALID_ID = -1L;
    /**
     * Maximum number of attempts to request a new block of IDs
     */
    private static final int MAX_NUMBER_OF_ATTEMPTS = 3;
    /**
     * Time threshold to consider a block request was lost
     */
    private static final long WAIT_FOR_REQUEST_TIME_THRESHOLD_NS = TimeUnit.SECONDS.toNanos(2);
    /**
     * Wait time by threads waiting for the response with the new block
     */
    private static final long WAIT_FOR_BLOCK_ID_TIME_MS = TimeUnit.SECONDS.toMillis(2);
    private final INCServiceContext serviceCtx;
    private final LongPriorityQueue resourceIds;
    private final String nodeId;
    private final int initialBlockSize;
    private final int maxBlockSize;
    /**
     * Current number of failed block requests
     */
    private final AtomicInteger numberOfFailedRequests;
    /**
     * Last time a request of a block is initiated
     */
    private final AtomicLong requestTime;
    private int currentBlockSize;
    private volatile boolean reset = false;

    public GlobalResourceIdFactory(INCServiceContext serviceCtx, int initialBlockSize) {
        this.serviceCtx = serviceCtx;
        this.nodeId = serviceCtx.getNodeId();
        this.initialBlockSize = initialBlockSize;
        maxBlockSize = initialBlockSize * 2;
        currentBlockSize = initialBlockSize;
        resourceIds = new LongArrayFIFOQueue(initialBlockSize);
        numberOfFailedRequests = new AtomicInteger();
        requestTime = new AtomicLong();
    }

    public synchronized void addNewIds(ResourceIdRequestResponseMessage resourceIdResponse)
            throws InterruptedException {
        LOGGER.debug("rec'd block of ids: {}", resourceIdResponse);
        // to ensure any block that was requested before a reset call isn't processed, we will ignore blocks where their
        // block size doesn't match the current block size
        if (resourceIdResponse.getBlockSize() != currentBlockSize) {
            LOGGER.debug("dropping outdated block size of resource ids: {}, current block size: {}", resourceIdResponse,
                    currentBlockSize);
            return;
        }
        populateIDs(resourceIdResponse);
    }

    @Override
    public long createId() throws HyracksDataException {
        // Rest IDs if requested to reset
        resetIDsIfNeeded();
        // Get a new ID if possible or request a new block
        long id = getID();
        while (id == INVALID_ID) {
            // All IDs in the previous block were consumed, wait for the new block
            waitForID();
            // Retry getting a new ID again
            id = getID();
        }

        return id;
    }

    @Override
    public synchronized void reset() {
        reset = true;
        currentBlockSize += 1;
        if (currentBlockSize > maxBlockSize) {
            currentBlockSize = initialBlockSize;
        }
        LOGGER.debug("current resource ids block size: {}", currentBlockSize);
    }

    private void populateIDs(ResourceIdRequestResponseMessage response) {
        synchronized (resourceIds) {
            long startingId = response.getResourceId();
            for (int i = 0; i < response.getBlockSize(); i++) {
                resourceIds.enqueue(startingId + i);
            }
            // Notify all waiting threads that a new block of IDs was acquired
            resourceIds.notifyAll();
        }
    }

    private void resetIDsIfNeeded() throws HyracksDataException {
        synchronized (resourceIds) {
            if (reset) {
                resourceIds.clear();
                reset = false;
                // Request the initial block
                requestNewBlock();
            }
        }
    }

    private long getID() throws HyracksDataException {
        long id = INVALID_ID;
        // Record the time of which getID was called
        long time = System.nanoTime();
        int size;
        synchronized (resourceIds) {
            size = resourceIds.size();
            if (size > 0) {
                id = resourceIds.dequeueLong();
            }
        }
        if (size == 1 || size == 0 && shouldRequestNewBlock(time)) {
            // The last ID was taken. Preemptively request a new block.
            // Or the last request failed, retry
            // Or waiting time for the response exceeded the maximum waiting time threshold
            requestNewBlock();
        }

        return id;
    }

    private void waitForID() throws HyracksDataException {
        long time = System.nanoTime();
        try {
            synchronized (resourceIds) {
                while (resourceIds.isEmpty() && !shouldRequestNewBlock(time)) {
                    resourceIds.wait(WAIT_FOR_BLOCK_ID_TIME_MS);
                    time = System.nanoTime();
                }
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private boolean shouldRequestNewBlock(long time) {
        int failures = numberOfFailedRequests.get();
        long timeDiff = time - requestTime.get();
        if (failures > 0 || timeDiff >= WAIT_FOR_REQUEST_TIME_THRESHOLD_NS) {
            long thresholdSec = TimeUnit.NANOSECONDS.toSeconds(WAIT_FOR_REQUEST_TIME_THRESHOLD_NS);
            long timeDiffSec = TimeUnit.NANOSECONDS.toSeconds(timeDiff);
            LOGGER.warn(
                    "Preemptive requests are either failed or lost "
                            + "(failures:{}, number-of-failures-threshold: {}),"
                            + " (time-since-last-request: {}s, time-threshold: {}s)",
                    failures, MAX_NUMBER_OF_ATTEMPTS, timeDiffSec, thresholdSec);
            return true;
        }
        return false;
    }

    private synchronized void requestNewBlock() throws HyracksDataException {
        int attempts = numberOfFailedRequests.get();
        if (attempts >= MAX_NUMBER_OF_ATTEMPTS) {
            synchronized (resourceIds) {
                // Notify all waiting threads so they can fail as well
                resourceIds.notifyAll();
            }
            throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "New block request was attempted (" + attempts
                    + " times) - exceeding the maximum number of allowed retries. See the logs for more information.");
        }

        requestTime.set(System.nanoTime());
        serviceCtx.getControllerService().getExecutor().submit(() -> {
            try {
                ResourceIdRequestMessage msg = new ResourceIdRequestMessage(nodeId, currentBlockSize);
                ((INCMessageBroker) serviceCtx.getMessageBroker()).sendMessageToPrimaryCC(msg);
                // Reset the number failures
                numberOfFailedRequests.set(0);
            } catch (Exception e) {
                LOGGER.warn("failed to request a new block", e);
                // Increment the number of failures
                numberOfFailedRequests.incrementAndGet();
                synchronized (resourceIds) {
                    // Notify a waiting thread (if any) to request a new block
                    resourceIds.notify();
                }
            }
        });
    }
}
