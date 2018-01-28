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
package org.apache.asterix.common.memory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConcurrentFramePool {
    private static final boolean DEBUG = false;
    private static final String ERROR_INVALID_FRAME_SIZE =
            "The size should be an integral " + "multiple of the default frame size";
    private static final String ERROR_LARGER_THAN_BUDGET_REQUEST =
            "The requested frame size" + " must not be greater than the allocated budget";
    private static final Logger LOGGER = LogManager.getLogger();
    private final String nodeId;
    private final int budget;
    private final int defaultFrameSize;
    private final ArrayDeque<ByteBuffer> pool;
    private final ArrayDeque<FrameAction> subscribers = new ArrayDeque<>();
    private final Map<Integer, ArrayDeque<ByteBuffer>> largeFramesPools;
    private int handedOut;
    private int created;

    public ConcurrentFramePool(String nodeId, long budgetInBytes, int frameSize) {
        this.nodeId = nodeId;
        this.defaultFrameSize = frameSize;
        this.budget = (int) (budgetInBytes / frameSize);
        this.pool = new ArrayDeque<>(budget);
        this.largeFramesPools = new HashMap<>();
    }

    public int getMaxFrameSize() {
        return budget * defaultFrameSize;
    }

    public synchronized ByteBuffer get() {
        // Subscribers have higher priority
        if (subscribers.isEmpty()) {
            return doGet();
        }
        if (DEBUG) {
            LOGGER.info("Unable to allocate buffer since subscribers are in-line. Number of subscribers = "
                    + subscribers.size());
        }
        return null;
    }

    private ByteBuffer doGet() {
        if (handedOut < budget) {
            handedOut++;
            return allocate();
        }
        if (DEBUG) {
            LOGGER.info("Unable to allocate buffer without exceeding budget. Remaining = " + remaining()
                    + ", Requested = 1");
        }
        return null;
    }

    public int remaining() {
        return budget - handedOut;
    }

    private ByteBuffer doGet(int bufferSize) throws HyracksDataException {
        // Subscribers have higher priority
        if (bufferSize % defaultFrameSize != 0) {
            throw new HyracksDataException(ERROR_INVALID_FRAME_SIZE);
        }
        int multiplier = bufferSize / defaultFrameSize;
        if (multiplier > budget) {
            throw new HyracksDataException(ERROR_LARGER_THAN_BUDGET_REQUEST);
        }
        if (handedOut + multiplier <= budget) {
            handedOut += multiplier;
            ArrayDeque<ByteBuffer> largeFramesPool = largeFramesPools.get(multiplier);
            if (largeFramesPool == null || largeFramesPool.isEmpty()) {
                if (created + multiplier > budget) {
                    freeup(multiplier);
                }
                created += multiplier;
                return ByteBuffer.allocate(bufferSize);
            }
            ByteBuffer buffer = largeFramesPool.poll();
            buffer.clear();
            return buffer;
        }
        // Not enough budget
        if (DEBUG) {
            LOGGER.info("Unable to allocate buffer without exceeding budget. Remaining = " + remaining()
                    + ", Requested = " + multiplier);
        }
        return null;
    }

    public synchronized ByteBuffer get(int bufferSize) throws HyracksDataException {
        if (subscribers.isEmpty()) {
            return doGet(bufferSize);
        }
        if (DEBUG) {
            LOGGER.info("Unable to allocate buffer since subscribers are in-line. Number of subscribers = "
                    + subscribers.size());
        }
        return null;
    }

    private int freeup(int desiredNumberOfFreePages) {
        int needToFree = desiredNumberOfFreePages - (budget - created);
        int freed = 0;
        // start by large frames
        for (Iterator<Entry<Integer, ArrayDeque<ByteBuffer>>> it = largeFramesPools.entrySet().iterator(); it
                .hasNext();) {
            Entry<Integer, ArrayDeque<ByteBuffer>> entry = it.next();
            if (entry.getKey() != desiredNumberOfFreePages) {
                while (!entry.getValue().isEmpty()) {
                    entry.getValue().pop();
                    freed += entry.getKey();
                    if (freed >= needToFree) {
                        // created is handled here
                        created -= freed;
                        return freed;
                    }
                }
                it.remove();
            }
        }
        // freed all large pages. need to free small pages as well
        needToFree -= freed;
        while (needToFree > 0) {
            pool.pop();
            needToFree--;
            freed++;
        }
        created -= freed;
        return freed;
    }

    private ByteBuffer allocate() {
        if (pool.isEmpty()) {
            if (created == budget) {
                freeup(1);
            }
            created++;
            return ByteBuffer.allocate(defaultFrameSize);
        } else {
            ByteBuffer buffer = pool.pop();
            buffer.clear();
            return buffer;
        }
    }

    public synchronized boolean get(Collection<ByteBuffer> buffers, int count) {
        if (handedOut + count <= budget) {
            handedOut += count;
            for (int i = 0; i < count; i++) {
                buffers.add(allocate());
            }
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "ConcurrentFramePool  [" + nodeId + "]" + "(consumed:" + handedOut + "/" + budget + ")";
    }

    public synchronized void release(Collection<ByteBuffer> buffers) throws HyracksDataException {
        for (ByteBuffer buffer : buffers) {
            release(buffer);
        }
    }

    public synchronized void release(ByteBuffer buffer) throws HyracksDataException {
        int multiples = buffer.capacity() / defaultFrameSize;
        handedOut -= multiples;
        if (DEBUG) {
            LOGGER.info("Releasing " + multiples + " frames. Remaining frames = " + remaining());
        }
        if (multiples == 1) {
            pool.add(buffer);
        } else {
            ArrayDeque<ByteBuffer> largeFramesPool = largeFramesPools.get(multiples);
            if (largeFramesPool == null) {
                largeFramesPool = new ArrayDeque<>();
                largeFramesPools.put(multiples, largeFramesPool);
            }
            largeFramesPool.push(buffer);
        }
        // check subscribers
        while (!subscribers.isEmpty()) {
            FrameAction frameAction = subscribers.peek();
            ByteBuffer freeBuffer;
            // check if we have enough and answer immediately.
            if (frameAction.getSize() == defaultFrameSize) {
                if (DEBUG) {
                    LOGGER.info("Attempting to callback a subscriber that requested 1 frame");
                }
                freeBuffer = doGet();
            } else {
                if (DEBUG) {
                    LOGGER.info("Attempting to callback a subscriber that requested "
                            + frameAction.getSize() / defaultFrameSize + " frames");
                }
                freeBuffer = doGet(frameAction.getSize());
            }
            if (freeBuffer != null) {
                int handedOutBeforeCall = handedOut;
                try {
                    frameAction.call(freeBuffer);
                } catch (Exception e) {
                    LOGGER.log(Level.ERROR, "Error while attempting to answer a subscription. Buffer will be reclaimed",
                            e);
                    // TODO(amoudi): Add test cases and get rid of recursion
                    if (handedOut == handedOutBeforeCall) {
                        release(freeBuffer);
                    }
                    throw e;
                } finally {
                    subscribers.remove();
                    if (DEBUG) {
                        LOGGER.info(
                                "A subscription has been satisfied. " + subscribers.size() + " remaining subscribers");
                    }
                }
            } else {
                if (DEBUG) {
                    LOGGER.info("Failed to allocate requested frames");
                }
                break;
            }
        }
        if (DEBUG) {
            LOGGER.info(subscribers.size() + " remaining subscribers");
        }
    }

    public synchronized boolean subscribe(FrameAction frameAction) throws HyracksDataException {
        // check if subscribers are empty?
        if (subscribers.isEmpty()) {
            ByteBuffer buffer;
            // check if we have enough and answer immediately.
            if (frameAction.getSize() == defaultFrameSize) {
                buffer = doGet();
            } else {
                buffer = doGet(frameAction.getSize());
            }
            if (buffer != null) {
                frameAction.call(buffer);
                // There is no need to subscribe. perform action and return false
                return false;
            }
        } else {
            int multiplier = frameAction.getSize() / defaultFrameSize;
            if (multiplier > budget) {
                throw new HyracksDataException(ERROR_LARGER_THAN_BUDGET_REQUEST);
            }
        }
        // none of the above, add to subscribers and return true
        subscribers.add(frameAction);
        return true;
    }

    /*
     * For unit testing purposes
     */
    public Collection<FrameAction> getSubscribers() {
        return subscribers;
    }
}
