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
package org.apache.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.api.IFeedMemoryComponent.Type;
import org.apache.asterix.common.feeds.api.IFeedMemoryManager;
import org.apache.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class FrameDistributor {

    private static final Logger LOGGER = Logger.getLogger(FrameDistributor.class.getName());

    private static final long MEMORY_AVAILABLE_POLL_PERIOD = 1000; // 1 second

    private final IHyracksTaskContext ctx;
    private final FeedId feedId;
    private final FeedRuntimeType feedRuntimeType;
    private final int partition;
    private final IFeedMemoryManager memoryManager;
    private final boolean enableSynchronousTransfer;
    /** A map storing the registered frame readers ({@code FeedFrameCollector}. **/
    private final Map<IFrameWriter, FeedFrameCollector> registeredCollectors;
    private final FrameTupleAccessor fta;

    private DataBucketPool pool;
    private DistributionMode distributionMode;
    private boolean spillToDiskRequired = false;

    public enum DistributionMode {
        /**
         * A single feed frame collector is registered for receiving tuples.
         * Tuple is sent via synchronous call, that is no buffering is involved
         **/
        SINGLE,

        /**
         * Multiple feed frame collectors are concurrently registered for
         * receiving tuples.
         **/
        SHARED,

        /**
         * Feed tuples are not being processed, irrespective of # of registered
         * feed frame collectors.
         **/
        INACTIVE
    }

    public FrameDistributor(IHyracksTaskContext ctx, FeedId feedId, FeedRuntimeType feedRuntimeType, int partition,
            boolean enableSynchronousTransfer, IFeedMemoryManager memoryManager, FrameTupleAccessor fta)
            throws HyracksDataException {
        this.ctx = ctx;
        this.feedId = feedId;
        this.feedRuntimeType = feedRuntimeType;
        this.partition = partition;
        this.memoryManager = memoryManager;
        this.enableSynchronousTransfer = enableSynchronousTransfer;
        this.registeredCollectors = new HashMap<IFrameWriter, FeedFrameCollector>();
        this.distributionMode = DistributionMode.INACTIVE;
        this.fta = fta;
    }

    public void notifyEndOfFeed() {
        DataBucket bucket = getDataBucket();
        if (bucket != null) {
            sendEndOfFeedDataBucket(bucket);
        } else {
            while (bucket == null) {
                try {
                    Thread.sleep(MEMORY_AVAILABLE_POLL_PERIOD);
                    bucket = getDataBucket();
                } catch (InterruptedException e) {
                    break;
                }
            }
            if (bucket != null) {
                sendEndOfFeedDataBucket(bucket);
            }
        }
    }

    private void sendEndOfFeedDataBucket(DataBucket bucket) {
        bucket.setContentType(DataBucket.ContentType.EOD);
        nextBucket(bucket);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("End of feed data packet sent " + this.feedId);
        }
    }

    public synchronized void registerFrameCollector(FeedFrameCollector frameCollector) {
        DistributionMode currentMode = distributionMode;
        switch (distributionMode) {
            case INACTIVE:
                if (!enableSynchronousTransfer) {
                    pool = (DataBucketPool) memoryManager.getMemoryComponent(Type.POOL);
                    frameCollector.start();
                }
                registeredCollectors.put(frameCollector.getFrameWriter(), frameCollector);
                setMode(DistributionMode.SINGLE);
                break;
            case SINGLE:
                pool = (DataBucketPool) memoryManager.getMemoryComponent(Type.POOL);
                registeredCollectors.put(frameCollector.getFrameWriter(), frameCollector);
                for (FeedFrameCollector reader : registeredCollectors.values()) {
                    reader.start();
                }
                setMode(DistributionMode.SHARED);
                break;
            case SHARED:
                frameCollector.start();
                registeredCollectors.put(frameCollector.getFrameWriter(), frameCollector);
                break;
        }
        evaluateIfSpillIsEnabled();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Switching to " + distributionMode + " mode from " + currentMode + " mode " + " Feed id "
                    + feedId);
        }
    }

    public synchronized void deregisterFrameCollector(FeedFrameCollector frameCollector) {
        switch (distributionMode) {
            case INACTIVE:
                throw new IllegalStateException("Invalid attempt to deregister frame collector in " + distributionMode
                        + " mode.");
            case SHARED:
                frameCollector.closeCollector();
                registeredCollectors.remove(frameCollector.getFrameWriter());
                int nCollectors = registeredCollectors.size();
                if (nCollectors == 1) {
                    FeedFrameCollector loneCollector = registeredCollectors.values().iterator().next();
                    setMode(DistributionMode.SINGLE);
                    loneCollector.setState(FeedFrameCollector.State.TRANSITION);
                    loneCollector.closeCollector();
                    memoryManager.releaseMemoryComponent(pool);
                    evaluateIfSpillIsEnabled();
                } else {
                    if (!spillToDiskRequired) {
                        evaluateIfSpillIsEnabled();
                    }
                }
                break;
            case SINGLE:
                frameCollector.closeCollector();
                setMode(DistributionMode.INACTIVE);
                spillToDiskRequired = false;
                break;

        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Deregistered frame reader" + frameCollector + " from feed distributor for " + feedId);
        }
    }

    public void evaluateIfSpillIsEnabled() {
        if (!spillToDiskRequired) {
            for (FeedFrameCollector collector : registeredCollectors.values()) {
                spillToDiskRequired = spillToDiskRequired
                        || collector.getFeedPolicyAccessor().spillToDiskOnCongestion();
                if (spillToDiskRequired) {
                    break;
                }
            }
        }
    }

    public boolean deregisterFrameCollector(IFrameWriter frameWriter) {
        FeedFrameCollector collector = registeredCollectors.get(frameWriter);
        if (collector != null) {
            deregisterFrameCollector(collector);
            return true;
        }
        return false;
    }

    public synchronized void setMode(DistributionMode mode) {
        this.distributionMode = mode;
    }

    public boolean isRegistered(IFrameWriter writer) {
        return registeredCollectors.get(writer) != null;
    }

    public synchronized void nextFrame(ByteBuffer frame) throws HyracksDataException {
        switch (distributionMode) {
            case INACTIVE:
                break;
            case SINGLE:
                FeedFrameCollector collector = registeredCollectors.values().iterator().next();
                switch (collector.getState()) {
                    case HANDOVER:
                    case ACTIVE:
                        if (enableSynchronousTransfer) {
                            collector.nextFrame(frame); // processing is synchronous
                        } else {
                            handleDataBucket(frame);
                        }
                        break;
                    case TRANSITION:
                        handleDataBucket(frame);
                        break;
                    case FINISHED:
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Discarding fetched tuples, feed has ended [" + registeredCollectors.get(0)
                                    + "]" + " Feed Id " + feedId + " frame distributor " + this.getFeedRuntimeType());
                        }
                        registeredCollectors.remove(0);
                        break;
                }
                break;
            case SHARED:
                handleDataBucket(frame);
                break;
        }
    }

    private void nextBucket(DataBucket bucket) {
        for (FeedFrameCollector collector : registeredCollectors.values()) {
            collector.sendMessage(bucket); // asynchronous call
        }
    }

    private void handleDataBucket(ByteBuffer frame) throws HyracksDataException {
        DataBucket bucket = getDataBucket();
        if (bucket == null) {
            handleFrameDuringMemoryCongestion(frame);
        } else {
            bucket.reset(frame);
            bucket.setDesiredReadCount(registeredCollectors.size());
            nextBucket(bucket);
        }
    }

    private void handleFrameDuringMemoryCongestion(ByteBuffer frame) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("Unable to allocate memory, will evaluate the need to spill");
        }
        // wait till memory is available
    }

    private DataBucket getDataBucket() {
        DataBucket bucket = null;
        if (pool != null) {
            bucket = pool.getDataBucket();
            if (bucket != null) {
                bucket.setDesiredReadCount(registeredCollectors.size());
                return bucket;
            } else {
                return null;
            }
        }
        return null;
    }

    public DistributionMode getMode() {
        return distributionMode;
    }

    public void close() {
        switch (distributionMode) {
            case INACTIVE:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("FrameDistributor is " + distributionMode);
                }
                break;
            case SINGLE:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Disconnecting single frame reader in " + distributionMode + " mode " + " for  feedId "
                            + feedId + " " + this.feedRuntimeType);
                }
                setMode(DistributionMode.INACTIVE);
                if (!enableSynchronousTransfer) {
                    notifyEndOfFeed(); // send EOD Data Bucket
                    waitForCollectorsToFinish();
                }
                registeredCollectors.values().iterator().next().disconnect();
                break;
            case SHARED:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Signalling End Of Feed; currently operating in " + distributionMode + " mode");
                }
                notifyEndOfFeed(); // send EOD Data Bucket
                waitForCollectorsToFinish();
                break;
        }
    }

    private void waitForCollectorsToFinish() {
        synchronized (registeredCollectors.values()) {
            while (!allCollectorsFinished()) {
                try {
                    registeredCollectors.values().wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean allCollectorsFinished() {
        boolean allFinished = true;
        for (FeedFrameCollector collector : registeredCollectors.values()) {
            allFinished = allFinished && collector.getState().equals(FeedFrameCollector.State.FINISHED);
        }
        return allFinished;
    }

    public Collection<FeedFrameCollector> getRegisteredCollectors() {
        return registeredCollectors.values();
    }

    public Map<IFrameWriter, FeedFrameCollector> getRegisteredReaders() {
        return registeredCollectors;
    }

    public FeedId getFeedId() {
        return feedId;
    }

    public DistributionMode getDistributionMode() {
        return distributionMode;
    }

    public FeedRuntimeType getFeedRuntimeType() {
        return feedRuntimeType;
    }

    public int getPartition() {
        return partition;
    }

    public FrameTupleAccessor getFta() {
        return fta;
    }

}