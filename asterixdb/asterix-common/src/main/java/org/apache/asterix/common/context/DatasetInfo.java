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
package org.apache.asterix.common.context;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.api.IIOBlockingOperation;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class DatasetInfo extends Info implements Comparable<DatasetInfo> {
    private static final Logger LOGGER = LogManager.getLogger();
    // partition -> index
    private final Map<Integer, Set<IndexInfo>> partitionIndexes;
    // resourceID -> index
    private final Map<Long, IndexInfo> indexes;
    private final Int2IntMap partitionPendingIO;
    private final int datasetID;
    private final ILogManager logManager;
    private final LogRecord waitLog = new LogRecord();
    private final AtomicInteger failedFlushes = new AtomicInteger();
    private final AtomicInteger failedMerges = new AtomicInteger();
    private int numActiveIOOps;
    private int pendingFlushes;
    private int pendingMerges;
    private int pendingReplications;
    private long lastAccess;
    private boolean isExternal;
    private boolean isRegistered;
    private boolean durable;

    public DatasetInfo(int datasetID, ILogManager logManager) {
        this.partitionIndexes = new HashMap<>();
        this.indexes = new HashMap<>();
        this.partitionPendingIO = new Int2IntOpenHashMap();
        this.setLastAccess(-1);
        this.datasetID = datasetID;
        this.setRegistered(false);
        this.logManager = logManager;
        waitLog.setLogType(LogType.WAIT_FOR_FLUSHES);
        waitLog.computeAndSetLogSize();
    }

    @Override
    public void touch() {
        super.touch();
        setLastAccess(System.currentTimeMillis());
    }

    @Override
    public void untouch() {
        super.untouch();
        setLastAccess(System.currentTimeMillis());
    }

    public synchronized void declareActiveIOOperation(ILSMIOOperation.LSMIOOperationType opType, int partition) {
        partitionPendingIO.put(partition, partitionPendingIO.getOrDefault(partition, 0) + 1);
        numActiveIOOps++;
        switch (opType) {
            case FLUSH:
                pendingFlushes++;
                break;
            case MERGE:
                pendingMerges++;
                break;
            case REPLICATE:
                pendingReplications++;
                break;
            default:
                break;
        }
    }

    public synchronized void undeclareActiveIOOperation(ILSMIOOperation.LSMIOOperationType opType, int partition) {
        partitionPendingIO.put(partition, partitionPendingIO.getOrDefault(partition, 0) - 1);
        numActiveIOOps--;
        switch (opType) {
            case FLUSH:
                pendingFlushes--;
                break;
            case MERGE:
                pendingMerges--;
                break;
            case REPLICATE:
                pendingReplications--;
                break;
            default:
                break;
        }
        //notify threads waiting on this dataset info
        notifyAll();
    }

    public synchronized Set<ILSMIndex> getDatasetPartitionOpenIndexes(int partition) {
        Set<ILSMIndex> indexSet = new HashSet<>();
        Set<IndexInfo> partitionIndexInfos = this.partitionIndexes.get(partition);
        if (partitionIndexInfos != null) {
            for (IndexInfo iInfo : partitionIndexInfos) {
                if (iInfo.isOpen()) {
                    indexSet.add(iInfo.getIndex());
                }
            }
        }
        return indexSet;
    }

    @Override
    public int compareTo(DatasetInfo i) {
        // sort by (isOpen, referenceCount, lastAccess) ascending, where true < false
        //
        // Example sort order:
        // -------------------
        // (F, 0, 70)       <-- largest
        // (F, 0, 60)
        // (T, 10, 80)
        // (T, 10, 70)
        // (T, 9, 90)
        // (T, 0, 100)      <-- smallest
        if (isOpen() && !i.isOpen()) {
            return -1;
        } else if (!isOpen() && i.isOpen()) {
            return 1;
        } else {
            if (getReferenceCount() < i.getReferenceCount()) {
                return -1;
            } else if (getReferenceCount() > i.getReferenceCount()) {
                return 1;
            } else {
                if (getLastAccess() < i.getLastAccess()) {
                    return -1;
                } else if (getLastAccess() > i.getLastAccess()) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DatasetInfo) {
            return datasetID == ((DatasetInfo) obj).datasetID;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return datasetID;
    }

    @Override
    public String toString() {
        return "DatasetID: " + getDatasetID() + ", isOpen: " + isOpen() + ", refCount: " + getReferenceCount()
                + ", lastAccess: " + getLastAccess() + ", isRegistered: " + isRegistered() + ", isDurable: "
                + isDurable();
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public void setExternal(boolean isExternal) {
        this.isExternal = isExternal;
    }

    public synchronized Map<Long, IndexInfo> getIndexes() {
        return Collections.unmodifiableMap(indexes);
    }

    public synchronized void addIndex(long resourceID, IndexInfo indexInfo) {
        indexes.put(resourceID, indexInfo);
        partitionIndexes.computeIfAbsent(indexInfo.getPartition(), partition -> new HashSet<>()).add(indexInfo);
        LOGGER.debug("registered reference to index {}", indexInfo);
    }

    public synchronized void removeIndex(long resourceID) {
        IndexInfo info = indexes.remove(resourceID);
        if (info != null) {
            partitionIndexes.get(info.getPartition()).remove(info);
            LOGGER.debug("removed reference to index {}", info);
        }
    }

    public boolean isRegistered() {
        return isRegistered;
    }

    public void setRegistered(boolean isRegistered) {
        this.isRegistered = isRegistered;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public int getDatasetID() {
        return datasetID;
    }

    public long getLastAccess() {
        return lastAccess;
    }

    public void setLastAccess(long lastAccess) {
        this.lastAccess = lastAccess;
    }

    public void waitForIO() throws HyracksDataException {
        logManager.log(waitLog);
        synchronized (this) {
            while (numActiveIOOps > 0) {
                try {
                    /**
                     * Will be Notified by {@link DatasetInfo#undeclareActiveIOOperation(ILSMIOOperation.LSMIOOperationType)}
                     */
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw HyracksDataException.create(e);
                }
            }
            if (numActiveIOOps < 0) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Number of IO operations cannot be negative for dataset: " + this);
                }
                throw new IllegalStateException("Number of IO operations cannot be negative");
            }
        }
    }

    public void waitForIOAndPerform(int partition, IIOBlockingOperation operation) throws HyracksDataException {
        logManager.log(waitLog);
        synchronized (this) {
            while (partitionPendingIO.getOrDefault(partition, 0) > 0) {
                try {
                    int numPendingIOOps = partitionPendingIO.getOrDefault(partition, 0);
                    LOGGER.debug("Waiting for {} IO operations in {} partition {}", numPendingIOOps, this, partition);
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw HyracksDataException.create(e);
                }
            }

            LOGGER.debug("All IO operations for {} partition {} are finished", this, partition);

            Set<IndexInfo> indexes = partitionIndexes.get(partition);
            if (indexes != null) {
                // Perform the required operation
                operation.perform(indexes);
            }

            if (partitionPendingIO.getOrDefault(partition, 0) < 0) {
                LOGGER.error("number of IO operations cannot be negative for dataset {}, partition {}", this,
                        partition);
                throw new IllegalStateException(
                        "Number of IO operations cannot be negative: " + this + ", partition " + partition);
            }
        }
    }

    public void waitForFlushes() throws HyracksDataException {
        logManager.log(waitLog);
        synchronized (this) {
            while (pendingFlushes > 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw HyracksDataException.create(e);
                }
            }
        }
    }

    public synchronized int getPendingFlushes() {
        return pendingFlushes;
    }

    public synchronized int getPendingMerges() {
        return pendingMerges;
    }

    public synchronized int getPendingReplications() {
        return pendingReplications;
    }

    public int getFailedFlushes() {
        return failedFlushes.get();
    }

    public int getFailedMerges() {
        return failedMerges.get();
    }

    public void incrementFailedIoOp(ILSMIOOperation.LSMIOOperationType operation) {
        switch (operation) {
            case FLUSH:
                failedFlushes.incrementAndGet();
                break;
            case MERGE:
                failedMerges.incrementAndGet();
                break;
            default:
                break;
        }
    }
}
