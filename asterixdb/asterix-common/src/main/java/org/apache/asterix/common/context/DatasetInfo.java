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

import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatasetInfo extends Info implements Comparable<DatasetInfo> {
    private static final Logger LOGGER = LogManager.getLogger();
    // partition -> index
    private final Map<Integer, Set<IndexInfo>> partitionIndexes;
    // resourceID -> index
    private final Map<Long, IndexInfo> indexes;
    private final int datasetID;
    private final ILogManager logManager;
    private final LogRecord waitLog = new LogRecord();
    private int numActiveIOOps;
    private long lastAccess;
    private boolean isExternal;
    private boolean isRegistered;
    private boolean memoryAllocated;
    private boolean durable;

    public DatasetInfo(int datasetID, ILogManager logManager) {
        this.partitionIndexes = new HashMap<>();
        this.indexes = new HashMap<>();
        this.setLastAccess(-1);
        this.datasetID = datasetID;
        this.setRegistered(false);
        this.setMemoryAllocated(false);
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

    public synchronized void declareActiveIOOperation() {
        numActiveIOOps++;
    }

    public synchronized void undeclareActiveIOOperation() {
        numActiveIOOps--;
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
                + ", lastAccess: " + getLastAccess() + ", isRegistered: " + isRegistered() + ", memoryAllocated: "
                + isMemoryAllocated() + ", isDurable: " + isDurable();
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
    }

    public synchronized void removeIndex(long resourceID) {
        IndexInfo info = indexes.remove(resourceID);
        if (info != null) {
            partitionIndexes.get(info.getPartition()).remove(info);
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

    public boolean isMemoryAllocated() {
        return memoryAllocated;
    }

    public void setMemoryAllocated(boolean memoryAllocated) {
        this.memoryAllocated = memoryAllocated;
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
                     * Will be Notified by {@link DatasetInfo#undeclareActiveIOOperation()}
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
}
