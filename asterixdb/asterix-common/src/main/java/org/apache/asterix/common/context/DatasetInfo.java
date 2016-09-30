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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;

public class DatasetInfo extends Info implements Comparable<DatasetInfo> {
    private final Map<Long, IndexInfo> indexes;
    private final int datasetID;
    private long lastAccess;
    private int numActiveIOOps;
    private boolean isExternal;
    private boolean isRegistered;
    private boolean memoryAllocated;
    private boolean durable;

    public DatasetInfo(int datasetID) {
        this.indexes = new HashMap<>();
        this.setLastAccess(-1);
        this.datasetID = datasetID;
        this.setRegistered(false);
        this.setMemoryAllocated(false);
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
        setNumActiveIOOps(getNumActiveIOOps() + 1);
    }

    public synchronized void undeclareActiveIOOperation() {
        setNumActiveIOOps(getNumActiveIOOps() - 1);
        //notify threads waiting on this dataset info
        notifyAll();
    }

    public synchronized Set<ILSMIndex> getDatasetIndexes() {
        Set<ILSMIndex> datasetIndexes = new HashSet<>();
        for (IndexInfo iInfo : getIndexes().values()) {
            if (iInfo.isOpen()) {
                datasetIndexes.add(iInfo.getIndex());
            }
        }

        return datasetIndexes;
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
    };

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

    public int getNumActiveIOOps() {
        return numActiveIOOps;
    }

    public void setNumActiveIOOps(int numActiveIOOps) {
        this.numActiveIOOps = numActiveIOOps;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public void setExternal(boolean isExternal) {
        this.isExternal = isExternal;
    }

    public Map<Long, IndexInfo> getIndexes() {
        return indexes;
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
}
