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

import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;

/**
 * A dataset can be in one of two states { EVICTED , LOADED }.
 * When a dataset is created, it is in the EVICTED state. In the EVICTED state, the allowed operations are:
 * 1. DELETE: delete the dataset completely.
 * 2. LOAD: move the dataset to the LOADED state.
 * When a dataset is in the LOADED state, the allowed operations are:
 * 1. OPEN: increment the open counter.
 * 2. ALLOCATE RESOURCES (memory)
 * 3. DEALLOCATE RESOURCES (memory)
 * 4. CLOSE: decrement the open counter.
 * 5. EVICT: deallocate resources and unload the dataset moving it to the EVICTED state
 */
public class DatasetResource implements Comparable<DatasetResource> {
    private final DatasetInfo datasetInfo;
    private final PrimaryIndexOperationTracker datasetPrimaryOpTracker;
    private final DatasetVirtualBufferCaches datasetVirtualBufferCaches;

    public DatasetResource(DatasetInfo datasetInfo,
            PrimaryIndexOperationTracker datasetPrimaryOpTracker,
            DatasetVirtualBufferCaches datasetVirtualBufferCaches) {
        this.datasetInfo = datasetInfo;
        this.datasetPrimaryOpTracker = datasetPrimaryOpTracker;
        this.datasetVirtualBufferCaches = datasetVirtualBufferCaches;
    }

    public boolean isRegistered() {
        return datasetInfo.isRegistered();
    }

    public IndexInfo getIndexInfo(long resourceID) {
        return datasetInfo.getIndexes().get(resourceID);
    }

    public boolean isOpen() {
        return datasetInfo.isOpen();
    }

    public boolean isExternal() {
        return datasetInfo.isExternal();
    }

    public void open(boolean open) {
        datasetInfo.setOpen(open);
    }

    public void touch() {
        datasetInfo.touch();
    }

    public void untouch() {
        datasetInfo.untouch();
    }

    public DatasetVirtualBufferCaches getVirtualBufferCaches() {
        return datasetVirtualBufferCaches;
    }

    public IIndex getIndex(long resourceID) {
        IndexInfo iInfo = getIndexInfo(resourceID);
        return (iInfo == null) ? null : iInfo.getIndex();
    }

    public void register(long resourceID, IIndex index) throws HyracksDataException {
        if (!datasetInfo.isRegistered()) {
            synchronized (datasetInfo) {
                if (!datasetInfo.isRegistered()) {
                    datasetInfo.setExternal(!index.hasMemoryComponents());
                    datasetInfo.setRegistered(true);
                    datasetInfo.setDurable(((ILSMIndex) index).isDurable());
                }
            }
        }
        if (datasetInfo.getIndexes().containsKey(resourceID)) {
            throw new HyracksDataException("Index with resource ID " + resourceID + " already exists.");
        }
        if (index == null) {
            throw new HyracksDataException("Attempt to register a null index");
        }
        datasetInfo.getIndexes().put(resourceID,
                new IndexInfo((ILSMIndex) index, datasetInfo.getDatasetID(), resourceID));
    }

    public DatasetInfo getDatasetInfo() {
        return datasetInfo;
    }

    public PrimaryIndexOperationTracker getOpTracker() {
        return datasetPrimaryOpTracker;
    }

    @Override
    public int compareTo(DatasetResource o) {
        return datasetInfo.compareTo(o.datasetInfo);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DatasetResource) {
            return datasetInfo.equals(((DatasetResource) obj).datasetInfo);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return datasetInfo.hashCode();
    }

    public Map<Long, IndexInfo> getIndexes() {
        return datasetInfo.getIndexes();
    }

    public int getDatasetID() {
        return datasetInfo.getDatasetID();
    }
}
