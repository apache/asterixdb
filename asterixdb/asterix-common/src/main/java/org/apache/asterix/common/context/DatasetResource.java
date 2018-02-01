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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.LocalResource;

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
    private final DatasetVirtualBufferCaches datasetVirtualBufferCaches;

    private final Map<Integer, PrimaryIndexOperationTracker> datasetPrimaryOpTrackers;
    private final Map<Integer, ILSMComponentIdGenerator> datasetComponentIdGenerators;

    public DatasetResource(DatasetInfo datasetInfo, DatasetVirtualBufferCaches datasetVirtualBufferCaches) {
        this.datasetInfo = datasetInfo;
        this.datasetVirtualBufferCaches = datasetVirtualBufferCaches;
        this.datasetPrimaryOpTrackers = new HashMap<>();
        this.datasetComponentIdGenerators = new HashMap<>();
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

    public ILSMIndex getIndex(long resourceID) {
        IndexInfo iInfo = getIndexInfo(resourceID);
        return (iInfo == null) ? null : iInfo.getIndex();
    }

    public void register(LocalResource resource, ILSMIndex index) throws HyracksDataException {
        long resourceID = resource.getId();
        if (!datasetInfo.isRegistered()) {
            synchronized (datasetInfo) {
                if (!datasetInfo.isRegistered()) {
                    datasetInfo.setExternal(index.getNumberOfAllMemoryComponents() == 0);
                    datasetInfo.setRegistered(true);
                    datasetInfo.setDurable(index.isDurable());
                }
            }
        }
        if (datasetInfo.getIndexes().containsKey(resourceID)) {
            throw new HyracksDataException("Index with resource ID " + resourceID + " already exists.");
        }
        if (index == null) {
            throw new HyracksDataException("Attempt to register a null index");
        }

        datasetInfo.addIndex(resourceID, new IndexInfo(index, datasetInfo.getDatasetID(), resource,
                ((DatasetLocalResource) resource.getResource()).getPartition()));
    }

    public DatasetInfo getDatasetInfo() {
        return datasetInfo;
    }

    public PrimaryIndexOperationTracker getOpTracker(int partition) {
        return datasetPrimaryOpTrackers.get(partition);
    }

    public Collection<PrimaryIndexOperationTracker> getOpTrackers() {
        return datasetPrimaryOpTrackers.values();
    }

    public ILSMComponentIdGenerator getComponentIdGenerator(int partition) {
        return datasetComponentIdGenerators.get(partition);
    }

    public void setPrimaryIndexOperationTracker(int partition, PrimaryIndexOperationTracker opTracker) {
        if (datasetPrimaryOpTrackers.containsKey(partition)) {
            throw new IllegalStateException(
                    "PrimaryIndexOperationTracker has already been set for partition " + partition);
        }
        datasetPrimaryOpTrackers.put(partition, opTracker);
    }

    public void setIdGenerator(int partition, ILSMComponentIdGenerator idGenerator) {
        if (datasetComponentIdGenerators.containsKey(partition)) {
            throw new IllegalStateException("LSMComponentIdGenerator has already been set for partition " + partition);
        }
        datasetComponentIdGenerators.put(partition, idGenerator);
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

    public boolean isMetadataDataset() {
        return MetadataIndexImmutableProperties.isMetadataDataset(getDatasetID());
    }
}
