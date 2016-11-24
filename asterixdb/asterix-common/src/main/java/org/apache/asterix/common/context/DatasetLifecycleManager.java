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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.AsterixStorageProperties;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.Resource;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.LocalResource;

public class DatasetLifecycleManager implements IDatasetLifecycleManager, ILifeCycleComponent {
    private final Map<Integer, DatasetResource> datasets = new ConcurrentHashMap<>();
    private final AsterixStorageProperties storageProperties;
    private final ILocalResourceRepository resourceRepository;
    private final int firstAvilableUserDatasetID;
    private final long capacity;
    private long used;
    private final ILogManager logManager;
    private final LogRecord logRecord;
    private final int numPartitions;
    private volatile boolean stopped = false;

    public DatasetLifecycleManager(AsterixStorageProperties storageProperties,
            ILocalResourceRepository resourceRepository, int firstAvilableUserDatasetID, ILogManager logManager,
            int numPartitions) {
        this.logManager = logManager;
        this.storageProperties = storageProperties;
        this.resourceRepository = resourceRepository;
        this.firstAvilableUserDatasetID = firstAvilableUserDatasetID;
        this.numPartitions = numPartitions;
        capacity = storageProperties.getMemoryComponentGlobalBudget();
        used = 0;
        logRecord = new LogRecord();
    }

    @Override
    public synchronized IIndex get(String resourcePath) throws HyracksDataException {
        validateDatasetLifecycleManagerState();
        int datasetID = getDIDfromResourcePath(resourcePath);
        long resourceID = getResourceIDfromResourcePath(resourcePath);
        return getIndex(datasetID, resourceID);
    }

    @Override
    public synchronized IIndex getIndex(int datasetID, long resourceID) throws HyracksDataException {
        validateDatasetLifecycleManagerState();
        DatasetResource datasetResource = datasets.get(datasetID);
        if (datasetResource == null) {
            return null;
        }
        return datasetResource.getIndex(resourceID);
    }

    @Override
    public synchronized void register(String resourcePath, IIndex index) throws HyracksDataException {
        validateDatasetLifecycleManagerState();
        int did = getDIDfromResourcePath(resourcePath);
        long resourceID = getResourceIDfromResourcePath(resourcePath);
        DatasetResource datasetResource = datasets.get(did);
        if (datasetResource == null) {
            datasetResource = getDatasetLifecycle(did);
        }
        datasetResource.register(resourceID, index);
    }

    public int getDIDfromResourcePath(String resourcePath) throws HyracksDataException {
        LocalResource lr = resourceRepository.get(resourcePath);
        if (lr == null) {
            return -1;
        }
        return ((Resource) lr.getResource()).datasetId();
    }

    public long getResourceIDfromResourcePath(String resourcePath) throws HyracksDataException {
        LocalResource lr = resourceRepository.get(resourcePath);
        if (lr == null) {
            return -1;
        }
        return lr.getId();
    }

    @Override
    public synchronized void unregister(String resourcePath) throws HyracksDataException {
        validateDatasetLifecycleManagerState();
        int did = getDIDfromResourcePath(resourcePath);
        long resourceID = getResourceIDfromResourcePath(resourcePath);

        DatasetResource dsr = datasets.get(did);
        IndexInfo iInfo = dsr == null ? null : dsr.getIndexInfo(resourceID);

        if (dsr == null || iInfo == null) {
            throw new HyracksDataException("Index with resource ID " + resourceID + " does not exist.");
        }

        PrimaryIndexOperationTracker opTracker = dsr.getOpTracker();
        if (iInfo.getReferenceCount() != 0 || (opTracker != null && opTracker.getNumActiveOperations() != 0)) {
            throw new HyracksDataException("Cannot remove index while it is open. (Dataset reference count = "
                    + iInfo.getReferenceCount() + ", Operation tracker number of active operations = "
                    + opTracker.getNumActiveOperations() + ")");
        }

        // TODO: use fine-grained counters, one for each index instead of a single counter per dataset.
        // First wait for any ongoing IO operations
        DatasetInfo dsInfo = dsr.getDatasetInfo();
        synchronized (dsInfo) {
            while (dsInfo.getNumActiveIOOps() > 0) {
                try {
                    //notification will come from DatasetInfo class (undeclareActiveIOOperation)
                    dsInfo.wait();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }
        }

        if (iInfo.isOpen()) {
            ILSMOperationTracker indexOpTracker = iInfo.getIndex().getOperationTracker();
            synchronized (indexOpTracker) {
                iInfo.getIndex().deactivate(false);
            }
        }

        dsInfo.getIndexes().remove(resourceID);
        if (dsInfo.getReferenceCount() == 0 && dsInfo.isOpen() && dsInfo.getIndexes().isEmpty()
                && !dsInfo.isExternal()) {
            removeDatasetFromCache(dsInfo.getDatasetID());
        }
    }

    @Override
    public synchronized void open(String resourcePath) throws HyracksDataException {
            validateDatasetLifecycleManagerState();
            int did = getDIDfromResourcePath(resourcePath);
            long resourceID = getResourceIDfromResourcePath(resourcePath);

            DatasetResource dsr = datasets.get(did);
            DatasetInfo dsInfo = dsr.getDatasetInfo();
            if (dsInfo == null || !dsInfo.isRegistered()) {
                throw new HyracksDataException(
                        "Failed to open index with resource ID " + resourceID + " since it does not exist.");
            }

            IndexInfo iInfo = dsInfo.getIndexes().get(resourceID);
            if (iInfo == null) {
                throw new HyracksDataException(
                        "Failed to open index with resource ID " + resourceID + " since it does not exist.");
            }

            dsr.open(true);
            dsr.touch();

            if (!iInfo.isOpen()) {
                ILSMOperationTracker opTracker = iInfo.getIndex().getOperationTracker();
                synchronized (opTracker) {
                    iInfo.getIndex().activate();
                }
                iInfo.setOpen(true);
            }
            iInfo.touch();
    }

    private boolean evictCandidateDataset() throws HyracksDataException {
        /**
         * We will take a dataset that has no active transactions, it is open (a dataset consuming memory),
         * that is not being used (refcount == 0) and has been least recently used, excluding metadata datasets.
         * The sort order defined for DatasetInfo maintains this. See DatasetInfo.compareTo().
         */
        List<DatasetResource> datasetsResources = new ArrayList<>(datasets.values());
        Collections.sort(datasetsResources);
        for (DatasetResource dsr : datasetsResources) {
            PrimaryIndexOperationTracker opTracker = dsr.getOpTracker();
            if (opTracker != null && opTracker.getNumActiveOperations() == 0
                    && dsr.getDatasetInfo().getReferenceCount() == 0
                    && dsr.getDatasetInfo().isOpen()
                    && dsr.getDatasetInfo().getDatasetID() >= getFirstAvilableUserDatasetID()) {
                closeDataset(dsr.getDatasetInfo());
                return true;
            }
        }
        return false;
    }

    private static void flushAndWaitForIO(DatasetInfo dsInfo, IndexInfo iInfo) throws HyracksDataException {
        if (iInfo.isOpen()) {
            ILSMIndexAccessor accessor = iInfo.getIndex().createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleFlush(iInfo.getIndex().getIOOperationCallback());
        }

        // Wait for the above flush op.
        synchronized (dsInfo) {
            while (dsInfo.getNumActiveIOOps() > 0) {
                try {
                    //notification will come from DatasetInfo class (undeclareActiveIOOperation)
                    dsInfo.wait();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
    }

    public DatasetResource getDatasetLifecycle(int did) {
        DatasetResource dsr = datasets.get(did);
        if (dsr != null) {
            return dsr;
        }
        synchronized (datasets) {
            dsr = datasets.get(did);
            if (dsr == null) {
                DatasetInfo dsInfo = new DatasetInfo(did);
                PrimaryIndexOperationTracker opTracker = new PrimaryIndexOperationTracker(did, logManager, dsInfo);
                DatasetVirtualBufferCaches vbcs = new DatasetVirtualBufferCaches(did, storageProperties,
                        getFirstAvilableUserDatasetID(),
                        getNumPartitions());
                dsr = new DatasetResource(dsInfo, opTracker, vbcs);
                datasets.put(did, dsr);
            }
            return dsr;
        }
    }

    @Override
    public DatasetInfo getDatasetInfo(int datasetID) {
        return getDatasetLifecycle(datasetID).getDatasetInfo();
    }

    @Override
    public synchronized void close(String resourcePath) throws HyracksDataException {
        validateDatasetLifecycleManagerState();
        int did = getDIDfromResourcePath(resourcePath);
        long resourceID = getResourceIDfromResourcePath(resourcePath);
        DatasetResource dsr = datasets.get(did);
        if (dsr == null) {
            throw new HyracksDataException("No index found with resourceID " + resourceID);
        }
        IndexInfo iInfo = dsr.getIndexInfo(resourceID);
        if (iInfo == null) {
            throw new HyracksDataException("No index found with resourceID " + resourceID);
        }
        iInfo.untouch();
        dsr.untouch();
    }

    @Override
    public synchronized List<IIndex> getOpenResources() {
        List<IndexInfo> openIndexesInfo = getOpenIndexesInfo();
        List<IIndex> openIndexes = new ArrayList<>();
        for (IndexInfo iInfo : openIndexesInfo) {
            openIndexes.add(iInfo.getIndex());
        }
        return openIndexes;
    }

    @Override
    public synchronized List<IndexInfo> getOpenIndexesInfo() {
        List<IndexInfo> openIndexesInfo = new ArrayList<>();
        for (DatasetResource dsr : datasets.values()) {
            for (IndexInfo iInfo : dsr.getIndexes().values()) {
                if (iInfo.isOpen()) {
                    openIndexesInfo.add(iInfo);
                }
            }
        }
        return openIndexesInfo;
    }

    private DatasetVirtualBufferCaches getVirtualBufferCaches(int datasetID) {
        return getDatasetLifecycle(datasetID).getVirtualBufferCaches();
    }

    @Override
    public List<IVirtualBufferCache> getVirtualBufferCaches(int datasetID, int ioDeviceNum) {
        DatasetVirtualBufferCaches dvbcs = getVirtualBufferCaches(datasetID);
        return dvbcs.getVirtualBufferCaches(this, ioDeviceNum);
    }

    private void removeDatasetFromCache(int datasetID) throws HyracksDataException {
        deallocateDatasetMemory(datasetID);
        datasets.remove(datasetID);
    }

    @Override
    public PrimaryIndexOperationTracker getOperationTracker(int datasetID) {
        return datasets.get(datasetID).getOpTracker();
    }

    private void validateDatasetLifecycleManagerState() throws HyracksDataException {
        if (stopped) {
            throw new HyracksDataException(DatasetLifecycleManager.class.getSimpleName() + " was stopped.");
        }
    }

    @Override
    public synchronized void start() {
        used = 0;
    }

    @Override
    public synchronized void flushAllDatasets() throws HyracksDataException {
        for (DatasetResource dsr : datasets.values()) {
            flushDatasetOpenIndexes(dsr.getDatasetInfo(), false);
        }
    }

    @Override
    public synchronized void flushDataset(int datasetId, boolean asyncFlush) throws HyracksDataException {
        DatasetResource dsr = datasets.get(datasetId);
        if (dsr != null) {
            flushDatasetOpenIndexes(dsr.getDatasetInfo(), asyncFlush);
        }
    }

    @Override
    public synchronized void scheduleAsyncFlushForLaggingDatasets(long targetLSN) throws HyracksDataException {
        //schedule flush for datasets with min LSN (Log Serial Number) < targetLSN
        for (DatasetResource dsr : datasets.values()) {
            PrimaryIndexOperationTracker opTracker = dsr.getOpTracker();
            synchronized (opTracker) {
                for (IndexInfo iInfo : dsr.getIndexes().values()) {
                    AbstractLSMIOOperationCallback ioCallback = (AbstractLSMIOOperationCallback) iInfo.getIndex()
                            .getIOOperationCallback();
                    if (!(((AbstractLSMIndex) iInfo.getIndex()).isCurrentMutableComponentEmpty()
                            || ioCallback.hasPendingFlush() || opTracker.isFlushLogCreated()
                            || opTracker.isFlushOnExit())) {
                        long firstLSN = ioCallback.getFirstLSN();
                        if (firstLSN < targetLSN) {
                            opTracker.setFlushOnExit(true);
                            if (opTracker.getNumActiveOperations() == 0) {
                                // No Modify operations currently, we need to trigger the flush and we can do so safely
                                opTracker.flushIfRequested();
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    /*
     * This method can only be called asynchronously safely if we're sure no modify operation will take place until the flush is scheduled
     */
    private void flushDatasetOpenIndexes(DatasetInfo dsInfo, boolean asyncFlush) throws HyracksDataException {
        if (!dsInfo.isExternal() && dsInfo.isDurable()) {
            synchronized (logRecord) {
                TransactionUtil.formFlushLogRecord(logRecord, dsInfo.getDatasetID(), null, logManager.getNodeId(),
                        dsInfo.getIndexes().size());
                try {
                    logManager.log(logRecord);
                } catch (ACIDException e) {
                    throw new HyracksDataException("could not write flush log while closing dataset", e);
                }

                try {
                    //notification will come from LogPage class (notifyFlushTerminator)
                    logRecord.wait();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }
            for (IndexInfo iInfo : dsInfo.getIndexes().values()) {
                //update resource lsn
                AbstractLSMIOOperationCallback ioOpCallback = (AbstractLSMIOOperationCallback) iInfo.getIndex()
                        .getIOOperationCallback();
                ioOpCallback.updateLastLSN(logRecord.getLSN());
            }
        }

        if (asyncFlush) {
            for (IndexInfo iInfo : dsInfo.getIndexes().values()) {
                ILSMIndexAccessor accessor = iInfo.getIndex().createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);
                accessor.scheduleFlush(iInfo.getIndex().getIOOperationCallback());
            }
        } else {
            for (IndexInfo iInfo : dsInfo.getIndexes().values()) {
                // TODO: This is not efficient since we flush the indexes sequentially.
                // Think of a way to allow submitting the flush requests concurrently. We don't do them concurrently because this
                // may lead to a deadlock scenario between the DatasetLifeCycleManager and the PrimaryIndexOperationTracker.
                flushAndWaitForIO(dsInfo, iInfo);
            }
        }
    }

    private void closeDataset(DatasetInfo dsInfo) throws HyracksDataException {
        // First wait for any ongoing IO operations
        synchronized (dsInfo) {
            while (dsInfo.getNumActiveIOOps() > 0) {
                try {
                    dsInfo.wait();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
        try {
            flushDatasetOpenIndexes(dsInfo, false);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        for (IndexInfo iInfo : dsInfo.getIndexes().values()) {
            if (iInfo.isOpen()) {
                ILSMOperationTracker opTracker = iInfo.getIndex().getOperationTracker();
                synchronized (opTracker) {
                    iInfo.getIndex().deactivate(false);
                }
                iInfo.setOpen(false);
            }
        }
        removeDatasetFromCache(dsInfo.getDatasetID());
        dsInfo.setOpen(false);
    }

    @Override
    public synchronized void closeAllDatasets() throws HyracksDataException {
        ArrayList<DatasetResource> openDatasets = new ArrayList<>(datasets.values());
        for (DatasetResource dsr : openDatasets) {
            closeDataset(dsr.getDatasetInfo());
        }
    }

    @Override
    public synchronized void closeUserDatasets() throws HyracksDataException {
        ArrayList<DatasetResource> openDatasets = new ArrayList<>(datasets.values());
        for (DatasetResource dsr : openDatasets) {
            if (dsr.getDatasetID() >= getFirstAvilableUserDatasetID()) {
                closeDataset(dsr.getDatasetInfo());
            }
        }
    }

    @Override
    public synchronized void stop(boolean dumpState, OutputStream outputStream) throws IOException {
        if (stopped) {
            return;
        }
        if (dumpState) {
            dumpState(outputStream);
        }

        closeAllDatasets();

        datasets.clear();
        stopped = true;
    }

    @Override
    public void dumpState(OutputStream outputStream) throws IOException {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("Memory budget = %d\n", capacity));
        sb.append(String.format("Memory used = %d\n", used));
        sb.append("\n");

        String dsHeaderFormat = "%-10s %-6s %-16s %-12s\n";
        String dsFormat = "%-10d %-6b %-16d %-12d\n";
        String idxHeaderFormat = "%-10s %-11s %-6s %-16s %-6s\n";
        String idxFormat = "%-10d %-11d %-6b %-16d %-6s\n";

        sb.append("[Datasets]\n");
        sb.append(String.format(dsHeaderFormat, "DatasetID", "Open", "Reference Count", "Last Access"));
        for (DatasetResource dsr : datasets.values()) {
            DatasetInfo dsInfo = dsr.getDatasetInfo();
            sb.append(
                    String.format(dsFormat, dsInfo.getDatasetID(), dsInfo.isOpen(), dsInfo.getReferenceCount(),
                            dsInfo.getLastAccess()));
        }
        sb.append("\n");

        sb.append("[Indexes]\n");
        sb.append(String.format(idxHeaderFormat, "DatasetID", "ResourceID", "Open", "Reference Count", "Index"));
        for (DatasetResource dsr : datasets.values()) {
            DatasetInfo dsInfo = dsr.getDatasetInfo();
            for (Map.Entry<Long, IndexInfo> entry : dsInfo.getIndexes().entrySet()) {
                IndexInfo iInfo = entry.getValue();
                sb.append(String.format(idxFormat, dsInfo.getDatasetID(), entry.getKey(), iInfo.isOpen(),
                        iInfo.getReferenceCount(),
                        iInfo.getIndex()));
            }
        }
        outputStream.write(sb.toString().getBytes());
    }

    private synchronized void deallocateDatasetMemory(int datasetId) throws HyracksDataException {
        DatasetResource dsr = datasets.get(datasetId);
        if (dsr == null) {
            throw new HyracksDataException(
                    "Failed to allocate memory for dataset with ID " + datasetId + " since it is not open.");
        }
        DatasetInfo dsInfo = dsr.getDatasetInfo();
        if (dsInfo == null) {
            throw new HyracksDataException(
                    "Failed to deallocate memory for dataset with ID " + datasetId + " since it is not open.");
        }
        synchronized (dsInfo) {
            if (dsInfo.isOpen() && dsInfo.isMemoryAllocated()) {
                used -= getVirtualBufferCaches(dsInfo.getDatasetID()).getTotalSize();
                dsInfo.setMemoryAllocated(false);
            }
        }
    }

    @Override
    public synchronized void allocateMemory(String resourcePath) throws HyracksDataException {
        //a resource name in the case of DatasetLifecycleManager is a dataset id which is passed to the ResourceHeapBufferAllocator.
        int datasetId = Integer.parseInt(resourcePath);
        DatasetResource dsr = datasets.get(datasetId);
        if (dsr == null) {
            throw new HyracksDataException(
                    "Failed to allocate memory for dataset with ID " + datasetId + " since it is not open.");
        }
        DatasetInfo dsInfo = dsr.getDatasetInfo();
        synchronized (dsInfo) {
            // This is not needed for external datasets' indexes since they never use the virtual buffer cache.
            if (!dsInfo.isMemoryAllocated() && !dsInfo.isExternal()) {
                long additionalSize = getVirtualBufferCaches(dsInfo.getDatasetID()).getTotalSize();
                while (used + additionalSize > capacity) {
                    if (!evictCandidateDataset()) {
                        throw new HyracksDataException("Cannot allocate dataset " + dsInfo.getDatasetID()
                                + " memory since memory budget would be exceeded.");
                    }
                }
                used += additionalSize;
                dsInfo.setMemoryAllocated(true);
            }
        }
    }

    public int getFirstAvilableUserDatasetID() {
        return firstAvilableUserDatasetID;
    }

    public int getNumPartitions() {
        return numPartitions;
    }
}
