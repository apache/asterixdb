/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.context;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.common.api.ILocalResourceMetadata;
import edu.uci.ics.asterix.common.config.AsterixStorageProperties;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.MultitenantVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;

public class DatasetLifecycleManager implements IIndexLifecycleManager, ILifeCycleComponent {
    private final AsterixStorageProperties storageProperties;
    private final Map<Integer, List<IVirtualBufferCache>> datasetVirtualBufferCaches;
    private final Map<Integer, ILSMOperationTracker> datasetOpTrackers;
    private final Map<Integer, DatasetInfo> datasetInfos;
    private final ILocalResourceRepository resourceRepository;
    private final int firstAvilableUserDatasetID;
    private final long capacity;
    private long used;

    public DatasetLifecycleManager(AsterixStorageProperties storageProperties,
            ILocalResourceRepository resourceRepository, int firstAvilableUserDatasetID) {
        this.storageProperties = storageProperties;
        this.resourceRepository = resourceRepository;
        this.firstAvilableUserDatasetID = firstAvilableUserDatasetID;
        datasetVirtualBufferCaches = new HashMap<Integer, List<IVirtualBufferCache>>();
        datasetOpTrackers = new HashMap<Integer, ILSMOperationTracker>();
        datasetInfos = new HashMap<Integer, DatasetInfo>();
        capacity = storageProperties.getMemoryComponentGlobalBudget();
        used = 0;
    }

    @Override
    public synchronized IIndex getIndex(long resourceID) throws HyracksDataException {
        DatasetInfo dsInfo = datasetInfos.get(getDIDfromRID(resourceID));
        if (dsInfo == null) {
            return null;
        }
        IndexInfo iInfo = dsInfo.indexes.get(resourceID);
        if (iInfo == null) {
            return null;
        }
        return iInfo.index;
    }

    @Override
    public synchronized void register(long resourceID, IIndex index) throws HyracksDataException {
        int did = getDIDfromRID(resourceID);
        DatasetInfo dsInfo = datasetInfos.get(did);
        if (dsInfo == null) {
            dsInfo = new DatasetInfo(did);
        } else if (dsInfo.indexes.containsKey(resourceID)) {
            throw new HyracksDataException("Index with resource ID " + resourceID + " already exists.");
        }
        datasetInfos.put(did, dsInfo);
        dsInfo.indexes.put(resourceID, new IndexInfo((ILSMIndex) index));
    }

    private int getDIDfromRID(long resourceID) throws HyracksDataException {
        LocalResource lr = resourceRepository.getResourceById(resourceID);
        if (lr == null) {
            return -1;
        }
        return ((ILocalResourceMetadata) lr.getResourceObject()).getDatasetID();
    }

    @Override
    public synchronized void unregister(long resourceID) throws HyracksDataException {
        int did = getDIDfromRID(resourceID);
        DatasetInfo dsInfo = datasetInfos.get(did);
        IndexInfo iInfo = dsInfo.indexes.get(resourceID);

        if (dsInfo == null || iInfo == null) {
            throw new HyracksDataException("Index with resource ID " + resourceID + " does not exist.");
        }

        PrimaryIndexOperationTracker opTracker = (PrimaryIndexOperationTracker) datasetOpTrackers.get(dsInfo.datasetID);
        if (iInfo.referenceCount != 0 || (opTracker != null && opTracker.getNumActiveOperations() != 0)) {
            throw new HyracksDataException("Cannot remove index while it is open.");
        }

        // TODO: use fine-grained counters, one for each index instead of a single counter per dataset.

        // First wait for any ongoing IO operations
        while (dsInfo.numActiveIOOps > 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }

        // Flush and wait for it to finish, it is separated from the above wait so they don't deadlock each other.
        // TODO: Find a better way to do this.
        flushAndWaitForIO(dsInfo, iInfo);

        if (iInfo.isOpen) {
            iInfo.index.deactivate(false);
        }

        dsInfo.indexes.remove(resourceID);
        if (dsInfo.referenceCount == 0 && dsInfo.isOpen && dsInfo.indexes.isEmpty()) {
            List<IVirtualBufferCache> vbcs = getVirtualBufferCaches(did);
            assert vbcs != null;
            for (IVirtualBufferCache vbc : vbcs) {
                used -= (vbc.getNumPages() * vbc.getPageSize());
            }
            datasetInfos.remove(did);
            datasetVirtualBufferCaches.remove(did);
            datasetOpTrackers.remove(did);
        }

    }

    public synchronized void declareActiveIOOperation(int datasetID) throws HyracksDataException {
        DatasetInfo dsInfo = datasetInfos.get(datasetID);
        if (dsInfo == null) {
            throw new HyracksDataException("Failed to find a dataset with ID " + datasetID);
        }
        dsInfo.incrementActiveIOOps();
    }

    public synchronized void undeclareActiveIOOperation(int datasetID) throws HyracksDataException {
        DatasetInfo dsInfo = datasetInfos.get(datasetID);
        if (dsInfo == null) {
            throw new HyracksDataException("Failed to find a dataset with ID " + datasetID);
        }
        dsInfo.decrementActiveIOOps();
        notifyAll();
    }

    @Override
    public synchronized void open(long resourceID) throws HyracksDataException {
        int did = getDIDfromRID(resourceID);
        DatasetInfo dsInfo = datasetInfos.get(did);
        if (dsInfo == null) {
            throw new HyracksDataException("Failed to open index with resource ID " + resourceID
                    + " since it does not exist.");
        }

        IndexInfo iInfo = dsInfo.indexes.get(resourceID);
        if (iInfo == null) {
            throw new HyracksDataException("Failed to open index with resource ID " + resourceID
                    + " since it does not exist.");
        }

        if (!dsInfo.isOpen) {
            List<IVirtualBufferCache> vbcs = getVirtualBufferCaches(did);
            assert vbcs != null;
            long additionalSize = 0;
            for (IVirtualBufferCache vbc : vbcs) {
                additionalSize += vbc.getNumPages() * vbc.getPageSize();
            }
            while (used + additionalSize > capacity) {
                if (!evictCandidateDataset()) {
                    throw new HyracksDataException("Cannot activate index since memory budget would be exceeded.");
                }
            }
            used += additionalSize;
        }

        dsInfo.isOpen = true;
        dsInfo.touch();
        if (!iInfo.isOpen) {
            iInfo.index.activate();
            iInfo.isOpen = true;
        }
        iInfo.touch();
    }

    private boolean evictCandidateDataset() throws HyracksDataException {
        // We will take a dataset that has no active transactions, it is open (a dataset consuming memory), 
        // that is not being used (refcount == 0) and has been least recently used. The sort order defined 
        // for DatasetInfo maintains this. See DatasetInfo.compareTo().

        List<DatasetInfo> datasetInfosList = new ArrayList<DatasetInfo>(datasetInfos.values());
        Collections.sort(datasetInfosList);
        for (DatasetInfo dsInfo : datasetInfosList) {
            PrimaryIndexOperationTracker opTracker = (PrimaryIndexOperationTracker) datasetOpTrackers
                    .get(dsInfo.datasetID);
            if (opTracker != null && opTracker.getNumActiveOperations() == 0 && dsInfo.referenceCount == 0
                    && dsInfo.isOpen) {

                // First wait for any ongoing IO operations
                while (dsInfo.numActiveIOOps > 0) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new HyracksDataException(e);
                    }
                }

                for (IndexInfo iInfo : dsInfo.indexes.values()) {
                    // TODO: This is not efficient since we flush the indexes sequentially. 
                    // Think of a way to allow submitting the flush requests concurrently. We don't do them concurrently because this
                    // may lead to a deadlock scenario between the DatasetLifeCycleManager and the PrimaryIndexOperationTracker.
                    flushAndWaitForIO(dsInfo, iInfo);
                }

                for (IndexInfo iInfo : dsInfo.indexes.values()) {
                    if (iInfo.isOpen) {
                        iInfo.index.deactivate(false);
                        iInfo.isOpen = false;
                    }
                    assert iInfo.referenceCount == 0;
                }
                dsInfo.isOpen = false;

                List<IVirtualBufferCache> vbcs = getVirtualBufferCaches(dsInfo.datasetID);
                for (IVirtualBufferCache vbc : vbcs) {
                    used -= vbc.getNumPages() * vbc.getPageSize();
                }
                return true;

            }
        }
        return false;
    }

    private void flushAndWaitForIO(DatasetInfo dsInfo, IndexInfo iInfo) throws HyracksDataException {
        if (iInfo.isOpen) {
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) iInfo.index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleFlush(iInfo.index.getIOOperationCallback());
        }
        // Wait for the above flush op.
        while (dsInfo.numActiveIOOps > 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public synchronized void close(long resourceID) throws HyracksDataException {
        int did = getDIDfromRID(resourceID);
        DatasetInfo dsInfo = datasetInfos.get(did);
        if (dsInfo == null) {
            throw new HyracksDataException("No index found with resourceID " + resourceID);
        }
        IndexInfo iInfo = dsInfo.indexes.get(resourceID);
        if (iInfo == null) {
            throw new HyracksDataException("No index found with resourceID " + resourceID);
        }
        iInfo.untouch();
        dsInfo.untouch();
    }

    @Override
    public synchronized List<IIndex> getOpenIndexes() {
        List<IIndex> openIndexes = new ArrayList<IIndex>();
        for (DatasetInfo dsInfo : datasetInfos.values()) {
            for (IndexInfo iInfo : dsInfo.indexes.values()) {
                if (iInfo.isOpen) {
                    openIndexes.add(iInfo.index);
                }
            }
        }
        return openIndexes;
    }

    public List<IVirtualBufferCache> getVirtualBufferCaches(int datasetID) {
        synchronized (datasetVirtualBufferCaches) {
            List<IVirtualBufferCache> vbcs = datasetVirtualBufferCaches.get(datasetID);
            if (vbcs == null) {
                vbcs = new ArrayList<IVirtualBufferCache>();
                int numPages = datasetID < firstAvilableUserDatasetID ? storageProperties
                        .getMetadataMemoryComponentNumPages() : storageProperties.getMemoryComponentNumPages();
                for (int i = 0; i < storageProperties.getMemoryComponentsNum(); i++) {
                    MultitenantVirtualBufferCache vbc = new MultitenantVirtualBufferCache(new VirtualBufferCache(
                            new HeapBufferAllocator(), storageProperties.getMemoryComponentPageSize(), numPages
                                    / storageProperties.getMemoryComponentsNum()));
                    vbcs.add(vbc);
                }
                datasetVirtualBufferCaches.put(datasetID, vbcs);
            }
            return vbcs;
        }
    }

    public ILSMOperationTracker getOperationTracker(int datasetID) {
        synchronized (datasetOpTrackers) {
            ILSMOperationTracker opTracker = datasetOpTrackers.get(datasetID);
            if (opTracker == null) {
                opTracker = new PrimaryIndexOperationTracker(this, datasetID);
                datasetOpTrackers.put(datasetID, opTracker);
            }

            return opTracker;
        }
    }

    public synchronized Set<ILSMIndex> getDatasetIndexes(int datasetID) throws HyracksDataException {
        DatasetInfo dsInfo = datasetInfos.get(datasetID);
        if (dsInfo == null) {
            throw new HyracksDataException("No dataset found with datasetID " + datasetID);
        }
        Set<ILSMIndex> datasetIndexes = new HashSet<ILSMIndex>();
        for (IndexInfo iInfo : dsInfo.indexes.values()) {
            if (iInfo.isOpen) {
                datasetIndexes.add(iInfo.index);
            }
        }
        return datasetIndexes;
    }

    private static abstract class Info {
        protected int referenceCount;
        protected boolean isOpen;

        public Info() {
            referenceCount = 0;
            isOpen = false;
        }

        public void touch() {
            ++referenceCount;
        }

        public void untouch() {
            --referenceCount;
        }
    }

    private static class IndexInfo extends Info {
        private ILSMIndex index;

        public IndexInfo(ILSMIndex index) {
            this.index = index;
        }
    }

    private static class DatasetInfo extends Info implements Comparable<DatasetInfo> {
        private final Map<Long, IndexInfo> indexes;
        private final int datasetID;
        private long lastAccess;
        private int numActiveIOOps;

        public DatasetInfo(int datasetID) {
            this.indexes = new HashMap<Long, IndexInfo>();
            this.lastAccess = -1;
            this.datasetID = datasetID;
        }

        public void touch() {
            super.touch();
            lastAccess = System.currentTimeMillis();
        }

        public void untouch() {
            super.untouch();
            lastAccess = System.currentTimeMillis();
        }

        public void incrementActiveIOOps() {
            numActiveIOOps++;
        }

        public void decrementActiveIOOps() {
            numActiveIOOps--;
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
            if (isOpen && !i.isOpen) {
                return -1;
            } else if (!isOpen && i.isOpen) {
                return 1;
            } else {
                if (referenceCount < i.referenceCount) {
                    return -1;
                } else if (referenceCount > i.referenceCount) {
                    return 1;
                } else {
                    if (lastAccess < i.lastAccess) {
                        return -1;
                    } else if (lastAccess > i.lastAccess) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            }

        }

        public String toString() {
            return "DatasetID: " + datasetID + ", isOpen: " + isOpen + ", refCount: " + referenceCount
                    + ", lastAccess: " + lastAccess + "}";
        }
    }

    @Override
    public void start() {
    }

    @Override
    public void stop(boolean dumpState, OutputStream outputStream) throws IOException {
        if (dumpState) {
            dumpState(outputStream);
        }

        List<IIndex> openIndexes = getOpenIndexes();
        for (IIndex index : openIndexes) {
            index.deactivate();
        }
        datasetVirtualBufferCaches.clear();
        datasetOpTrackers.clear();
        datasetInfos.clear();
    }

    private void dumpState(OutputStream outputStream) throws IOException {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("Memory budget = %d\n", capacity));
        sb.append(String.format("Memory used = %d\n", used));

        String headerFormat = "%-20s %-10s %-20s %-20s\n";
        String dsFormat = "%-20d %-10b %-20d %-20s %-20s\n";
        String idxFormat = "\t%-20d %-10b %-20d %-20s\n";
        sb.append(String.format(headerFormat, "DatasetID", "Open", "Reference Count", "Last Access"));
        for (DatasetInfo dsInfo : datasetInfos.values()) {
            sb.append(String
                    .format(dsFormat, dsInfo.datasetID, dsInfo.isOpen, dsInfo.referenceCount, dsInfo.lastAccess));
            for (Map.Entry<Long, IndexInfo> entry : dsInfo.indexes.entrySet()) {
                IndexInfo iInfo = entry.getValue();
                sb.append(String.format(idxFormat, entry.getKey(), iInfo.isOpen, iInfo.referenceCount, iInfo.index));
            }
        }
        outputStream.write(sb.toString().getBytes());
    }
}