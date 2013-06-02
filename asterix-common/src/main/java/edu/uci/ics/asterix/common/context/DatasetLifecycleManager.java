package edu.uci.ics.asterix.common.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.common.api.ILocalResourceMetadata;
import edu.uci.ics.asterix.common.config.AsterixStorageProperties;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.MultitenantVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;

public class DatasetLifecycleManager implements IIndexLifecycleManager {
    private final AsterixStorageProperties storageProperties;
    private final Map<Integer, MultitenantVirtualBufferCache> datasetVirtualBufferCaches;
    private final Map<Integer, DatasetInfo> datasetInfos;
    private final ILocalResourceRepository resourceRepository;
    private final long capacity;
    private long used;

    public DatasetLifecycleManager(AsterixStorageProperties storageProperties,
            ILocalResourceRepository resourceRepository) {
        this.storageProperties = storageProperties;
        this.resourceRepository = resourceRepository;
        datasetVirtualBufferCaches = new HashMap<Integer, MultitenantVirtualBufferCache>();
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
        IndexInfo iInfo = dsInfo.indexes.remove(resourceID);
        if (dsInfo == null || iInfo == null) {
            throw new HyracksDataException("Index with resource ID " + resourceID + " does not exist.");
        }

        if (iInfo.referenceCount != 0) {
            dsInfo.indexes.put(resourceID, iInfo);
            throw new HyracksDataException("Cannot remove index while it is open.");
        }

        if (iInfo.isOpen) {
            iInfo.index.deactivate(true);
        }

        if (dsInfo.referenceCount == 0 && dsInfo.isOpen && dsInfo.indexes.isEmpty()) {
            IVirtualBufferCache vbc = getVirtualBufferCache(did);
            assert vbc != null;
            used -= (vbc.getNumPages() * vbc.getPageSize());
            datasetInfos.remove(did);
        }

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
            IVirtualBufferCache vbc = getVirtualBufferCache(did);
            assert vbc != null;
            long additionalSize = vbc.getNumPages() * vbc.getPageSize();
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
        // Why min()? As a heuristic for eviction, we will take an open index (an index consuming memory) 
        // that is not being used (refcount == 0) and has been least recently used. The sort order defined 
        // for IndexInfo maintains this. See IndexInfo.compareTo().
        DatasetInfo dsInfo = Collections.min(datasetInfos.values());
        if (dsInfo.referenceCount == 0 && dsInfo.isOpen) {
            for (IndexInfo iInfo : dsInfo.indexes.values()) {
                if (iInfo.isOpen) {
                    iInfo.index.deactivate(true);
                    iInfo.isOpen = false;
                }
                assert iInfo.referenceCount == 0;
            }

            IVirtualBufferCache vbc = getVirtualBufferCache(dsInfo.datasetID);
            used -= vbc.getNumPages() * vbc.getPageSize();
            dsInfo.isOpen = false;
            return true;
        }
        return false;
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

    public IVirtualBufferCache getVirtualBufferCache(int datasetID) {
        synchronized (datasetVirtualBufferCaches) {
            MultitenantVirtualBufferCache vbc = datasetVirtualBufferCaches.get(datasetID);
            if (vbc == null) {
                vbc = new MultitenantVirtualBufferCache(new VirtualBufferCache(new HeapBufferAllocator(),
                        storageProperties.getMemoryComponentPageSize(), storageProperties.getMemoryComponentNumPages()));
                datasetVirtualBufferCaches.put(datasetID, vbc);
            }
            return vbc;
        }
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

}
