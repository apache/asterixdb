package edu.uci.ics.hyracks.storage.am.common.dataflow;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;

public class IndexLifecycleManager implements IIndexLifecycleManager {
    private static final long DEFAULT_MEMORY_BUDGET = 1024 * 1024 * 100; // 100 megabytes

    private final Map<Long, IndexInfo> indexInfos;
    private final long memoryBudget;

    private long memoryUsed;

    public IndexLifecycleManager() {
        this(DEFAULT_MEMORY_BUDGET);
    }

    public IndexLifecycleManager(long memoryBudget) {
        this.indexInfos = new HashMap<Long, IndexInfo>();
        this.memoryBudget = memoryBudget;
        this.memoryUsed = 0;
    }

    private boolean evictCandidateIndex() throws HyracksDataException {
        // Why min()? As a heuristic for eviction, we will take an open index (an index consuming memory) 
        // that is not being used (refcount == 0) and has been least recently used. The sort order defined 
        // for IndexInfo maintains this. See IndexInfo.compareTo().
        IndexInfo info = Collections.min(indexInfos.values());
        if (info.referenceCount != 0 || !info.isOpen) {
            return false;
        }

        info.index.deactivate();
        memoryUsed -= info.index.getMemoryAllocationSize();
        info.isOpen = false;

        return true;
    }

    @Override
    public IIndex getIndex(long resourceID) {
        IndexInfo info = indexInfos.get(resourceID);
        return info == null ? null : info.index;
    }

    @Override
    public void register(long resourceID, IIndex index) throws HyracksDataException {
        if (indexInfos.containsKey(resourceID)) {
            throw new HyracksDataException("Index with resource ID " + resourceID + " already exists.");
        }

        indexInfos.put(resourceID, new IndexInfo(index));
    }

    @Override
    public void unregister(long resourceID) throws HyracksDataException {
        IndexInfo info = indexInfos.remove(resourceID);
        if (info == null) {
            throw new HyracksDataException("Index with resource ID " + resourceID + " does not exist.");
        }

        if (info.referenceCount != 0) {
            indexInfos.put(resourceID, info);
            throw new HyracksDataException("Cannot remove index while it is open.");
        }

        if (info.isOpen) {
            info.index.deactivate();
            memoryUsed -= info.index.getMemoryAllocationSize();
        }
    }

    @Override
    public void open(long resourceID) throws HyracksDataException {
        IndexInfo info = indexInfos.get(resourceID);
        if (info == null) {
            throw new HyracksDataException("Failed to open index with resource ID " + resourceID
                    + " since it does not exist.");
        }

        long inMemorySize = info.index.getMemoryAllocationSize();
        while (memoryUsed + inMemorySize > memoryBudget) {
            if (!evictCandidateIndex()) {
                throw new HyracksDataException("Cannot activate index since memory budget would be exceeded.");
            }
        }

        if (!info.isOpen) {
            info.index.activate();
            info.isOpen = true;
            memoryUsed += inMemorySize;
        }
        info.touch();
    }

    @Override
    public void close(long resourceID) {
        indexInfos.get(resourceID).untouch();
    }

    private class IndexInfo implements Comparable<IndexInfo> {
        private final IIndex index;
        private int referenceCount;
        private long lastAccess;
        private boolean isOpen;

        public IndexInfo(IIndex index) {
            this.index = index;
            this.lastAccess = -1;
            this.referenceCount = 0;
            this.isOpen = false;
        }

        public void touch() {
            lastAccess = System.currentTimeMillis();
            referenceCount++;
        }

        public void untouch() {
            lastAccess = System.currentTimeMillis();
            referenceCount--;
        }

        @Override
        public int compareTo(IndexInfo i) {
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
            return "{index: " + index + ", isOpen: " + isOpen + ", refCount: " + referenceCount + ", lastAccess: "
                    + lastAccess + "}";
        }
    }
}