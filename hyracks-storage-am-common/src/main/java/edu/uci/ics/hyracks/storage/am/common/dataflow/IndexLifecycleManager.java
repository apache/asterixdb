package edu.uci.ics.hyracks.storage.am.common.dataflow;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.common.file.IIndexArtifactMap;

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

    @Override
    public synchronized void create(IIndexDataflowHelper helper) throws HyracksDataException {
        IHyracksTaskContext ctx = helper.getHyracksTaskContext();
        IIOManager ioManager = ctx.getIOManager();
        IIndexArtifactMap indexArtifactMap = helper.getOperatorDescriptor().getStorageManager()
                .getIndexArtifactMap(ctx);
        IIndex index = helper.getIndexInstance();

        boolean generateResourceID = helper.getResourceID() == -1 ? true : false;
        if (generateResourceID) {
            try {
                indexArtifactMap.create(helper.getFileReference().getFile().getPath(), ioManager.getIODevices());
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        index.create();
    }

    @Override
    public synchronized void destroy(IIndexDataflowHelper helper) throws HyracksDataException {
        IHyracksTaskContext ctx = helper.getHyracksTaskContext();
        IIOManager ioManager = ctx.getIOManager();
        IIndexArtifactMap indexArtifactMap = helper.getOperatorDescriptor().getStorageManager()
                .getIndexArtifactMap(ctx);
        IIndex index = helper.getIndexInstance();

        indexArtifactMap.delete(helper.getFileReference().getFile().getPath(), ioManager.getIODevices());
        index.destroy();
    }

    @Override
    public synchronized IIndex open(IIndexDataflowHelper helper) throws HyracksDataException {
        long resourceID = helper.getResourceID();
        IndexInfo info = indexInfos.get(resourceID);

        if (info == null) {
            IIndex index = helper.getIndexInstance();
            if (memoryUsed + index.getInMemorySize() > memoryBudget) {
                if (!evictCandidateIndex()) {
                    throw new HyracksDataException("Cannot activate index since memory budget would be exceeded.");
                }
            }

            info = new IndexInfo(index, resourceID);
            indexInfos.put(resourceID, info);
            index.activate();
            memoryUsed += index.getInMemorySize();
        }

        info.touch();
        return info.index;
    }

    private boolean evictCandidateIndex() throws HyracksDataException {
        IndexInfo info = Collections.min(indexInfos.values());
        if (info.referenceCount != 0) {
            return false;
        }

        info.index.deactivate();
        indexInfos.remove(info.resourceID);

        return true;
    }

    @Override
    public synchronized void close(IIndexDataflowHelper helper) throws HyracksDataException {
        indexInfos.get(helper.getResourceID()).untouch();
    }

    private class IndexInfo implements Comparable<IndexInfo> {
        private final IIndex index;
        private final long resourceID;
        private int referenceCount;
        private long lastAccess;

        public IndexInfo(IIndex index, long resourceID) {
            this.index = index;
            this.resourceID = resourceID;
            this.lastAccess = -1;
            this.referenceCount = 0;
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
            // sort by (referenceCount, lastAccess), ascending
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

        public String toString() {
            return "{lastAccess: " + lastAccess + ", refCount: " + referenceCount + "}";
        }
    }
}