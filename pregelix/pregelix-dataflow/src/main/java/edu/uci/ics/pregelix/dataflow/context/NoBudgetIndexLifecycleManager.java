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
package edu.uci.ics.pregelix.dataflow.context;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;

public class NoBudgetIndexLifecycleManager implements IIndexLifecycleManager, ILifeCycleComponent {

    private final Map<Long, IndexInfo> indexInfos;

    public NoBudgetIndexLifecycleManager() {
        this.indexInfos = new HashMap<Long, IndexInfo>();
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
        }
    }

    @Override
    public void open(long resourceID) throws HyracksDataException {
        IndexInfo info = indexInfos.get(resourceID);
        if (info == null) {
            throw new HyracksDataException("Failed to open index with resource ID " + resourceID
                    + " since it does not exist.");
        }

        if (!info.isOpen) {
            info.index.activate();
            info.isOpen = true;
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
            // sort by (isOpen, referenceCount, lastAccess) ascending, where
            // true < false
            //
            // Example sort order:
            // -------------------
            // (F, 0, 70) <-- largest
            // (F, 0, 60)
            // (T, 10, 80)
            // (T, 10, 70)
            // (T, 9, 90)
            // (T, 0, 100) <-- smallest
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

    @Override
    public List<IIndex> getOpenIndexes() {
        List<IIndex> openIndexes = new ArrayList<IIndex>();
        for (IndexInfo i : indexInfos.values()) {
            if (i.isOpen) {
                openIndexes.add(i.index);
            }
        }
        return openIndexes;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop(boolean dumpState, OutputStream outputStream) throws IOException {
        if (dumpState) {
            dumpState(outputStream);
        }

        for (IndexInfo i : indexInfos.values()) {
            if (i.isOpen) {
                i.index.deactivate();
            }
        }
    }

    private void dumpState(OutputStream os) throws IOException {
        StringBuilder sb = new StringBuilder();

        String headerFormat = "%-20s %-10s %-20s %-20s %-20s\n";
        String rowFormat = "%-20d %-10b %-20d %-20s %-20s\n";
        sb.append(String.format(headerFormat, "ResourceID", "Open", "Reference Count", "Last Access", "Index Name"));
        IndexInfo ii;
        for (Map.Entry<Long, IndexInfo> entry : indexInfos.entrySet()) {
            ii = entry.getValue();
            sb.append(String.format(rowFormat, entry.getKey(), ii.isOpen, ii.referenceCount, ii.lastAccess, ii.index));
        }
        os.write(sb.toString().getBytes());
    }
}