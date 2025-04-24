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
package org.apache.hyracks.storage.am.common.dataflow;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;

public class IndexLifecycleManager implements IResourceLifecycleManager<IIndex>, ILifeCycleComponent {
    private static final long DEFAULT_MEMORY_BUDGET = 1024 * 1024 * 100; // 100 megabytes

    private final Map<String, IndexInfo> indexInfos;
    private final long memoryBudget;
    private long memoryUsed;

    public IndexLifecycleManager() {
        this(DEFAULT_MEMORY_BUDGET);
    }

    public IndexLifecycleManager(long memoryBudget) {
        this.indexInfos = new HashMap<>();
        this.memoryBudget = memoryBudget;
        this.memoryUsed = 0;
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

        @Override
        public String toString() {
            return "{index: " + index + ", isOpen: " + isOpen + ", refCount: " + referenceCount + ", lastAccess: "
                    + lastAccess + "}";
        }
    }

    @Override
    public List<IIndex> getOpenResources() {
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

    @Override
    public void dumpState(OutputStream os) throws IOException {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("Memory budget = %d\n", memoryBudget));
        sb.append(String.format("Memory used = %d\n", memoryUsed));

        String headerFormat = "%-20s %-10s %-20s %-20s %-20s\n";
        String rowFormat = "%-20d %-10b %-20d %-20s %-20s\n";
        sb.append(String.format(headerFormat, "ResourceName", "Open", "Reference Count", "Last Access", "Index Name"));
        IndexInfo ii;
        for (Map.Entry<String, IndexInfo> entry : indexInfos.entrySet()) {
            ii = entry.getValue();
            sb.append(String.format(rowFormat, entry.getKey(), ii.isOpen, ii.referenceCount, ii.lastAccess, ii.index));
        }
        os.write(sb.toString().getBytes());
    }

    @Override
    public void register(String resourcePath, IIndex index) throws HyracksDataException {
        if (indexInfos.containsKey(resourcePath)) {
            throw new HyracksDataException("Index with resource name " + resourcePath + " already exists.");
        }
        indexInfos.put(resourcePath, new IndexInfo(index));
    }

    @Override
    public void open(String resourcePath) throws HyracksDataException {
        IndexInfo info = indexInfos.get(resourcePath);
        if (info == null) {
            throw new HyracksDataException(
                    "Failed to open index with resource name " + resourcePath + " since it does not exist.");
        }

        if (!info.isOpen) {
            info.index.activate();
            info.isOpen = true;
        }
        info.touch();
    }

    @Override
    public void close(String resourcePath) throws HyracksDataException {
        indexInfos.get(resourcePath).untouch();
    }

    @Override
    public IIndex get(String resourcePath) throws HyracksDataException {
        IndexInfo info = indexInfos.get(resourcePath);
        return info == null ? null : info.index;
    }

    @Override
    public void unregister(String resourcePath) throws HyracksDataException {
        IndexInfo info = indexInfos.get(resourcePath);
        if (info == null) {
            throw HyracksDataException.create(ErrorCode.INDEX_DOES_NOT_EXIST, resourcePath);
        }

        if (info.referenceCount != 0) {
            indexInfos.put(resourcePath, info);
            throw HyracksDataException.create(ErrorCode.CANNOT_DROP_IN_USE_INDEX, resourcePath);
        }

        if (info.isOpen) {
            info.index.deactivate();
        }
        indexInfos.remove(resourcePath);
    }
}
