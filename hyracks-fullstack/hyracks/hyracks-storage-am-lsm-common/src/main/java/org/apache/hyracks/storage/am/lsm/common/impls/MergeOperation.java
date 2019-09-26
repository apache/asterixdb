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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.lsm.common.api.AbstractLSMWithBloomFilterDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;

public abstract class MergeOperation extends AbstractIoOperation {
    protected final IIndexCursor cursor;
    protected final IIndexCursorStats stats;
    protected final long totalPages;

    public MergeOperation(ILSMIndexAccessor accessor, FileReference target, ILSMIOOperationCallback callback,
            String indexIdentifier, IIndexCursor cursor, IIndexCursorStats stats) {
        super(accessor, target, callback, indexIdentifier);
        this.cursor = cursor;
        this.stats = stats;
        this.totalPages = computeTotalComponentPages(accessor);
    }

    public List<ILSMComponent> getMergingComponents() {
        return accessor.getOpContext().getComponentHolder();
    }

    @Override
    public LSMIOOperationStatus call() throws HyracksDataException {
        accessor.merge(this);
        return getStatus();
    }

    @Override
    public LSMIOOperationType getIOOpertionType() {
        return LSMIOOperationType.MERGE;
    }

    public IIndexCursor getCursor() {
        return cursor;
    }

    private long computeTotalComponentPages(ILSMIndexAccessor accessor) {
        List<ILSMDiskComponent> components = accessor.getOpContext().getComponentsToBeMerged();
        long totalSize = 0;
        for (ILSMDiskComponent component : components) {
            long componentSize = component.getComponentSize();
            if (component instanceof AbstractLSMWithBloomFilterDiskComponent) {
                // exclude the size of bloom filters since we do not scan them during merge
                componentSize -= ((AbstractLSMWithBloomFilterDiskComponent) component).getBloomFilter()
                        .getFileReference().getFile().length();
            }
            totalSize += componentSize;
        }
        return totalSize / accessor.getOpContext().getIndex().getBufferCache().getPageSize();
    }

    public long getRemainingPages() {
        return totalPages - stats.getPageCounter().get();
    }

    public long getTotalPages() {
        return totalPages;
    }

    public IIndexCursorStats getCursorStats() {
        return stats;
    }
}
