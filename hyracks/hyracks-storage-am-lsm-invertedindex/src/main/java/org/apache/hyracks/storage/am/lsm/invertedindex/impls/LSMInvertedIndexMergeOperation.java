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

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;

public class LSMInvertedIndexMergeOperation implements ILSMIOOperation {
    private final ILSMIndexAccessorInternal accessor;
    private final List<ILSMComponent> mergingComponents;
    private final IIndexCursor cursor;
    private final FileReference dictBTreeMergeTarget;
    private final FileReference deletedKeysBTreeMergeTarget;
    private final FileReference bloomFilterMergeTarget;
    private final ILSMIOOperationCallback callback;
    private final String indexIdentifier;

    public LSMInvertedIndexMergeOperation(ILSMIndexAccessorInternal accessor, List<ILSMComponent> mergingComponents,
            IIndexCursor cursor, FileReference dictBTreeMergeTarget, FileReference deletedKeysBTreeMergeTarget,
            FileReference bloomFilterMergeTarget, ILSMIOOperationCallback callback, String indexIdentifier) {
        this.accessor = accessor;
        this.mergingComponents = mergingComponents;
        this.cursor = cursor;
        this.dictBTreeMergeTarget = dictBTreeMergeTarget;
        this.deletedKeysBTreeMergeTarget = deletedKeysBTreeMergeTarget;
        this.bloomFilterMergeTarget = bloomFilterMergeTarget;
        this.callback = callback;
        this.indexIdentifier = indexIdentifier;
    }

    @Override
    public Set<IODeviceHandle> getReadDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>();
        for (Object o : mergingComponents) {
            LSMInvertedIndexDiskComponent component = (LSMInvertedIndexDiskComponent) o;
            OnDiskInvertedIndex invIndex = (OnDiskInvertedIndex) component.getInvIndex();
            devs.add(invIndex.getBTree().getFileReference().getDeviceHandle());
            devs.add(component.getDeletedKeysBTree().getFileReference().getDeviceHandle());
            devs.add(component.getBloomFilter().getFileReference().getDeviceHandle());
        }
        return devs;
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>();
        devs.add(dictBTreeMergeTarget.getDeviceHandle());
        devs.add(deletedKeysBTreeMergeTarget.getDeviceHandle());
        devs.add(bloomFilterMergeTarget.getDeviceHandle());
        return devs;
    }

    @Override
    public Boolean call() throws HyracksDataException, IndexException {
        accessor.merge(this);
        return true;
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return callback;
    }

    public FileReference getDictBTreeMergeTarget() {
        return dictBTreeMergeTarget;
    }

    public FileReference getDeletedKeysBTreeMergeTarget() {
        return deletedKeysBTreeMergeTarget;
    }

    public FileReference getBloomFilterMergeTarget() {
        return bloomFilterMergeTarget;
    }

    public IIndexCursor getCursor() {
        return cursor;
    }

    public List<ILSMComponent> getMergingComponents() {
        return mergingComponents;
    }

    @Override
    public String getIndexUniqueIdentifier() {
        return indexIdentifier;
    }

    @Override
    public LSMIOOpertionType getIOOpertionType() {
        return LSMIOOpertionType.MERGE;
    }
}