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

package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;

public class LSMBTreeMergeOperation implements ILSMIOOperation {

    private final ILSMIndexAccessorInternal accessor;
    private final List<ILSMComponent> mergingComponents;
    private final ITreeIndexCursor cursor;
    private final FileReference btreeMergeTarget;
    private final FileReference bloomFilterMergeTarget;
    private final ILSMIOOperationCallback callback;
    private final String indexIdentifier;

    public LSMBTreeMergeOperation(ILSMIndexAccessorInternal accessor, List<ILSMComponent> mergingComponents,
            ITreeIndexCursor cursor, FileReference btreeMergeTarget, FileReference bloomFilterMergeTarget,
            ILSMIOOperationCallback callback, String indexIdentifier) {
        this.accessor = accessor;
        this.mergingComponents = mergingComponents;
        this.cursor = cursor;
        this.btreeMergeTarget = btreeMergeTarget;
        this.bloomFilterMergeTarget = bloomFilterMergeTarget;
        this.callback = callback;
        this.indexIdentifier = indexIdentifier;
    }

    @Override
    public Set<IODeviceHandle> getReadDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>();
        for (ILSMComponent o : mergingComponents) {
            LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) o;
            devs.add(component.getBTree().getFileReference().getDeviceHandle());
            devs.add(component.getBloomFilter().getFileReference().getDeviceHandle());
        }
        return devs;
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>();
        devs.add(btreeMergeTarget.getDeviceHandle());
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

    public FileReference getBTreeMergeTarget() {
        return btreeMergeTarget;
    }

    public FileReference getBloomFilterMergeTarget() {
        return bloomFilterMergeTarget;
    }

    public ITreeIndexCursor getCursor() {
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
