/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;

public class LSMInvertedIndexMergeOperation implements ILSMIOOperation {
    private final ILSMIndexAccessorInternal accessor;
    private final List<ILSMComponent> mergingComponents;
    private final IIndexCursor cursor;
    private final FileReference dictBTreeMergeTarget;
    private final FileReference deletedKeysBTreeMergeTarget;
    private final ILSMIOOperationCallback callback;

    public LSMInvertedIndexMergeOperation(ILSMIndexAccessorInternal accessor, List<ILSMComponent> mergingComponents,
            IIndexCursor cursor, FileReference dictBTreeMergeTarget, FileReference deletedKeysBTreeMergeTarget,
            ILSMIOOperationCallback callback) {
        this.accessor = accessor;
        this.mergingComponents = mergingComponents;
        this.cursor = cursor;
        this.dictBTreeMergeTarget = dictBTreeMergeTarget;
        this.deletedKeysBTreeMergeTarget = deletedKeysBTreeMergeTarget;
        this.callback = callback;
    }

    @Override
    public Set<IODeviceHandle> getReadDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>();
        for (Object o : mergingComponents) {
            LSMInvertedIndexImmutableComponent component = (LSMInvertedIndexImmutableComponent) o;
            OnDiskInvertedIndex invIndex = (OnDiskInvertedIndex) component.getInvIndex();
            devs.add(invIndex.getBTree().getFileReference().getDeviceHandle());
            devs.add(component.getDeletedKeysBTree().getFileReference().getDeviceHandle());
        }
        return devs;
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>(2);
        devs.add(dictBTreeMergeTarget.getDeviceHandle());
        devs.add(deletedKeysBTreeMergeTarget.getDeviceHandle());
        return devs;
    }

    @Override
    public void perform() throws HyracksDataException, IndexException {
        accessor.merge(this);
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

    public IIndexCursor getCursor() {
        return cursor;
    }

    public List<ILSMComponent> getMergingComponents() {
        return mergingComponents;
    }

}