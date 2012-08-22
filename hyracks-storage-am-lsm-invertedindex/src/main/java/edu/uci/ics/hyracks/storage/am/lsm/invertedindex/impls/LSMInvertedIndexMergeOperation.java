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

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex.LSMInvertedIndexComponent;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;

public class LSMInvertedIndexMergeOperation implements ILSMIOOperation {
    private final ILSMIndex index;
    private final List<Object> mergingComponents;
    private final IIndexCursor cursor;
    private final FileReference dictBTreeMergeTarget;
    private final FileReference deletedKeysBTreeMergeTarget;
    private final ILSMIOOperationCallback callback;

    public LSMInvertedIndexMergeOperation(ILSMIndex index, List<Object> mergingComponents, IIndexCursor cursor,
            FileReference dictBTreeMergeTarget, FileReference deletedKeysBTreeMergeTarget, ILSMIOOperationCallback callback) {
        this.index = index;
        this.mergingComponents = mergingComponents;
        this.cursor = cursor;
        this.dictBTreeMergeTarget = dictBTreeMergeTarget;
        this.deletedKeysBTreeMergeTarget = deletedKeysBTreeMergeTarget;
        this.callback = callback;
    }

    @Override
    public List<IODeviceHandle> getReadDevices() {
        List<IODeviceHandle> devs = new ArrayList<IODeviceHandle>();
        for (Object o : mergingComponents) {
            LSMInvertedIndexComponent component = (LSMInvertedIndexComponent) o;
            OnDiskInvertedIndex invIndex = (OnDiskInvertedIndex) component.getInvIndex();
            devs.add(invIndex.getBTree().getFileReference().getDevideHandle());
            devs.add(component.getDeletedKeysBTree().getFileReference().getDevideHandle());
        }
        return devs;
    }

    @Override
    public List<IODeviceHandle> getWriteDevices() {
        List<IODeviceHandle> devs = new ArrayList<IODeviceHandle>(2);
        devs.add(dictBTreeMergeTarget.getDevideHandle());
        devs.add(deletedKeysBTreeMergeTarget.getDevideHandle());
        return devs;
    }

    @Override
    public void perform() throws HyracksDataException, IndexException {
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
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

    public List<Object> getMergingComponents() {
        return mergingComponents;
    }

}