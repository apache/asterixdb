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

package edu.uci.ics.hyracks.storage.am.lsm.btree.impls;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;

public class LSMBTreeMergeOperation implements ILSMIOOperation {

    private final ILSMIndexAccessorInternal accessor;
    private final List<ILSMComponent> mergingComponents;
    private final ITreeIndexCursor cursor;
    private final FileReference mergeTarget;
    private final ILSMIOOperationCallback callback;

    public LSMBTreeMergeOperation(ILSMIndexAccessorInternal accessor, List<ILSMComponent> mergingComponents,
            ITreeIndexCursor cursor, FileReference mergeTarget, ILSMIOOperationCallback callback) {
        this.accessor = accessor;
        this.mergingComponents = mergingComponents;
        this.cursor = cursor;
        this.mergeTarget = mergeTarget;
        this.callback = callback;
    }

    @Override
    public Set<IODeviceHandle> getReadDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>();
        for (ILSMComponent o : mergingComponents) {
            LSMBTreeImmutableComponent component = (LSMBTreeImmutableComponent) o;
            devs.add(component.getBTree().getFileReference().getDeviceHandle());
        }
        return devs;
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        return Collections.singleton(mergeTarget.getDeviceHandle());
    }

    @Override
    public void perform() throws HyracksDataException, IndexException {
        accessor.merge(this);
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return callback;
    }

    public FileReference getMergeTarget() {
        return mergeTarget;
    }

    public ITreeIndexCursor getCursor() {
        return cursor;
    }

    public List<ILSMComponent> getMergingComponents() {
        return mergingComponents;
    }
}
