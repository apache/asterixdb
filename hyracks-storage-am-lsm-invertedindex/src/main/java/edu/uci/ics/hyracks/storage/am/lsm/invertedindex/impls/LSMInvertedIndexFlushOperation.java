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

import java.util.Collections;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public class LSMInvertedIndexFlushOperation implements ILSMIOOperation {
    private final ILSMIndex index;
    private final FileReference dictBTreeFlushTarget;
    private final FileReference deletedKeysBTreeFlushTarget;
    private final ILSMIOOperationCallback callback;

    public LSMInvertedIndexFlushOperation(ILSMIndex index, FileReference dictBTreeFlushTarget,
            FileReference deletedKeysBTreeFlushTarget, ILSMIOOperationCallback callback) {
        this.index = index;
        this.dictBTreeFlushTarget = dictBTreeFlushTarget;
        this.deletedKeysBTreeFlushTarget = deletedKeysBTreeFlushTarget;
        this.callback = callback;
    }

    @Override
    public List<IODeviceHandle> getReadDevices() {
        return Collections.emptyList();
    }

    @Override
    public List<IODeviceHandle> getWriteDevices() {
        return Collections.singletonList(dictBTreeFlushTarget.getDevideHandle());
    }

    @Override
    public void perform() throws HyracksDataException, IndexException {
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        accessor.flush(this);
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return callback;
    }

    public FileReference getDictBTreeFlushTarget() {
        return dictBTreeFlushTarget;
    }

    public FileReference getDeletedKeysBTreeFlushTarget() {
        return deletedKeysBTreeFlushTarget;
    }
}
