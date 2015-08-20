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
package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;

public class LSMRTreeFlushOperation implements ILSMIOOperation, Comparable<LSMRTreeFlushOperation> {

    private final ILSMIndexAccessorInternal accessor;
    private final ILSMComponent flushingComponent;
    private final FileReference rtreeFlushTarget;
    private final FileReference btreeFlushTarget;
    private final FileReference bloomFilterFlushTarget;
    private final ILSMIOOperationCallback callback;
    private final String indexIdentifier;

    public LSMRTreeFlushOperation(ILSMIndexAccessorInternal accessor, ILSMComponent flushingComponent,
            FileReference rtreeFlushTarget, FileReference btreeFlushTarget, FileReference bloomFilterFlushTarget,
            ILSMIOOperationCallback callback, String indexIdentifier) {
        this.accessor = accessor;
        this.flushingComponent = flushingComponent;
        this.rtreeFlushTarget = rtreeFlushTarget;
        this.btreeFlushTarget = btreeFlushTarget;
        this.bloomFilterFlushTarget = bloomFilterFlushTarget;
        this.callback = callback;
        this.indexIdentifier = indexIdentifier;
    }

    @Override
    public Set<IODeviceHandle> getReadDevices() {
        return Collections.emptySet();
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>();
        devs.add(rtreeFlushTarget.getDeviceHandle());
        if (btreeFlushTarget != null) {
            devs.add(btreeFlushTarget.getDeviceHandle());
            devs.add(bloomFilterFlushTarget.getDeviceHandle());
        }
        return devs;
    }

    @Override
    public Boolean call() throws HyracksDataException, IndexException {
        accessor.flush(this);
        return true;
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return callback;
    }

    public FileReference getRTreeFlushTarget() {
        return rtreeFlushTarget;
    }

    public FileReference getBTreeFlushTarget() {
        return btreeFlushTarget;
    }

    public FileReference getBloomFilterFlushTarget() {
        return bloomFilterFlushTarget;
    }

    public ILSMComponent getFlushingComponent() {
        return flushingComponent;
    }

    @Override
    public String getIndexUniqueIdentifier() {
        return indexIdentifier;
    }

    @Override
    public LSMIOOpertionType getIOOpertionType() {
        return LSMIOOpertionType.FLUSH;
    }

    @Override
    public int compareTo(LSMRTreeFlushOperation o) {
        return rtreeFlushTarget.getFile().getName().compareTo(o.getRTreeFlushTarget().getFile().getName());
    }
}
