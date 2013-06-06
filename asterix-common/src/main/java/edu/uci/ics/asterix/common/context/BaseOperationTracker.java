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
package edu.uci.ics.asterix.common.context;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMOperationType;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;

public class BaseOperationTracker implements ILSMOperationTracker {

    protected final ILSMIOOperationCallback ioOpCallback;
    protected long lastLSN;
    protected long firstLSN;

    public BaseOperationTracker(ILSMIOOperationCallbackFactory ioOpCallbackFactory) {
        this.ioOpCallback = ioOpCallbackFactory == null ? NoOpIOOperationCallback.INSTANCE : ioOpCallbackFactory
                .createIOOperationCallback(this);
        resetLSNs();
    }

    public ILSMIOOperationCallback getIOOperationCallback() {
        return ioOpCallback;
    }

    public long getLastLSN() {
        return lastLSN;
    }

    public long getFirstLSN() {
        return firstLSN;
    }

    public void updateLastLSN(long lastLSN) {
        if (firstLSN == -1) {
            firstLSN = lastLSN;
        }
        this.lastLSN = Math.max(this.lastLSN, lastLSN);
    }

    public void resetLSNs() {
        lastLSN = -1;
        firstLSN = -1;
    }

    @Override
    public void beforeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
    }

    @Override
    public void afterOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
    }

    @Override
    public void completeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
    }

    public void exclusiveJobCommitted() throws HyracksDataException {
    }
}
