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
package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;

/**
 * Operation tracker that does nothing.
 * WARNING: This op tracker should only be used for specific testing purposes.
 * It is assumed than an op tracker cooperates with an lsm index to synchronize flushes with
 * regular operations, and this implementation does no such tracking at all.
 */
public class NoOpOperationTrackerProvider implements ILSMOperationTrackerProvider {
    private static final long serialVersionUID = 1L;

    public static NoOpOperationTrackerProvider INSTANCE = new NoOpOperationTrackerProvider();

    @Override
    public ILSMOperationTracker getOperationTracker(IHyracksTaskContext ctx) {
        return new ILSMOperationTracker() {

            @Override
            public void completeOperation(ILSMIndex index, LSMOperationType opType,
                    ISearchOperationCallback searchCallback, IModificationOperationCallback modificationCallback)
                    throws HyracksDataException {
                // Do nothing.
            }

            @Override
            public void beforeOperation(ILSMIndex index, LSMOperationType opType,
                    ISearchOperationCallback searchCallback, IModificationOperationCallback modificationCallback)
                    throws HyracksDataException {
            }

            @Override
            public void afterOperation(ILSMIndex index, LSMOperationType opType,
                    ISearchOperationCallback searchCallback, IModificationOperationCallback modificationCallback)
                    throws HyracksDataException {
                // Do nothing.                        
            }
        };
    }

    // Enforce singleton.
    private NoOpOperationTrackerProvider() {
    }

};
