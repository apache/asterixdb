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

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.ISearchOperationCallback;

/**
 * Operation tracker that does nothing.
 * WARNING: This op tracker should only be used for specific testing purposes.
 * It is assumed than an op tracker cooperates with an lsm index to synchronize flushes with
 * regular operations, and this implementation does no such tracking at all.
 */
public class NoOpOperationTrackerFactory implements ILSMOperationTrackerFactory {
    private static final long serialVersionUID = 1L;
    public static final NoOpOperationTrackerFactory INSTANCE = new NoOpOperationTrackerFactory();
    private static final NoOpOperationTracker tracker = new NoOpOperationTracker();

    // Enforce singleton.
    private NoOpOperationTrackerFactory() {
    }

    @Override
    public ILSMOperationTracker getOperationTracker(INCServiceContext ctx, IResource resource) {
        return tracker;
    }

    private static final class NoOpOperationTracker implements ILSMOperationTracker {

        @Override
        public void beforeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
                IModificationOperationCallback modificationCallback) throws HyracksDataException {
            // No Op
        }

        @Override
        public void afterOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
                IModificationOperationCallback modificationCallback) throws HyracksDataException {
            // No Op
        }

        @Override
        public void completeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
                IModificationOperationCallback modificationCallback) throws HyracksDataException {
            // No Op
        }
    }
}
