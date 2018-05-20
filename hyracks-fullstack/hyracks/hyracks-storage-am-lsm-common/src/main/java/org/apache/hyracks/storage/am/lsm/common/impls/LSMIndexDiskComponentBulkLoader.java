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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.IIndexBulkLoader;

public class LSMIndexDiskComponentBulkLoader implements IIndexBulkLoader {
    private final AbstractLSMIndex lsmIndex;
    private final ILSMDiskComponentBulkLoader componentBulkLoader;
    private final ILSMIndexOperationContext opCtx;

    public LSMIndexDiskComponentBulkLoader(AbstractLSMIndex lsmIndex, ILSMIndexOperationContext opCtx, float fillFactor,
            boolean verifyInput, long numElementsHint) throws HyracksDataException {
        this.lsmIndex = lsmIndex;
        this.opCtx = opCtx;
        this.componentBulkLoader = opCtx.getIoOperation().getNewComponent().createBulkLoader(opCtx.getIoOperation(),
                fillFactor, verifyInput, numElementsHint, false, true, true);
    }

    public ILSMDiskComponent getComponent() {
        return opCtx.getIoOperation().getNewComponent();
    }

    @SuppressWarnings("squid:S1181")
    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        try {
            componentBulkLoader.add(tuple);
        } catch (Throwable th) {
            opCtx.getIoOperation().setFailure(th);
            throw th;
        }
    }

    @SuppressWarnings("squid:S1181")
    public void delete(ITupleReference tuple) throws HyracksDataException {
        try {
            componentBulkLoader.delete(tuple);
        } catch (Throwable th) {
            opCtx.getIoOperation().setFailure(th);
            throw th;
        }
    }

    @Override
    public void end() throws HyracksDataException {
        try {
            try {
                lsmIndex.getIOOperationCallback().afterOperation(opCtx.getIoOperation());
                componentBulkLoader.end();
            } catch (Throwable th) { // NOSONAR Must not call afterFinalize without setting failure
                opCtx.getIoOperation().setStatus(LSMIOOperationStatus.FAILURE);
                opCtx.getIoOperation().setFailure(th);
                throw th;
            } finally {
                lsmIndex.getIOOperationCallback().afterFinalize(opCtx.getIoOperation());
            }
            if (opCtx.getIoOperation().getStatus() == LSMIOOperationStatus.SUCCESS
                    && opCtx.getIoOperation().getNewComponent().getComponentSize() > 0) {
                lsmIndex.getHarness().addBulkLoadedComponent(opCtx.getIoOperation().getNewComponent());
            }
        } finally {
            lsmIndex.getIOOperationCallback().completed(opCtx.getIoOperation());
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        opCtx.getIoOperation().setStatus(LSMIOOperationStatus.FAILURE);
        try {
            try {
                componentBulkLoader.abort();
            } finally {
                lsmIndex.getIOOperationCallback().afterFinalize(opCtx.getIoOperation());
            }
        } finally {
            lsmIndex.getIOOperationCallback().completed(opCtx.getIoOperation());
        }
    }

}