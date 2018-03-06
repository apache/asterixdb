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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.IIndexBulkLoader;

public class LSMIndexDiskComponentBulkLoader implements IIndexBulkLoader {
    private final AbstractLSMIndex lsmIndex;
    private final ILSMDiskComponentBulkLoader componentBulkLoader;
    private ILSMIndexOperationContext opCtx;

    public LSMIndexDiskComponentBulkLoader(AbstractLSMIndex lsmIndex, ILSMIndexOperationContext opCtx, float fillFactor,
            boolean verifyInput, long numElementsHint) throws HyracksDataException {
        this.lsmIndex = lsmIndex;
        this.opCtx = opCtx;
        // Note that by using a flush target file name, we state that the
        // new bulk loaded component is "newer" than any other merged component.
        opCtx.setNewComponent(lsmIndex.createBulkLoadTarget());
        this.componentBulkLoader =
                opCtx.getNewComponent().createBulkLoader(fillFactor, verifyInput, numElementsHint, false, true, true);
    }

    public ILSMDiskComponent getComponent() {
        return opCtx.getNewComponent();
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        componentBulkLoader.add(tuple);
    }

    public void delete(ITupleReference tuple) throws HyracksDataException {
        componentBulkLoader.delete(tuple);
    }

    @Override
    public void end() throws HyracksDataException {
        try {
            componentBulkLoader.end();
            if (opCtx.getNewComponent().getComponentSize() > 0) {
                //TODO(amoudi): Ensure Bulk load follow the same lifecycle Other Operations (Flush, Merge, etc).
                //then after operation should be called from harness as well
                //https://issues.apache.org/jira/browse/ASTERIXDB-1764
                lsmIndex.getIOOperationCallback().afterOperation(opCtx);
                lsmIndex.getHarness().addBulkLoadedComponent(opCtx.getNewComponent());
            }
        } finally {
            lsmIndex.getIOOperationCallback().afterFinalize(opCtx);
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        try {
            componentBulkLoader.abort();
            opCtx.setNewComponent(null);
            lsmIndex.getIOOperationCallback().afterOperation(opCtx);
        } finally {
            lsmIndex.getIOOperationCallback().afterFinalize(opCtx);
        }
    }

}