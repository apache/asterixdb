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

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;

/**
 * This class is for testing. It's basically a way to get the new/old component info from the
 * harness callback simply.
 */

public class StubIOOperationCallback implements ILSMIOOperationCallback {

    private ILSMIndexOperationContext opCtx = null;

    @Override
    public void beforeOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
        // Not interested in this
    }

    @Override
    public void afterOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
        this.opCtx = opCtx;
    }

    @Override
    public void afterFinalize(ILSMIndexOperationContext opCtx) throws HyracksDataException {
        // Redundant info from after
    }

    public List<ILSMDiskComponent> getLastOldComponents() {
        return opCtx.getComponentsToBeMerged();
    }

    public ILSMDiskComponent getLastNewComponent() {
        return opCtx.getNewComponent();
    }

    @Override
    public void recycled(ILSMMemoryComponent component, boolean componentSwitched) {
        // Not interested in this
    }

    @Override
    public void allocated(ILSMMemoryComponent component) {
        // Not interested in this
    }
}
