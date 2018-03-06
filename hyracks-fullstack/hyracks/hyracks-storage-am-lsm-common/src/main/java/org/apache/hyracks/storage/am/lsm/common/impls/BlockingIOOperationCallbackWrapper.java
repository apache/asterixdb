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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;

public class BlockingIOOperationCallbackWrapper implements ILSMIOOperationCallback {

    private boolean notified = false;

    private final ILSMIOOperationCallback wrappedCallback;

    public BlockingIOOperationCallbackWrapper(ILSMIOOperationCallback callback) {
        this.wrappedCallback = callback;
    }

    public synchronized void waitForIO() throws InterruptedException {
        if (!notified) {
            wait();
        }
        notified = false;
    }

    @Override
    public void beforeOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
        wrappedCallback.beforeOperation(opCtx);
    }

    @Override
    public void afterOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
        wrappedCallback.afterOperation(opCtx);
    }

    @Override
    public synchronized void afterFinalize(ILSMIndexOperationContext opCtx) throws HyracksDataException {
        wrappedCallback.afterFinalize(opCtx);
        notifyAll();
        notified = true;
    }

    @Override
    public void recycled(ILSMMemoryComponent component, boolean componentSwitched) throws HyracksDataException {
        wrappedCallback.recycled(component, componentSwitched);
    }

    @Override
    public void allocated(ILSMMemoryComponent component) throws HyracksDataException {
        wrappedCallback.allocated(component);
    }
}
