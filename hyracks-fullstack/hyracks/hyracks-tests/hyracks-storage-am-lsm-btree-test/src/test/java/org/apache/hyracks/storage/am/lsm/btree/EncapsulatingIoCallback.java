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

package org.apache.hyracks.storage.am.lsm.btree;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.impl.ITestOpCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;

public class EncapsulatingIoCallback implements ILSMIOOperationCallback {

    private final ILSMIOOperationCallback encapsulated;
    private final ITestOpCallback<ILSMIOOperation> scheduledCallback;
    private final ITestOpCallback<ILSMIOOperation> beforeOperationCallback;
    private final ITestOpCallback<ILSMIOOperation> afterOperationCallback;
    private final ITestOpCallback<ILSMIOOperation> afterFinalizeCallback;
    private final ITestOpCallback<ILSMIOOperation> completedCallback;

    public EncapsulatingIoCallback(ILSMIOOperationCallback inner, ITestOpCallback<ILSMIOOperation> scheduledCallback,
            ITestOpCallback<ILSMIOOperation> beforeOperationCallback,
            ITestOpCallback<ILSMIOOperation> afterOperationCallback,
            ITestOpCallback<ILSMIOOperation> afterFinalizeCallback,
            ITestOpCallback<ILSMIOOperation> completedCallback) {
        this.encapsulated = inner;
        this.scheduledCallback = scheduledCallback;
        this.beforeOperationCallback = beforeOperationCallback;
        this.afterOperationCallback = afterOperationCallback;
        this.afterFinalizeCallback = afterFinalizeCallback;
        this.completedCallback = completedCallback;
    }

    @Override
    public void scheduled(ILSMIOOperation operation) throws HyracksDataException {
        scheduledCallback.before(operation);
        encapsulated.scheduled(operation);
        scheduledCallback.after(operation);
    }

    @Override
    public void beforeOperation(ILSMIOOperation operation) throws HyracksDataException {
        beforeOperationCallback.before(operation);
        encapsulated.beforeOperation(operation);
        beforeOperationCallback.after(operation);
    }

    @Override
    public void afterOperation(ILSMIOOperation operation) throws HyracksDataException {
        afterOperationCallback.before(operation);
        encapsulated.afterOperation(operation);
        afterOperationCallback.after(operation);
    }

    @Override
    public void afterFinalize(ILSMIOOperation operation) throws HyracksDataException {
        afterFinalizeCallback.before(operation);
        encapsulated.afterFinalize(operation);
        afterFinalizeCallback.after(operation);
    }

    @Override
    public void completed(ILSMIOOperation operation) {
        try {
            completedCallback.before(operation);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
        encapsulated.completed(operation);
        try {
            completedCallback.after(operation);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void recycled(ILSMMemoryComponent component) throws HyracksDataException {
        encapsulated.recycled(component);
    }

    @Override
    public void allocated(ILSMMemoryComponent component) throws HyracksDataException {
        encapsulated.allocated(component);
    }

    public ILSMIOOperationCallback getEncapsulated() {
        return encapsulated;
    }

}
