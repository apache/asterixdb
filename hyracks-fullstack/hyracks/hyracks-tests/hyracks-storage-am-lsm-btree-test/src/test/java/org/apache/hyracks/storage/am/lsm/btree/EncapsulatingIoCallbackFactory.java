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

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.impl.ITestOpCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.IResource;

public class EncapsulatingIoCallbackFactory implements ILSMIOOperationCallbackFactory {

    private static final long serialVersionUID = 1L;
    private final ILSMIOOperationCallbackFactory encapsulated;
    private final ITestOpCallback<ILSMIOOperation> scheduledCallback;
    private final ITestOpCallback<ILSMIOOperation> beforeOperationCallback;
    private final ITestOpCallback<ILSMIOOperation> afterOperationCallback;
    private final ITestOpCallback<ILSMIOOperation> afterFinalizeCallback;
    private final ITestOpCallback<ILSMIOOperation> completedCallback;

    public EncapsulatingIoCallbackFactory(ILSMIOOperationCallbackFactory factory,
            ITestOpCallback<ILSMIOOperation> scheduledCallback,
            ITestOpCallback<ILSMIOOperation> beforeOperationCallback,
            ITestOpCallback<ILSMIOOperation> afterOperationCallback,
            ITestOpCallback<ILSMIOOperation> afterFinalizeCallback,
            ITestOpCallback<ILSMIOOperation> completedCallback) {
        encapsulated = factory;
        this.scheduledCallback = scheduledCallback;
        this.beforeOperationCallback = beforeOperationCallback;
        this.afterOperationCallback = afterOperationCallback;
        this.afterFinalizeCallback = afterFinalizeCallback;
        this.completedCallback = completedCallback;
    }

    @Override
    public void initialize(INCServiceContext ncCtx, IResource resource) {
        encapsulated.initialize(ncCtx, resource);
    }

    @Override
    public ILSMIOOperationCallback createIoOpCallback(ILSMIndex index) throws HyracksDataException {
        ILSMIOOperationCallback inner = encapsulated.createIoOpCallback(index);
        return new EncapsulatingIoCallback(inner, scheduledCallback, beforeOperationCallback, afterOperationCallback,
                afterFinalizeCallback, completedCallback);
    }

    @Override
    public int getCurrentMemoryComponentIndex() throws HyracksDataException {
        return encapsulated.getCurrentMemoryComponentIndex();
    }

}
