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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;

public enum NoOpIOOperationCallback implements ILSMIOOperationCallback, ILSMIOOperationCallbackProvider, ILSMIOOperationCallbackFactory {
    INSTANCE;

    @Override
    public void beforeOperation(LSMOperationType opType) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void afterOperation(LSMOperationType opType, List<ILSMComponent> oldComponents, ILSMComponent newComponent)
            throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void afterFinalize(LSMOperationType opType, ILSMComponent newComponent) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public ILSMIOOperationCallback getIOOperationCallback(ILSMIndex index) {
        return INSTANCE;
    }

    @Override
    public ILSMIOOperationCallback createIOOperationCallback() {
        return INSTANCE;
    }

    @Override
    public void setNumOfMutableComponents(int count) {
        // Do nothing.
    }
}
