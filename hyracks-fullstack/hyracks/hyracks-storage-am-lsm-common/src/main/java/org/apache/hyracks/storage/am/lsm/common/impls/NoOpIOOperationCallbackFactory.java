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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.common.IResource;

public enum NoOpIOOperationCallbackFactory implements ILSMIOOperationCallbackFactory {
    INSTANCE;

    @Override
    public ILSMIOOperationCallback createIoOpCallback(ILSMIndex index) {
        return NoOpIOOperationCallback.INSTANCE;
    }

    @Override
    public void initialize(INCServiceContext ncCtx, IResource resource) {
        // No op
    }

    public static class NoOpIOOperationCallback implements ILSMIOOperationCallback {
        private static final NoOpIOOperationCallback INSTANCE = new NoOpIOOperationCallback();

        private NoOpIOOperationCallback() {
        }

        @Override
        public void beforeOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
            // Do nothing.
        }

        @Override
        public void afterOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
            // Do nothing.
        }

        @Override
        public void afterFinalize(ILSMIndexOperationContext opCtx) throws HyracksDataException {
            // Do nothing.
        }

        @Override
        public void recycled(ILSMMemoryComponent component, boolean componentSwitched) {
            // Do nothing.
        }

        @Override
        public void allocated(ILSMMemoryComponent component) {
            // Do nothing.
        }
    }
}
