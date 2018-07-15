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
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.common.IResource;

import com.fasterxml.jackson.databind.JsonNode;

public class NoOpIOOperationCallbackFactory implements ILSMIOOperationCallbackFactory {

    private static final long serialVersionUID = 1L;
    public static final NoOpIOOperationCallbackFactory INSTANCE = new NoOpIOOperationCallbackFactory();

    private NoOpIOOperationCallbackFactory() {
    }

    @Override
    public ILSMIOOperationCallback createIoOpCallback(ILSMIndex index) {
        return NoOpIOOperationCallback.INSTANCE;
    }

    @Override
    public void initialize(INCServiceContext ncCtx, IResource resource) {
        // No op
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        return registry.getClassIdentifier(getClass(), serialVersionUID);
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return INSTANCE;
    }

    public static class NoOpIOOperationCallback implements ILSMIOOperationCallback {
        private static final NoOpIOOperationCallback INSTANCE = new NoOpIOOperationCallback();

        private NoOpIOOperationCallback() {
        }

        @Override
        public void recycled(ILSMMemoryComponent component) {
            // Do nothing.
        }

        @Override
        public void allocated(ILSMMemoryComponent component) {
            // Do nothing.
        }

        @Override
        public void scheduled(ILSMIOOperation operation) throws HyracksDataException {
            // Do nothing.
        }

        @Override
        public void beforeOperation(ILSMIOOperation operation) throws HyracksDataException {
            // Do nothing.
        }

        @Override
        public void afterOperation(ILSMIOOperation operation) throws HyracksDataException {
            // Do nothing.
        }

        @Override
        public void afterFinalize(ILSMIOOperation operation) throws HyracksDataException {
            // Do nothing.
        }

        @Override
        public void completed(ILSMIOOperation operation) {
            // Do nothing.
        }
    }

    @Override
    public int getCurrentMemoryComponentIndex() throws HyracksDataException {
        return 0;
    }
}
