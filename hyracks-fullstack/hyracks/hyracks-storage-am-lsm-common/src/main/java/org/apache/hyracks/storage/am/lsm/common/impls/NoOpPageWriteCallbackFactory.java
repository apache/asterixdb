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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;

import com.fasterxml.jackson.databind.JsonNode;

public class NoOpPageWriteCallbackFactory implements ILSMPageWriteCallbackFactory {

    private static final long serialVersionUID = 1L;
    public static final NoOpPageWriteCallbackFactory INSTANCE = new NoOpPageWriteCallbackFactory();

    private NoOpPageWriteCallbackFactory() {
    }

    @Override
    public IPageWriteCallback createPageWriteCallback() throws HyracksDataException {
        return NoOpPageWriteCallback.INSTANCE;
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

}
