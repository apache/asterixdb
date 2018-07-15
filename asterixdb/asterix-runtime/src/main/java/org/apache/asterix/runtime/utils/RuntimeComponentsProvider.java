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
package org.apache.asterix.runtime.utils;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class RuntimeComponentsProvider implements IStorageManager, ILSMIOOperationSchedulerProvider {

    private static final long serialVersionUID = 1L;

    public static final RuntimeComponentsProvider RUNTIME_PROVIDER = new RuntimeComponentsProvider();

    private RuntimeComponentsProvider() {
    }

    @Override
    public ILSMIOOperationScheduler getIoScheduler(INCServiceContext ctx) {
        return ((INcApplicationContext) ctx.getApplicationContext()).getLSMIOScheduler();
    }

    @Override
    public IBufferCache getBufferCache(INCServiceContext ctx) {
        return ((INcApplicationContext) ctx.getApplicationContext()).getBufferCache();
    }

    @Override
    public ILocalResourceRepository getLocalResourceRepository(INCServiceContext ctx) {
        return ((INcApplicationContext) ctx.getApplicationContext()).getLocalResourceRepository();
    }

    @Override
    public IDatasetLifecycleManager getLifecycleManager(INCServiceContext ctx) {
        return ((INcApplicationContext) ctx.getApplicationContext()).getDatasetLifecycleManager();
    }

    @Override
    public IResourceIdFactory getResourceIdFactory(INCServiceContext ctx) {
        return ((INcApplicationContext) ctx.getApplicationContext()).getResourceIdFactory();
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        return registry.getClassIdentifier(getClass(), serialVersionUID);
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return RUNTIME_PROVIDER;
    }
}
