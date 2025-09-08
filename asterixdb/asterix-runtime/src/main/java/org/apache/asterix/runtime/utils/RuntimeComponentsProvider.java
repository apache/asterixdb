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
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IColumnBufferPool;
import org.apache.hyracks.storage.common.disk.IDiskCacheMonitoringService;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class RuntimeComponentsProvider implements IStorageManager, ILSMIOOperationSchedulerProvider {

    private static final long serialVersionUID = 1L;

    public static final RuntimeComponentsProvider RUNTIME_PROVIDER = new RuntimeComponentsProvider();

    private RuntimeComponentsProvider() {
    }

    @Override
    public ILSMIOOperationScheduler getIoScheduler(INCServiceContext ctx) {
        return getAppCtx(ctx).getLSMIOScheduler();
    }

    @Override
    public IIOManager getIoManager(INCServiceContext ctx) {
        return getAppCtx(ctx).getPersistenceIoManager();
    }

    @Override
    public IBufferCache getBufferCache(INCServiceContext ctx) {
        return getAppCtx(ctx).getBufferCache();
    }

    @Override
    public IColumnBufferPool getColumnBufferPool(INCServiceContext ctx) {
        return getAppCtx(ctx).getColumnBufferPool();
    }

    @Override
    public ILocalResourceRepository getLocalResourceRepository(INCServiceContext ctx) {
        return getAppCtx(ctx).getLocalResourceRepository();
    }

    @Override
    public IDatasetLifecycleManager getLifecycleManager(INCServiceContext ctx) {
        return getAppCtx(ctx).getDatasetLifecycleManager();
    }

    @Override
    public IResourceIdFactory getResourceIdFactory(INCServiceContext ctx) {
        return getAppCtx(ctx).getResourceIdFactory();
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        return registry.getClassIdentifier(getClass(), serialVersionUID);
    }

    @Override
    public IDiskCacheMonitoringService getDiskCacheMonitoringService(INCServiceContext ctx) {
        return getAppCtx(ctx).getDiskCacheService();

    }

    private INcApplicationContext getAppCtx(INCServiceContext ctx) {
        return ((INcApplicationContext) ctx.getApplicationContext());
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return RUNTIME_PROVIDER;
    }
}
