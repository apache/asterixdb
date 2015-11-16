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
package org.apache.asterix.transaction.management.service.transaction;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.common.IStorageManagerInterface;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.ResourceIdFactory;

public class AsterixRuntimeComponentsProvider implements IIndexLifecycleManagerProvider, IStorageManagerInterface,
        ILSMIOOperationSchedulerProvider {

    private static final long serialVersionUID = 1L;

    public static final AsterixRuntimeComponentsProvider RUNTIME_PROVIDER = new AsterixRuntimeComponentsProvider();

    private AsterixRuntimeComponentsProvider() {
    }

    @Override
    public ILSMIOOperationScheduler getIOScheduler(IHyracksTaskContext ctx) {
        return ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getLSMIOScheduler();
    }

    @Override
    public IBufferCache getBufferCache(IHyracksTaskContext ctx) {
        return ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getBufferCache();
    }

    @Override
    public IFileMapProvider getFileMapProvider(IHyracksTaskContext ctx) {
        return ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getFileMapManager();
    }

    @Override
    public ILocalResourceRepository getLocalResourceRepository(IHyracksTaskContext ctx) {
        return ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getLocalResourceRepository();
    }

    @Override
    public IDatasetLifecycleManager getLifecycleManager(IHyracksTaskContext ctx) {
        return ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getDatasetLifecycleManager();
    }

    @Override
    public ResourceIdFactory getResourceIdFactory(IHyracksTaskContext ctx) {
        return ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getResourceIdFactory();
    }

}
