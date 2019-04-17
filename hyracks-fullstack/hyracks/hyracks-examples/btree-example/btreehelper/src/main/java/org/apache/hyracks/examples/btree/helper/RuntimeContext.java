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

package org.apache.hyracks.examples.btree.helper;

import java.util.concurrent.ThreadFactory;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.dataflow.IndexLifecycleManager;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import org.apache.hyracks.storage.common.buffercache.DelayPageCleanerPolicy;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import org.apache.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import org.apache.hyracks.storage.common.file.FileMapManager;
import org.apache.hyracks.storage.common.file.IFileMapManager;
import org.apache.hyracks.storage.common.file.IFileMapProvider;
import org.apache.hyracks.storage.common.file.ILocalResourceRepositoryFactory;
import org.apache.hyracks.storage.common.file.ResourceIdFactory;
import org.apache.hyracks.storage.common.file.ResourceIdFactoryProvider;
import org.apache.hyracks.storage.common.file.TransientLocalResourceRepositoryFactory;

public class RuntimeContext {
    private IBufferCache bufferCache;
    private IFileMapManager fileMapManager;
    private ILocalResourceRepository localResourceRepository;
    private IResourceLifecycleManager<IIndex> lcManager;
    private ResourceIdFactory resourceIdFactory;
    private ThreadFactory threadFactory = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r);
        }
    };

    public RuntimeContext(INCServiceContext appCtx) throws HyracksDataException {
        fileMapManager = new FileMapManager();
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy(allocator, 32768, 50);
        bufferCache = new BufferCache(appCtx.getIoManager(), prs, new DelayPageCleanerPolicy(1000), fileMapManager, 100,
                10, threadFactory);
        ILocalResourceRepositoryFactory localResourceRepositoryFactory = new TransientLocalResourceRepositoryFactory();
        localResourceRepository = localResourceRepositoryFactory.createRepository();
        resourceIdFactory = (new ResourceIdFactoryProvider(localResourceRepository)).createResourceIdFactory();
        lcManager = new IndexLifecycleManager();
    }

    public void close() throws HyracksDataException {
        bufferCache.close();
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public IFileMapProvider getFileMapManager() {
        return fileMapManager;
    }

    public static RuntimeContext get(INCServiceContext ctx) {
        return (RuntimeContext) ctx.getApplicationContext();
    }

    public ILocalResourceRepository getLocalResourceRepository() {
        return localResourceRepository;
    }

    public ResourceIdFactory getResourceIdFactory() {
        return resourceIdFactory;
    }

    public IResourceLifecycleManager<IIndex> getIndexLifecycleManager() {
        return lcManager;
    }
}
