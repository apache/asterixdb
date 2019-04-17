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
package org.apache.hyracks.test.support;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.control.nc.io.DefaultDeviceResolver;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexLifecycleManager;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManagerFactory;
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

public class TestStorageManagerComponentHolder {
    private static IBufferCache bufferCache;
    private static IFileMapProvider fileMapProvider;
    private static IOManager ioManager;
    private static ILocalResourceRepository localResourceRepository;
    private static IResourceLifecycleManager<IIndex> lcManager;
    private static ResourceIdFactory resourceIdFactory;
    private static IMetadataPageManagerFactory metadataPageManagerFactory =
            AppendOnlyLinkedMetadataPageManagerFactory.INSTANCE;
    private static int pageSize;
    private static int numPages;
    private static int maxOpenFiles;
    private final static ThreadFactory threadFactory = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r);
        }
    };

    public static void init(int pageSize, int numPages, int maxOpenFiles) {
        TestStorageManagerComponentHolder.pageSize = pageSize;
        TestStorageManagerComponentHolder.numPages = numPages;
        TestStorageManagerComponentHolder.maxOpenFiles = maxOpenFiles;
        bufferCache = null;
        fileMapProvider = null;
        localResourceRepository = null;
        lcManager = null;
    }

    public synchronized static IResourceLifecycleManager<IIndex> getIndexLifecycleManager() {
        if (lcManager == null) {
            lcManager = new IndexLifecycleManager();
        }
        return lcManager;
    }

    public synchronized static IBufferCache getBufferCache(INCServiceContext ctx) {
        if (bufferCache == null) {
            IIOManager ioManager = ctx.getIoManager();
            return getBufferCache(ioManager);
        }
        return bufferCache;
    }

    private synchronized static IFileMapProvider getFileMapProvider() {
        if (fileMapProvider == null) {
            fileMapProvider = new FileMapManager();
        }
        return fileMapProvider;
    }

    public synchronized static IOManager getIOManager() throws HyracksDataException {
        if (ioManager == null) {
            List<IODeviceHandle> devices = new ArrayList<>();
            devices.add(new IODeviceHandle(new File(System.getProperty("user.dir") + File.separator + "target"),
                    "iodev_test_wa"));
            ioManager = new IOManager(devices, new DefaultDeviceResolver(), 2, 10);
        }
        return ioManager;
    }

    public synchronized static ILocalResourceRepository getLocalResourceRepository() {
        if (localResourceRepository == null) {
            try {
                ILocalResourceRepositoryFactory localResourceRepositoryFactory =
                        new TransientLocalResourceRepositoryFactory();
                localResourceRepository = localResourceRepositoryFactory.createRepository();
            } catch (HyracksException e) {
                //In order not to change the IStorageManagerInterface due to the test code, throw runtime exception.
                throw new IllegalArgumentException();
            }
        }
        return localResourceRepository;
    }

    public static IMetadataPageManagerFactory getMetadataPageManagerFactory() {
        return metadataPageManagerFactory;
    }

    public synchronized static ResourceIdFactory getResourceIdFactory() {
        if (resourceIdFactory == null) {
            try {
                ResourceIdFactoryProvider resourceIdFactoryFactory =
                        new ResourceIdFactoryProvider(getLocalResourceRepository());
                resourceIdFactory = resourceIdFactoryFactory.createResourceIdFactory();
            } catch (HyracksException e) {
                //In order not to change the IStorageManagerInterface due to the test code, throw runtime exception.
                throw new IllegalArgumentException();
            }
        }
        return resourceIdFactory;
    }

    public static IBufferCache getBufferCache(IIOManager ioManager) {
        if (bufferCache != null) {
            return bufferCache;
        }
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy(allocator, pageSize, numPages);
        IFileMapProvider fileMapProvider = getFileMapProvider();
        bufferCache = new BufferCache(ioManager, prs, new DelayPageCleanerPolicy(1000),
                (IFileMapManager) fileMapProvider, maxOpenFiles, 10, threadFactory);
        return bufferCache;
    }
}
