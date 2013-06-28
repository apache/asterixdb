/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.test.support;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexLifecycleManager;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.DelayPageCleanerPolicy;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepositoryFactory;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactoryProvider;
import edu.uci.ics.hyracks.storage.common.file.TransientFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.TransientLocalResourceRepositoryFactory;

public class TestStorageManagerComponentHolder {
    private static IBufferCache bufferCache;
    private static IFileMapProvider fileMapProvider;
    private static IOManager ioManager;
    private static ILocalResourceRepository localResourceRepository;
    private static IIndexLifecycleManager lcManager;
    private static ResourceIdFactory resourceIdFactory;

    private static int pageSize;
    private static int numPages;
    private static int maxOpenFiles;
    private final static ThreadFactory threadFactory = new ThreadFactory() {
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

    public synchronized static IIndexLifecycleManager getIndexLifecycleManager(IHyracksTaskContext ctx) {
        if (lcManager == null) {
            lcManager = new IndexLifecycleManager();
        }
        return lcManager;
    }

    public synchronized static IBufferCache getBufferCache(IHyracksTaskContext ctx) {
        if (bufferCache == null) {
            ICacheMemoryAllocator allocator = new HeapBufferAllocator();
            IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
            IFileMapProvider fileMapProvider = getFileMapProvider(ctx);
            bufferCache = new BufferCache(ctx.getIOManager(), allocator, prs, new DelayPageCleanerPolicy(1000),
                    (IFileMapManager) fileMapProvider, pageSize, numPages, maxOpenFiles, threadFactory);
        }
        return bufferCache;
    }

    public synchronized static IFileMapProvider getFileMapProvider(IHyracksTaskContext ctx) {
        if (fileMapProvider == null) {
            fileMapProvider = new TransientFileMapManager();
        }
        return fileMapProvider;
    }

    public synchronized static IOManager getIOManager() throws HyracksException {
        if (ioManager == null) {
            List<IODeviceHandle> devices = new ArrayList<IODeviceHandle>();
            devices.add(new IODeviceHandle(new File(System.getProperty("java.io.tmpdir")), "iodev_test_wa"));
            ioManager = new IOManager(devices, Executors.newCachedThreadPool());
        }
        return ioManager;
    }

    public synchronized static ILocalResourceRepository getLocalResourceRepository(IHyracksTaskContext ctx) {
        if (localResourceRepository == null) {
            try {
                ILocalResourceRepositoryFactory localResourceRepositoryFactory = new TransientLocalResourceRepositoryFactory();
                localResourceRepository = localResourceRepositoryFactory.createRepository();
            } catch (HyracksException e) {
                //In order not to change the IStorageManagerInterface due to the test code, throw runtime exception.
                throw new IllegalArgumentException();
            }
        }
        return localResourceRepository;
    }

    public synchronized static ResourceIdFactory getResourceIdFactory(IHyracksTaskContext ctx) {
        if (resourceIdFactory == null) {
            try {
                ResourceIdFactoryProvider resourceIdFactoryFactory = new ResourceIdFactoryProvider(
                        getLocalResourceRepository(ctx));
                resourceIdFactory = resourceIdFactoryFactory.createResourceIdFactory();
            } catch (HyracksException e) {
                //In order not to change the IStorageManagerInterface due to the test code, throw runtime exception.
                throw new IllegalArgumentException();
            }
        }
        return resourceIdFactory;
    }
}