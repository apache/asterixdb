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

package org.apache.hyracks.storage.am.lsm.invertedindex.common;

import java.io.File;
import java.io.FilenameFilter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.MultitenantVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.impls.ThreadCountingTracker;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;

public class LSMInvertedIndexTestHarness {

    private static final long RANDOM_SEED = 50;

    protected final int diskPageSize;
    protected final int diskNumPages;
    protected final int diskMaxOpenFiles;
    protected final int memPageSize;
    protected final int memNumPages;
    protected final int hyracksFrameSize;
    protected final double bloomFilterFalsePositiveRate;
    protected final int numMutableComponents;

    protected IOManager ioManager;
    protected int ioDeviceId;
    protected IBufferCache diskBufferCache;
    protected List<IVirtualBufferCache> virtualBufferCaches;
    protected IHyracksTaskContext ctx;
    protected ILSMIOOperationScheduler ioScheduler;
    protected ILSMMergePolicy mergePolicy;
    protected ILSMOperationTracker opTracker;
    protected ILSMIOOperationCallbackFactory ioOpCallbackFactory;
    protected ILSMPageWriteCallbackFactory pageWriteCallbackFactory;

    protected final Random rnd = new Random();
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String sep = System.getProperty("file.separator");
    protected String onDiskDir;
    protected String btreeFileName = "btree_vocab";
    protected String invIndexFileName = "inv_index";
    protected FileReference invIndexFileRef;
    protected IMetadataPageManagerFactory metadataPageManagerFactory =
            AppendOnlyLinkedMetadataPageManagerFactory.INSTANCE;

    public LSMInvertedIndexTestHarness() {
        this.diskPageSize = AccessMethodTestsConfig.LSM_INVINDEX_DISK_PAGE_SIZE;
        this.diskNumPages = AccessMethodTestsConfig.LSM_INVINDEX_DISK_NUM_PAGES;
        this.diskMaxOpenFiles = AccessMethodTestsConfig.LSM_INVINDEX_DISK_MAX_OPEN_FILES;
        this.memPageSize = AccessMethodTestsConfig.LSM_INVINDEX_MEM_PAGE_SIZE;
        this.memNumPages = AccessMethodTestsConfig.LSM_INVINDEX_MEM_NUM_PAGES;
        this.hyracksFrameSize = AccessMethodTestsConfig.LSM_INVINDEX_HYRACKS_FRAME_SIZE;
        this.bloomFilterFalsePositiveRate = AccessMethodTestsConfig.LSM_INVINDEX_BLOOMFILTER_FALSE_POSITIVE_RATE;
        this.ioScheduler = SynchronousSchedulerProvider.INSTANCE.getIoScheduler(null);
        this.mergePolicy = new NoMergePolicy();
        this.opTracker = new ThreadCountingTracker();
        this.ioOpCallbackFactory = NoOpIOOperationCallbackFactory.INSTANCE;
        this.pageWriteCallbackFactory = NoOpPageWriteCallbackFactory.INSTANCE;
        this.numMutableComponents = AccessMethodTestsConfig.LSM_INVINDEX_NUM_MUTABLE_COMPONENTS;
    }

    public void setUp() throws HyracksDataException {
        ioManager = TestStorageManagerComponentHolder.getIOManager();
        ioDeviceId = 0;
        onDiskDir = ioManager.getIODevices().get(ioDeviceId).getMount() + sep + "lsm_invertedindex_"
                + simpleDateFormat.format(new Date()) + sep;
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(diskPageSize, diskNumPages, diskMaxOpenFiles);
        diskBufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        virtualBufferCaches = new ArrayList<>();
        for (int i = 0; i < numMutableComponents; i++) {
            IVirtualBufferCache virtualBufferCache = new MultitenantVirtualBufferCache(
                    new VirtualBufferCache(new HeapBufferAllocator(), memPageSize, memNumPages / numMutableComponents));
            virtualBufferCaches.add(virtualBufferCache);
            virtualBufferCache.open();
        }
        rnd.setSeed(RANDOM_SEED);
        invIndexFileRef = ioManager.resolveAbsolutePath(onDiskDir + invIndexFileName);
    }

    public void tearDown() throws HyracksDataException {
        diskBufferCache.close();
        IODeviceHandle dev = ioManager.getIODevices().get(ioDeviceId);
        File dir = new File(dev.getMount(), onDiskDir);
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return !name.startsWith(".");
            }
        };
        String[] files = dir.list(filter);
        if (files != null) {
            for (String fileName : files) {
                File file = new File(dir.getPath() + File.separator + fileName);
                file.delete();
            }
        }
        dir.delete();
        for (int i = 0; i < numMutableComponents; i++) {
            virtualBufferCaches.get(i).close();
        }
    }

    public FileReference getInvListsFileRef() {
        return invIndexFileRef;
    }

    public int getDiskPageSize() {
        return diskPageSize;
    }

    public int getDiskNumPages() {
        return diskNumPages;
    }

    public int getDiskMaxOpenFiles() {
        return diskMaxOpenFiles;
    }

    public int getMemPageSize() {
        return memPageSize;
    }

    public int getMemNumPages() {
        return memNumPages;
    }

    public int getHyracksFrameSize() {
        return hyracksFrameSize;
    }

    public IOManager getIOManager() {
        return ioManager;
    }

    public int getIODeviceId() {
        return ioDeviceId;
    }

    public IBufferCache getDiskBufferCache() {
        return diskBufferCache;
    }

    public List<IVirtualBufferCache> getVirtualBufferCaches() {
        return virtualBufferCaches;
    }

    public double getBoomFilterFalsePositiveRate() {
        return bloomFilterFalsePositiveRate;
    }

    public IHyracksTaskContext getHyracksTastContext() {
        return ctx;
    }

    public String getOnDiskDir() {
        return onDiskDir;
    }

    public Random getRandom() {
        return rnd;
    }

    public ILSMIOOperationScheduler getIOScheduler() {
        return ioScheduler;
    }

    public ILSMOperationTracker getOperationTracker() {
        return opTracker;
    }

    public ILSMMergePolicy getMergePolicy() {
        return mergePolicy;
    }

    public ILSMIOOperationCallbackFactory getIOOperationCallbackFactory() {
        return ioOpCallbackFactory;
    }

    public ILSMPageWriteCallbackFactory getPageWriteCallbackFactory() {
        return pageWriteCallbackFactory;
    }

    public IMetadataPageManagerFactory getMetadataPageManagerFactory() {
        return metadataPageManagerFactory;
    }
}
