/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.common;

import java.io.File;
import java.io.FilenameFilter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.SynchronousScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ThreadCountingOperationTrackerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.TransientFileMapManager;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class LSMInvertedIndexTestHarness {

    private static final long RANDOM_SEED = 50;

    protected final int diskPageSize;
    protected final int diskNumPages;
    protected final int diskMaxOpenFiles;
    protected final int memPageSize;
    protected final int memNumPages;
    protected final int hyracksFrameSize;
    protected final double bloomFilterFalsePositiveRate;

    protected IOManager ioManager;
    protected IBufferCache diskBufferCache;
    protected IFileMapProvider diskFileMapProvider;
    protected IVirtualBufferCache virtualBufferCache;
    protected IInMemoryFreePageManager memFreePageManager;
    protected IHyracksTaskContext ctx;
    protected ILSMIOOperationScheduler ioScheduler;
    protected ILSMMergePolicy mergePolicy;
    protected ILSMOperationTrackerFactory opTrackerFactory;
    protected ILSMIOOperationCallbackProvider ioOpCallbackProvider;

    protected final Random rnd = new Random();
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String sep = System.getProperty("file.separator");
    protected String onDiskDir;
    protected String btreeFileName = "btree_vocab";
    protected String invIndexFileName = "inv_index";
    protected FileReference invIndexFileRef;

    public LSMInvertedIndexTestHarness() {
        this.diskPageSize = AccessMethodTestsConfig.LSM_INVINDEX_DISK_PAGE_SIZE;
        this.diskNumPages = AccessMethodTestsConfig.LSM_INVINDEX_DISK_NUM_PAGES;
        this.diskMaxOpenFiles = AccessMethodTestsConfig.LSM_INVINDEX_DISK_MAX_OPEN_FILES;
        this.memPageSize = AccessMethodTestsConfig.LSM_INVINDEX_MEM_PAGE_SIZE;
        this.memNumPages = AccessMethodTestsConfig.LSM_INVINDEX_MEM_NUM_PAGES;
        this.hyracksFrameSize = AccessMethodTestsConfig.LSM_INVINDEX_HYRACKS_FRAME_SIZE;
        this.bloomFilterFalsePositiveRate = AccessMethodTestsConfig.LSM_INVINDEX_BLOOMFILTER_FALSE_POSITIVE_RATE;
        this.ioScheduler = SynchronousScheduler.INSTANCE;
        this.mergePolicy = NoMergePolicy.INSTANCE;
        this.opTrackerFactory = ThreadCountingOperationTrackerFactory.INSTANCE;
        this.ioOpCallbackProvider = NoOpIOOperationCallback.INSTANCE;
    }

    public LSMInvertedIndexTestHarness(int diskPageSize, int diskNumPages, int diskMaxOpenFiles, int memPageSize,
            int memNumPages, int hyracksFrameSize, double bloomFilterFalsePositiveRate) {
        this.diskPageSize = diskPageSize;
        this.diskNumPages = diskNumPages;
        this.diskMaxOpenFiles = diskMaxOpenFiles;
        this.memPageSize = memPageSize;
        this.memNumPages = memNumPages;
        this.hyracksFrameSize = hyracksFrameSize;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.ioScheduler = SynchronousScheduler.INSTANCE;
        this.mergePolicy = NoMergePolicy.INSTANCE;
        this.opTrackerFactory = ThreadCountingOperationTrackerFactory.INSTANCE;
    }

    public void setUp() throws HyracksException {
        onDiskDir = "lsm_invertedindex_" + simpleDateFormat.format(new Date()) + sep;
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(diskPageSize, diskNumPages, diskMaxOpenFiles);
        diskBufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        diskFileMapProvider = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        virtualBufferCache = new VirtualBufferCache(new HeapBufferAllocator(), new TransientFileMapManager(),
                memPageSize, memNumPages);
        virtualBufferCache.open();
        memFreePageManager = new InMemoryFreePageManager(memNumPages, new LIFOMetaDataFrameFactory());
        ioManager = TestStorageManagerComponentHolder.getIOManager();
        rnd.setSeed(RANDOM_SEED);
        invIndexFileRef = ioManager.getIODevices().get(0).createFileReference(onDiskDir + invIndexFileName);
    }

    public void tearDown() throws HyracksDataException {
        diskBufferCache.close();
        for (IODeviceHandle dev : ioManager.getIODevices()) {
            File dir = new File(dev.getPath(), onDiskDir);
            FilenameFilter filter = new FilenameFilter() {
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
        }
        virtualBufferCache.close();
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

    public IBufferCache getDiskBufferCache() {
        return diskBufferCache;
    }

    public IFileMapProvider getDiskFileMapProvider() {
        return diskFileMapProvider;
    }

    public IVirtualBufferCache getVirtualBufferCache() {
        return virtualBufferCache;
    }

    public double getBoomFilterFalsePositiveRate() {
        return bloomFilterFalsePositiveRate;
    }

    public IInMemoryFreePageManager getMemFreePageManager() {
        return memFreePageManager;
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

    public ILSMOperationTrackerFactory getOperationTrackerFactory() {
        return opTrackerFactory;
    }

    public ILSMMergePolicy getMergePolicy() {
        return mergePolicy;
    }

    public ILSMIOOperationCallbackProvider getIOOperationCallbackProvider() {
        return ioOpCallbackProvider;
    }
}
