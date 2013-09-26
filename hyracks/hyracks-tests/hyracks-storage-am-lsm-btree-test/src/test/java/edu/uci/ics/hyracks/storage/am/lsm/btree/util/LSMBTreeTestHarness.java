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

package edu.uci.ics.hyracks.storage.am.lsm.btree.util;

import java.io.File;
import java.io.FilenameFilter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.SynchronousScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ThreadCountingTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class LSMBTreeTestHarness {
    protected static final Logger LOGGER = Logger.getLogger(LSMBTreeTestHarness.class.getName());

    public static final BTreeLeafFrameType[] LEAF_FRAMES_TO_TEST = new BTreeLeafFrameType[] { BTreeLeafFrameType.REGULAR_NSM };

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
    protected IFileMapProvider diskFileMapProvider;
    protected List<IVirtualBufferCache> virtualBufferCaches;
    protected IHyracksTaskContext ctx;
    protected ILSMIOOperationScheduler ioScheduler;
    protected ILSMMergePolicy mergePolicy;
    protected ILSMOperationTracker opTracker;
    protected ILSMIOOperationCallback ioOpCallback;

    protected final Random rnd = new Random();
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String sep = System.getProperty("file.separator");
    protected String onDiskDir;
    protected FileReference file;

    public LSMBTreeTestHarness() {
        this.diskPageSize = AccessMethodTestsConfig.LSM_BTREE_DISK_PAGE_SIZE;
        this.diskNumPages = AccessMethodTestsConfig.LSM_BTREE_DISK_NUM_PAGES;
        this.diskMaxOpenFiles = AccessMethodTestsConfig.LSM_BTREE_DISK_MAX_OPEN_FILES;
        this.memPageSize = AccessMethodTestsConfig.LSM_BTREE_MEM_PAGE_SIZE;
        this.memNumPages = AccessMethodTestsConfig.LSM_BTREE_MEM_NUM_PAGES;
        this.hyracksFrameSize = AccessMethodTestsConfig.LSM_BTREE_HYRACKS_FRAME_SIZE;
        this.bloomFilterFalsePositiveRate = AccessMethodTestsConfig.LSM_BTREE_BLOOMFILTER_FALSE_POSITIVE_RATE;
        this.ioScheduler = SynchronousScheduler.INSTANCE;
        this.mergePolicy = NoMergePolicy.INSTANCE;
        this.opTracker = new ThreadCountingTracker();
        this.ioOpCallback = NoOpIOOperationCallback.INSTANCE;
        this.numMutableComponents = AccessMethodTestsConfig.LSM_BTREE_NUM_MUTABLE_COMPONENTS;
    }

    public void setUp() throws HyracksException {
        ioManager = TestStorageManagerComponentHolder.getIOManager();
        ioDeviceId = 0;
        onDiskDir = ioManager.getIODevices().get(ioDeviceId).getPath() + sep + "lsm_btree_"
                + simpleDateFormat.format(new Date()) + sep;
        file = new FileReference(new File(onDiskDir));
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(diskPageSize, diskNumPages, diskMaxOpenFiles);
        diskBufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        diskFileMapProvider = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        virtualBufferCaches = new ArrayList<IVirtualBufferCache>();
        for (int i = 0; i < numMutableComponents; i++) {
            IVirtualBufferCache virtualBufferCache = new VirtualBufferCache(new HeapBufferAllocator(), memPageSize,
                    memNumPages / numMutableComponents);
            virtualBufferCaches.add(virtualBufferCache);
        }
        rnd.setSeed(RANDOM_SEED);
    }

    public void tearDown() throws HyracksDataException {
        diskBufferCache.close();
        IODeviceHandle dev = ioManager.getIODevices().get(ioDeviceId);
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

    public IFileMapProvider getDiskFileMapProvider() {
        return diskFileMapProvider;
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

    public FileReference getFileReference() {
        return file;
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

    public ILSMIOOperationCallback getIOOperationCallback() {
        return ioOpCallback;
    }
}
