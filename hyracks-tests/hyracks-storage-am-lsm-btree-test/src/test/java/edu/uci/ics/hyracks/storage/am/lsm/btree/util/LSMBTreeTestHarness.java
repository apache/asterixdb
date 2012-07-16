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

package edu.uci.ics.hyracks.storage.am.lsm.btree.util;

import java.io.File;
import java.io.FilenameFilter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.FlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ImmediateScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.RefCountingOperationTracker;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.TransientFileMapManager;
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

    protected IOManager ioManager;
    protected IBufferCache diskBufferCache;
    protected IFileMapProvider diskFileMapProvider;
    protected InMemoryBufferCache memBufferCache;
    protected InMemoryFreePageManager memFreePageManager;
    protected IHyracksTaskContext ctx;
    protected ILSMIOScheduler ioScheduler;
    protected ILSMFlushController flushController;
    protected ILSMMergePolicy mergePolicy;
    protected ILSMOperationTracker opTracker;

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
        this.ioScheduler = ImmediateScheduler.INSTANCE;
        this.mergePolicy = NoMergePolicy.INSTANCE;
        this.flushController = new FlushController();
        this.opTracker = new RefCountingOperationTracker();
    }

    public LSMBTreeTestHarness(int diskPageSize, int diskNumPages, int diskMaxOpenFiles, int memPageSize,
            int memNumPages, int hyracksFrameSize) {
        this.diskPageSize = diskPageSize;
        this.diskNumPages = diskNumPages;
        this.diskMaxOpenFiles = diskMaxOpenFiles;
        this.memPageSize = memPageSize;
        this.memNumPages = memNumPages;
        this.hyracksFrameSize = hyracksFrameSize;
        this.ioScheduler = ImmediateScheduler.INSTANCE;
        this.mergePolicy = NoMergePolicy.INSTANCE;
        this.flushController = new FlushController();
        this.opTracker = new RefCountingOperationTracker();
    }

    public void setUp() throws HyracksException {
        onDiskDir = "lsm_btree_" + simpleDateFormat.format(new Date()) + sep;
        file = new FileReference(new File(onDiskDir));
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(diskPageSize, diskNumPages, diskMaxOpenFiles);
        diskBufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        diskFileMapProvider = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        memBufferCache = new InMemoryBufferCache(new HeapBufferAllocator(), memPageSize, memNumPages,
                new TransientFileMapManager());
        memFreePageManager = new InMemoryFreePageManager(memNumPages, new LIFOMetaDataFrameFactory());
        ioManager = TestStorageManagerComponentHolder.getIOManager();
        rnd.setSeed(RANDOM_SEED);
    }

    public void tearDown() throws HyracksDataException {
        ioScheduler.shutdown();
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

    public InMemoryBufferCache getMemBufferCache() {
        return memBufferCache;
    }

    public InMemoryFreePageManager getMemFreePageManager() {
        return memFreePageManager;
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

    public ILSMIOScheduler getIOScheduler() {
        return ioScheduler;
    }

    public ILSMOperationTracker getOperationTracker() {
        return opTracker;
    }

    public ILSMFlushController getFlushController() {
        return flushController;
    }

    public ILSMMergePolicy getMergePolicy() {
        return mergePolicy;
    }
}
