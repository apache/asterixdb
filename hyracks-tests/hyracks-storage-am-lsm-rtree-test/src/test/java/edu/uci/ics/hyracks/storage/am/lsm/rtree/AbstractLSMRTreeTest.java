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

package edu.uci.ics.hyracks.storage.am.lsm.rtree;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTreeInMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTreeInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class AbstractLSMRTreeTest {
    protected static final Logger LOGGER = Logger.getLogger(AbstractLSMRTreeTest.class.getName());

    private static final long RANDOM_SEED = 50;
    private static final int DEFAULT_DISK_PAGE_SIZE = 256;
    private static final int DEFAULT_DISK_NUM_PAGES = 100;
    private static final int DEFAULT_DISK_MAX_OPEN_FILES = 100;
    private static final int DEFAULT_MEM_PAGE_SIZE = 256;
    private static final int DEFAULT_MEM_NUM_PAGES = 100;
    private static final int DEFAULT_HYRACKS_FRAME_SIZE = 128;
    private static final int DUMMY_FILE_ID = -1;

    protected final int diskPageSize;
    protected final int diskNumPages;
    protected final int diskMaxOpenFiles;
    protected final int memPageSize;
    protected final int memNumPages;
    protected final int hyracksFrameSize;

    protected IBufferCache diskBufferCache;
    protected IFileMapProvider diskFileMapProvider;
    protected InMemoryBufferCache memBufferCache;
    protected InMemoryFreePageManager memFreePageManager;
    protected IHyracksTaskContext ctx;

    protected final Random rnd = new Random();
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String tmpDir = System.getProperty("java.io.tmpdir");
    protected final static String sep = System.getProperty("file.separator");
    protected String onDiskDir;

    public AbstractLSMRTreeTest() {
        this.diskPageSize = DEFAULT_DISK_PAGE_SIZE;
        this.diskNumPages = DEFAULT_DISK_NUM_PAGES;
        this.diskMaxOpenFiles = DEFAULT_DISK_MAX_OPEN_FILES;
        this.memPageSize = DEFAULT_MEM_PAGE_SIZE;
        this.memNumPages = DEFAULT_MEM_NUM_PAGES;
        this.hyracksFrameSize = DEFAULT_HYRACKS_FRAME_SIZE;
    }

    public AbstractLSMRTreeTest(int diskPageSize, int diskNumPages, int diskMaxOpenFiles, int memPageSize,
            int memNumPages, int hyracksFrameSize) {
        this.diskPageSize = diskPageSize;
        this.diskNumPages = diskNumPages;
        this.diskMaxOpenFiles = diskMaxOpenFiles;
        this.memPageSize = memPageSize;
        this.memNumPages = memNumPages;
        this.hyracksFrameSize = hyracksFrameSize;
    }

    @Before
    public void setUp() throws HyracksDataException {
        onDiskDir = tmpDir + sep + "lsm_rtree_" + simpleDateFormat.format(new Date()) + sep;
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(diskPageSize, diskNumPages, diskMaxOpenFiles);
        diskBufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        diskFileMapProvider = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        memBufferCache = new LSMRTreeInMemoryBufferCache(new HeapBufferAllocator(), getMemPageSize(), getMemNumPages());
        memFreePageManager = new LSMRTreeInMemoryFreePageManager(memNumPages, new LIFOMetaDataFrameFactory());
        rnd.setSeed(RANDOM_SEED);
    }

    @After
    public void tearDown() throws HyracksDataException {
        diskBufferCache.close();
        File f = new File(onDiskDir);
        // TODO: For some reason the dir fails to be deleted. Ask Vinayak about
        // this.
        f.deleteOnExit();
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

    public int getFileId() {
        return DUMMY_FILE_ID;
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

    public String getOnDiskDir() {
        return onDiskDir;
    }

    public Random getRandom() {
        return rnd;
    }
}
