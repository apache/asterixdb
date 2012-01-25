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

package edu.uci.ics.hyracks.storage.am.lsmtree.btree;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public abstract class AbstractLSMTreeTest {
    protected static final Logger LOGGER = Logger.getLogger(AbstractLSMTreeTest.class.getName());
    private static final long RANDOM_SEED = 50;
    private static final int DISK_PAGE_SIZE = 256;
    private static final int DISK_NUM_PAGES = 10;
    private static final int DISK_MAX_OPEN_FILES = 100;
    private static final int MEM_PAGE_SIZE = 256;
    private static final int MEM_NUM_PAGES = 10;
    private static final int HYRACKS_FRAME_SIZE = 128;
    protected static final int dummyFileId = -1;           
    
    protected IBufferCache diskBufferCache;
    protected IFileMapProvider diskFileMapProvider;
    protected InMemoryBufferCache memBufferCache;
    protected IHyracksTaskContext ctx; 
    
    protected final Random rnd = new Random();
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String tmpDir = System.getProperty("java.io.tmpdir");
    protected final static String sep = System.getProperty("file.separator");
    protected String onDiskDir;
    
    @Before
    public void setUp() throws HyracksDataException {
        onDiskDir = tmpDir + sep + "lsm_btree" + simpleDateFormat.format(new Date()) + sep;
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(getDiskPageSize(), getDiskNumPages(), getDiskMaxOpenFiles());
        diskBufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        diskFileMapProvider = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        memBufferCache = new InMemoryBufferCache(new HeapBufferAllocator(), getMemPageSize(), getDiskPageSize());
        rnd.setSeed(RANDOM_SEED);
    }
    
    @After
    public void tearDown() throws HyracksDataException {
        diskBufferCache.close();
        File f = new File(onDiskDir);
        f.deleteOnExit();
    }
    
    public int getDiskPageSize() {
        return DISK_PAGE_SIZE;
    }
    
    public int getDiskNumPages() {
        return DISK_NUM_PAGES;
    }
    
    public int getDiskMaxOpenFiles() {
        return DISK_MAX_OPEN_FILES;
    }
    
    public int getMemPageSize() {
        return MEM_PAGE_SIZE;
    }
    
    public int getMemNumPages() {
        return MEM_NUM_PAGES;
    }
    
    public int getHyracksFrameSize() {
        return HYRACKS_FRAME_SIZE;
    }
}
