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

package edu.uci.ics.hyracks.storage.am.btree.util;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class BTreeTestHarness {    
    public static final BTreeLeafFrameType[] LEAF_FRAMES_TO_TEST = new BTreeLeafFrameType[] {
        BTreeLeafFrameType.REGULAR_NSM, BTreeLeafFrameType.FIELD_PREFIX_COMPRESSED_NSM };
    
    private static final long RANDOM_SEED = 50;
    private static final int DEFAULT_PAGE_SIZE = 256;
    private static final int DEFAULT_NUM_PAGES = 100;
    private static final int DEFAULT_MAX_OPEN_FILES = 10;
    private static final int DEFAULT_HYRACKS_FRAME_SIZE = 128;
    
    protected final int pageSize;
    protected final int numPages;
    protected final int maxOpenFiles;
    protected final int hyracksFrameSize;
        
    protected IHyracksTaskContext ctx; 
    protected IBufferCache bufferCache;
    protected int btreeFileId;
    
    protected final Random rnd = new Random();
    protected final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final String tmpDir = System.getProperty("java.io.tmpdir");
    protected final String sep = System.getProperty("file.separator");
    protected String fileName;
    
    public BTreeTestHarness() {
    	this.pageSize = DEFAULT_PAGE_SIZE;
    	this.numPages = DEFAULT_NUM_PAGES;
    	this.maxOpenFiles = DEFAULT_MAX_OPEN_FILES;
    	this.hyracksFrameSize = DEFAULT_HYRACKS_FRAME_SIZE;
    }
    
    public BTreeTestHarness(int pageSize, int numPages, int maxOpenFiles, int hyracksFrameSize) {
    	this.pageSize = pageSize;
    	this.numPages = numPages;
    	this.maxOpenFiles = maxOpenFiles;
    	this.hyracksFrameSize = hyracksFrameSize;
    }
    
    public void setUp() throws HyracksDataException {
        fileName = tmpDir + sep + simpleDateFormat.format(new Date());
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(pageSize, numPages, maxOpenFiles);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        btreeFileId = fmp.lookupFileId(file);
        bufferCache.openFile(btreeFileId);
        rnd.setSeed(RANDOM_SEED);
    }
    
    public void tearDown() throws HyracksDataException {
        bufferCache.closeFile(btreeFileId);
        bufferCache.close();
        File f = new File(fileName);
        f.deleteOnExit();
    }
    
    public IHyracksTaskContext getHyracksTaskContext() {
    	return ctx;
    }
    
    public IBufferCache getBufferCache() {
    	return bufferCache;
    }
    
    public int getBTreeFileId() {
    	return btreeFileId;
    }
    
    public String getFileName() {
        return fileName;
    }
    
    public Random getRandom() {
    	return rnd;
    }
    
    public int getPageSize() {
        return pageSize;
    }
    
    public int getNumPages() {
        return numPages;
    }
    
    public int getHyracksFrameSize() {
        return hyracksFrameSize;
    }
    
    public int getMaxOpenFiles() {
        return maxOpenFiles;
    }
    
    public IOperationCallback getOpCallback() {
        return NoOpOperationCallback.INSTANCE;
    }
}
