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

package edu.uci.ics.hyracks.storage.am.rtree.utils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class RTreeTestHarness {

    private static final long RANDOM_SEED = 50;

    protected final int pageSize;
    protected final int numPages;
    protected final int maxOpenFiles;
    protected final int hyracksFrameSize;

    protected IHyracksTaskContext ctx;
    protected IBufferCache bufferCache;
    protected IFileMapProvider fileMapProvider;
    protected int treeFileId;

    protected final Random rnd = new Random();
    protected final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final String tmpDir = System.getProperty("java.io.tmpdir");
    protected final String sep = System.getProperty("file.separator");
    protected String fileName;
    protected FileReference file;

    public RTreeTestHarness() {
        this.pageSize = AccessMethodTestsConfig.RTREE_PAGE_SIZE;
        this.numPages = AccessMethodTestsConfig.RTREE_NUM_PAGES;
        this.maxOpenFiles = AccessMethodTestsConfig.RTREE_MAX_OPEN_FILES;
        this.hyracksFrameSize = AccessMethodTestsConfig.RTREE_HYRACKS_FRAME_SIZE;
    }

    public RTreeTestHarness(int pageSize, int numPages, int maxOpenFiles, int hyracksFrameSize) {
        this.pageSize = pageSize;
        this.numPages = numPages;
        this.maxOpenFiles = maxOpenFiles;
        this.hyracksFrameSize = hyracksFrameSize;
    }

    public void setUp() throws HyracksDataException {
        fileName = tmpDir + sep + simpleDateFormat.format(new Date());
        file = new FileReference(new File(fileName));
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(pageSize, numPages, maxOpenFiles);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        fileMapProvider = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        rnd.setSeed(RANDOM_SEED);
    }

    public void tearDown() throws HyracksDataException {
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

    public IFileMapProvider getFileMapProvider() {
        return fileMapProvider;
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

    public FileReference getFileReference() {
        return file;
    }
}
