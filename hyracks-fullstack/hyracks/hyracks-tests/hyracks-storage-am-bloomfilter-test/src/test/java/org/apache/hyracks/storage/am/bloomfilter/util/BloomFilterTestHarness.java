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

package org.apache.hyracks.storage.am.bloomfilter.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;

public class BloomFilterTestHarness {

    private static final long RANDOM_SEED = 50;

    protected final int pageSize;
    protected final int numPages;
    protected final int maxOpenFiles;
    protected final int hyracksFrameSize;

    protected IIOManager ioManager;
    protected IHyracksTaskContext ctx;
    protected IBufferCache bufferCache;
    protected FileReference file;

    protected final Random rnd = new Random();
    protected final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");

    public BloomFilterTestHarness() {
        this.pageSize = AccessMethodTestsConfig.BLOOM_FILTER_PAGE_SIZE;
        this.numPages = AccessMethodTestsConfig.BLOOM_FILTER_NUM_PAGES;
        this.maxOpenFiles = AccessMethodTestsConfig.BLOOM_FILTER_MAX_OPEN_FILES;
        this.hyracksFrameSize = AccessMethodTestsConfig.BLOOM_FILTER_HYRACKS_FRAME_SIZE;
    }

    public BloomFilterTestHarness(int pageSize, int numPages, int maxOpenFiles, int hyracksFrameSize) {
        this.pageSize = pageSize;
        this.numPages = numPages;
        this.maxOpenFiles = maxOpenFiles;
        this.hyracksFrameSize = hyracksFrameSize;
    }

    public void setUp() throws HyracksDataException {
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(pageSize, numPages, maxOpenFiles);
        ioManager = ctx.getIoManager();
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        file = ioManager.getFileReference(0, simpleDateFormat.format(new Date()));
        rnd.setSeed(RANDOM_SEED);
    }

    public void tearDown() throws HyracksDataException {
        bufferCache.close();
        file.delete();
    }

    public IHyracksTaskContext getHyracksTaskContext() {
        return ctx;
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
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
