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

package org.apache.hyracks.storage.am.btree.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.IPageManagerFactory;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;

public class BTreeTestHarness {
    public static final BTreeLeafFrameType[] LEAF_FRAMES_TO_TEST =
            new BTreeLeafFrameType[] { BTreeLeafFrameType.REGULAR_NSM, BTreeLeafFrameType.FIELD_PREFIX_COMPRESSED_NSM };

    private static final long RANDOM_SEED = 50;

    protected final int pageSize;
    protected final int numPages;
    protected final int maxOpenFiles;
    protected final int hyracksFrameSize;

    protected IHyracksTaskContext ctx;
    protected IBufferCache bufferCache;
    protected FileReference file;
    protected IMetadataPageManagerFactory pageManagerFactory;

    protected final Random rnd = new Random();
    protected final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final String tmpDir = System.getProperty("java.io.tmpdir");
    protected final String sep = System.getProperty("file.separator");

    public BTreeTestHarness() {
        this.pageSize = AccessMethodTestsConfig.BTREE_PAGE_SIZE;
        this.numPages = AccessMethodTestsConfig.BTREE_NUM_PAGES;
        this.maxOpenFiles = AccessMethodTestsConfig.BTREE_MAX_OPEN_FILES;
        this.hyracksFrameSize = AccessMethodTestsConfig.BTREE_HYRACKS_FRAME_SIZE;
    }

    public BTreeTestHarness(int pageSize, int numPages, int maxOpenFiles, int hyracksFrameSize) {
        this.pageSize = pageSize;
        this.numPages = numPages;
        this.maxOpenFiles = maxOpenFiles;
        this.hyracksFrameSize = hyracksFrameSize;
    }

    public void setUp() throws HyracksDataException {
        ctx = TestUtils.create(getHyracksFrameSize());
        TestStorageManagerComponentHolder.init(pageSize, numPages, maxOpenFiles);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        file = ctx.getIoManager().getFileReference(0, simpleDateFormat.format(new Date()));
        pageManagerFactory = new LinkedMetadataPageManagerFactory();
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

    public FileReference getFileReference() {
        return file;
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

    public IPageManagerFactory getPageManagerFactory() {
        return pageManagerFactory;
    }
}
