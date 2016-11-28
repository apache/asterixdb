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
package org.apache.hyracks.storage.common;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapProvider;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

public class BufferCacheTest {
    protected static final List<String> openedFiles = new ArrayList<>();
    protected static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected static final String tmpDir = System.getProperty("java.io.tmpdir");
    protected static final String sep = System.getProperty("file.separator");

    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 10;
    private static final int MAX_OPEN_FILES = 20;
    private static final int HYRACKS_FRAME_SIZE = PAGE_SIZE;
    private IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    private static final Random rnd = new Random(50);

    private String getFileName() {
        String fileName = tmpDir + sep + simpleDateFormat.format(new Date()) + openedFiles.size();
        openedFiles.add(fileName);
        return fileName;
    }

    @Test
    public void simpleOpenPinCloseTest() throws HyracksException {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
        String fileName = getFileName();
        FileReference file = ioManager.resolveAbsolutePath(fileName);
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        int num = 10;
        int testPageId = 0;

        bufferCache.openFile(fileId);

        ICachedPage page = null;

        // tryPin should fail
        page = bufferCache.tryPin(BufferedFileHandle.getDiskPageId(fileId, testPageId));
        Assert.assertNull(page);

        // pin page should succeed
        page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, testPageId), true);
        page.acquireWriteLatch();
        try {
            for (int i = 0; i < num; i++) {
                page.getBuffer().putInt(i * 4, i);
            }

            // try pin should succeed
            ICachedPage page2 = bufferCache.tryPin(BufferedFileHandle.getDiskPageId(fileId, testPageId));
            Assert.assertNotNull(page2);
            bufferCache.unpin(page2);

        } finally {
            page.releaseWriteLatch(true);
            bufferCache.unpin(page);
        }

        bufferCache.closeFile(fileId);

        // This code is commented because the method pinSanityCheck in the BufferCache is commented.
        /*boolean exceptionThrown = false;

        // tryPin should fail since file is not open
        try {
            page = bufferCache.tryPin(BufferedFileHandle.getDiskPageId(fileId, testPageId));
        } catch (HyracksDataException e) {
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);

        // pin should fail since file is not open
        exceptionThrown = false;
        try {
            page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, testPageId), false);
        } catch (HyracksDataException e) {
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);*/

        // open file again
        bufferCache.openFile(fileId);

        // tryPin should succeed because page should still be cached
        page = bufferCache.tryPin(BufferedFileHandle.getDiskPageId(fileId, testPageId));
        Assert.assertNotNull(page);
        page.acquireReadLatch();
        try {
            // verify contents of page
            for (int i = 0; i < num; i++) {
                Assert.assertEquals(page.getBuffer().getInt(i * 4), i);
            }
        } finally {
            page.releaseReadLatch();
            bufferCache.unpin(page);
        }

        bufferCache.closeFile(fileId);
        bufferCache.close();
    }

    @Test
    public void simpleMaxOpenFilesTest() throws HyracksException {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();

        List<Integer> fileIds = new ArrayList<>();

        for (int i = 0; i < MAX_OPEN_FILES; i++) {
            String fileName = getFileName();

            FileReference file = ioManager.resolveAbsolutePath(fileName);
            bufferCache.createFile(file);
            int fileId = fmp.lookupFileId(file);
            bufferCache.openFile(fileId);
            fileIds.add(fileId);
        }

        boolean exceptionThrown = false;

        // since all files are open, next open should fail
        try {
            String fileName = getFileName();
            FileReference file = ioManager.resolveAbsolutePath(fileName);
            bufferCache.createFile(file);
            int fileId = fmp.lookupFileId(file);
            bufferCache.openFile(fileId);
        } catch (HyracksDataException e) {
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);

        // close a random file
        int ix = Math.abs(rnd.nextInt()) % fileIds.size();
        bufferCache.closeFile(fileIds.get(ix));
        fileIds.remove(ix);

        // now open should succeed again
        exceptionThrown = false;
        try {
            String fileName = getFileName();
            FileReference file = ioManager.resolveAbsolutePath(fileName);
            bufferCache.createFile(file);
            int fileId = fmp.lookupFileId(file);
            bufferCache.openFile(fileId);
            fileIds.add(fileId);

        } catch (HyracksDataException e) {
            exceptionThrown = true;
        }
        Assert.assertFalse(exceptionThrown);

        for (Integer i : fileIds) {
            bufferCache.closeFile(i.intValue());
        }

        bufferCache.close();
    }

    @Test
    public void contentCheckingMaxOpenFilesTest() throws HyracksException {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();

        List<Integer> fileIds = new ArrayList<>();
        Map<Integer, ArrayList<Integer>> pageContents = new HashMap<>();
        int num = 10;
        int testPageId = 0;

        // open max number of files and write some stuff into their first page
        for (int i = 0; i < MAX_OPEN_FILES; i++) {
            String fileName = getFileName();
            FileReference file = ioManager.resolveAbsolutePath(fileName);
            bufferCache.createFile(file);
            int fileId = fmp.lookupFileId(file);
            bufferCache.openFile(fileId);
            fileIds.add(fileId);

            ICachedPage page = null;
            page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, testPageId), true);
            page.acquireWriteLatch();
            try {
                ArrayList<Integer> values = new ArrayList<>();
                for (int j = 0; j < num; j++) {
                    int x = Math.abs(rnd.nextInt());
                    page.getBuffer().putInt(j * 4, x);
                    values.add(x);
                }
                pageContents.put(fileId, values);
            } finally {
                page.releaseWriteLatch(true);
                bufferCache.unpin(page);
            }
        }

        boolean exceptionThrown = false;

        // since all files are open, next open should fail
        try {
            String fileName = getFileName();
            FileReference file = ioManager.resolveAbsolutePath(fileName);
            bufferCache.createFile(file);
            int fileId = fmp.lookupFileId(file);
            bufferCache.openFile(fileId);
        } catch (HyracksDataException e) {
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);

        // close a few random files
        ArrayList<Integer> closedFileIds = new ArrayList<>();
        int filesToClose = 5;
        for (int i = 0; i < filesToClose; i++) {
            int ix = Math.abs(rnd.nextInt()) % fileIds.size();
            bufferCache.closeFile(fileIds.get(ix));
            closedFileIds.add(fileIds.get(ix));
            fileIds.remove(ix);
        }

        // now open a few new files
        for (int i = 0; i < filesToClose; i++) {
            String fileName = getFileName();
            FileReference file = ioManager.resolveAbsolutePath(fileName);
            bufferCache.createFile(file);
            int fileId = fmp.lookupFileId(file);
            bufferCache.openFile(fileId);
            fileIds.add(fileId);
        }

        // since all files are open, next open should fail
        try {
            String fileName = getFileName();
            FileReference file = ioManager.resolveAbsolutePath(fileName);
            bufferCache.createFile(file);
            int fileId = fmp.lookupFileId(file);
            bufferCache.openFile(fileId);
        } catch (HyracksDataException e) {
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);

        // close a few random files again
        for (int i = 0; i < filesToClose; i++) {
            int ix = Math.abs(rnd.nextInt()) % fileIds.size();
            bufferCache.closeFile(fileIds.get(ix));
            closedFileIds.add(fileIds.get(ix));
            fileIds.remove(ix);
        }

        // now open those closed files again and verify their contents
        for (int i = 0; i < filesToClose; i++) {
            int closedFileId = closedFileIds.get(i);
            bufferCache.openFile(closedFileId);
            fileIds.add(closedFileId);

            // pin first page and verify contents
            ICachedPage page = null;
            page = bufferCache.pin(BufferedFileHandle.getDiskPageId(closedFileId, testPageId), false);
            page.acquireReadLatch();
            try {
                ArrayList<Integer> values = pageContents.get(closedFileId);
                for (int j = 0; j < values.size(); j++) {
                    Assert.assertEquals(values.get(j).intValue(), page.getBuffer().getInt(j * 4));
                }
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }

        for (Integer i : fileIds) {
            bufferCache.closeFile(i.intValue());
        }

        bufferCache.close();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        for (String s : openedFiles) {
            File f = new File(s);
            f.deleteOnExit();
        }
    }
}
