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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.HaltOnFailureCallback;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

public class BufferCacheTest {
    private static final Logger LOGGER = LogManager.getLogger();
    protected static final List<String> openedFiles = new ArrayList<>();
    protected static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");

    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 10;
    private static final int MAX_OPEN_FILES = 20;
    private static final int HYRACKS_FRAME_SIZE = PAGE_SIZE;
    private IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    private static final Random rnd = new Random(50);

    private String getFileName() {
        String fileName = simpleDateFormat.format(new Date()) + openedFiles.size();
        openedFiles.add(fileName);
        return fileName;
    }

    @Test
    public void interruptPinTest() throws Exception {
        /*
         * This test will create a buffer cache of a small size (4 pages)
         * and then will create a file of size = 16 pages and have 4 threads
         * pin and unpin the pages one by one. and another thread interrupts them
         * for some time.. It then will close the file and ensure that all the pages are
         * unpinned and that no problems are found
         */
        final int bufferCacheNumPages = 4;
        TestStorageManagerComponentHolder.init(PAGE_SIZE, bufferCacheNumPages, MAX_OPEN_FILES);
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
        IBufferCache bufferCache =
                TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        final long duration = TimeUnit.SECONDS.toMillis(20);
        final String fileName = getFileName();
        final FileReference file = ioManager.resolve(fileName);
        final int fileId = bufferCache.createFile(file);
        final int numPages = 16;
        bufferCache.openFile(fileId);
        for (int i = 0; i < numPages; i++) {
            long dpid = BufferedFileHandle.getDiskPageId(fileId, i);
            ICachedPage page = bufferCache.confiscatePage(dpid);
            page.getBuffer().putInt(0, i);
            bufferCache.createFIFOWriter(NoOpPageWriteCallback.INSTANCE, HaltOnFailureCallback.INSTANCE).write(page);
        }
        bufferCache.closeFile(fileId);
        ExecutorService executor = Executors.newFixedThreadPool(bufferCacheNumPages);
        MutableObject<Thread>[] readers = new MutableObject[bufferCacheNumPages];
        Future<Void>[] futures = new Future[bufferCacheNumPages];
        for (int i = 0; i < bufferCacheNumPages; i++) {
            readers[i] = new MutableObject<>();
            final int threadNumber = i;
            futures[i] = executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    synchronized (readers[threadNumber]) {
                        readers[threadNumber].setValue(Thread.currentThread());
                        readers[threadNumber].notifyAll();
                    }
                    // for duration, just read the pages one by one.
                    // At the end, close the file
                    bufferCache.openFile(fileId);
                    final long start = System.currentTimeMillis();
                    int pageNumber = 0;
                    int totalReads = 0;
                    int successfulReads = 0;
                    int interruptedReads = 0;
                    while (System.currentTimeMillis() - start < duration) {
                        totalReads++;
                        pageNumber = (pageNumber + 1) % numPages;
                        try {
                            long dpid = BufferedFileHandle.getDiskPageId(fileId, pageNumber);
                            ICachedPage page = bufferCache.pin(dpid, false);
                            successfulReads++;
                            bufferCache.unpin(page);
                        } catch (HyracksDataException hde) {
                            interruptedReads++;
                            // clear
                            Thread.interrupted();
                        }
                    }
                    bufferCache.closeFile(fileId);
                    LOGGER.log(Level.INFO, "Total reads = " + totalReads + " Successful Reads = " + successfulReads
                            + " Interrupted Reads = " + interruptedReads);
                    return null;
                }
            });
        }

        for (int i = 0; i < bufferCacheNumPages; i++) {
            synchronized (readers[i]) {
                while (readers[i].getValue() == null) {
                    readers[i].wait();
                }
            }
        }
        final long start = System.currentTimeMillis();

        while (System.currentTimeMillis() - start < duration) {
            for (int i = 0; i < bufferCacheNumPages; i++) {
                readers[i].getValue().interrupt();
            }
            Thread.sleep(25); // NOSONAR Sleep so some reads are successful
        }
        try {
            for (int i = 0; i < bufferCacheNumPages; i++) {
                futures[i].get();
            }
        } finally {
            bufferCache.deleteFile(fileId);
            bufferCache.close();
        }
    }

    @Test
    public void simpleOpenPinCloseTest() throws HyracksException {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache =
                TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());

        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
        String fileName = getFileName();
        FileReference file = ioManager.resolve(fileName);
        int fileId = bufferCache.createFile(file);
        int num = 10;
        int testPageId = 0;

        bufferCache.openFile(fileId);

        ICachedPage page = null;

        // pin page should succeed
        page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, testPageId), true);
        page.acquireWriteLatch();
        try {
            for (int i = 0; i < num; i++) {
                page.getBuffer().putInt(i * 4, i);
            }
        } finally {
            page.releaseWriteLatch(true);
            bufferCache.unpin(page);
        }

        bufferCache.closeFile(fileId);

        // open file again
        bufferCache.openFile(fileId);

        // tryPin should succeed because page should still be cached
        page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, testPageId), false);
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
        IBufferCache bufferCache =
                TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();

        List<Integer> fileIds = new ArrayList<>();

        for (int i = 0; i < MAX_OPEN_FILES; i++) {
            String fileName = getFileName();

            FileReference file = ioManager.resolve(fileName);
            int fileId = bufferCache.createFile(file);
            bufferCache.openFile(fileId);
            fileIds.add(fileId);
        }

        boolean exceptionThrown = false;

        // since all files are open, next open should fail
        try {
            String fileName = getFileName();
            FileReference file = ioManager.resolve(fileName);
            int fileId = bufferCache.createFile(file);
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
            FileReference file = ioManager.resolve(fileName);
            int fileId = bufferCache.createFile(file);
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
        IBufferCache bufferCache =
                TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();

        List<Integer> fileIds = new ArrayList<>();
        Map<Integer, ArrayList<Integer>> pageContents = new HashMap<>();
        int num = 10;
        int testPageId = 0;

        // open max number of files and write some stuff into their first page
        for (int i = 0; i < MAX_OPEN_FILES; i++) {
            String fileName = getFileName();
            FileReference file = ioManager.resolve(fileName);
            int fileId = bufferCache.createFile(file);
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
            FileReference file = ioManager.resolve(fileName);
            int fileId = bufferCache.createFile(file);
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
            FileReference file = ioManager.resolve(fileName);
            int fileId = bufferCache.createFile(file);
            bufferCache.openFile(fileId);
            fileIds.add(fileId);
        }

        // since all files are open, next open should fail
        try {
            String fileName = getFileName();
            FileReference file = ioManager.resolve(fileName);
            int fileId = bufferCache.createFile(file);
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

    @Test
    public void interruptedConcurrentReadTest() throws Exception {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, 200, MAX_OPEN_FILES);
        IBufferCache bufferCache =
                TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
        String fileName = getFileName();
        FileReference file = ioManager.resolve(fileName);
        int fileId = bufferCache.createFile(file);
        int testPageId = 0;
        bufferCache.openFile(fileId);

        final int expectedPinCount = 100;
        final AtomicInteger actualPinCount = new AtomicInteger(0);
        Thread innocentReader = new Thread(() -> {
            Thread interruptedReader = null;
            try {
                for (int i = 0; i < expectedPinCount; i++) {
                    ICachedPage aPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, testPageId), false);
                    bufferCache.unpin(aPage);
                    ((CachedPage) aPage).invalidate();
                    actualPinCount.incrementAndGet();
                    if (i % 10 == 0) {
                        // start an interruptedReader that will cause the channel to closed
                        interruptedReader = new Thread(() -> {
                            try {
                                Thread.currentThread().interrupt();
                                bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, testPageId + 1), false);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                        interruptedReader.start();
                    }
                }
                if (interruptedReader != null) {
                    interruptedReader.join();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        innocentReader.start();
        innocentReader.join();
        // make sure that all reads by the innocentReader succeeded
        Assert.assertEquals(actualPinCount.get(), expectedPinCount);
        // close file
        bufferCache.closeFile(fileId);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        for (String s : openedFiles) {
            File f = new File(s);
            f.deleteOnExit();
        }
    }
}
