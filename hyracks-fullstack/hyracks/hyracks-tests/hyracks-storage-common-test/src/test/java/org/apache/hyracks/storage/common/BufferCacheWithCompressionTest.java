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
import org.apache.hyracks.api.compression.ICompressorDecompressor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.HaltOnFailureCallback;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.storage.common.compression.SnappyCompressorDecompressorFactory;
import org.apache.hyracks.storage.common.compression.file.CompressedFileReference;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

public class BufferCacheWithCompressionTest {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final List<String> openFiles = new ArrayList<>();
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");

    private static final ICompressorDecompressor compDecomp =
            (new SnappyCompressorDecompressorFactory()).createInstance();
    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 10;
    private static final int MAX_OPEN_DATA_FILES = 20;
    //Additional file (LAF) for each compressed file
    private static final int ACTUAL_MAX_OPEN_FILE = MAX_OPEN_DATA_FILES * 2;
    private static final int HYRACKS_FRAME_SIZE = PAGE_SIZE;
    private IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    private static final Random rnd = new Random(50);

    private FileReference getFileReference(IIOManager ioManager) throws HyracksDataException {
        String fileName = simpleDateFormat.format(new Date()) + openFiles.size();
        final FileReference fileRef = ioManager.resolve(fileName);
        final CompressedFileReference cFileRef = new CompressedFileReference(fileRef.getDeviceHandle(), compDecomp,
                fileRef.getRelativePath(), fileRef.getRelativePath() + ".dic");

        openFiles.add(fileName);
        openFiles.add(cFileRef.getLAFRelativePath());
        return cFileRef;
    }

    @Test
    public void interruptPinTest() throws Exception {
        final int bufferCacheNumPages = 4;
        TestStorageManagerComponentHolder.init(PAGE_SIZE, bufferCacheNumPages, ACTUAL_MAX_OPEN_FILE);
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
        IBufferCache bufferCache =
                TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        final long duration = TimeUnit.SECONDS.toMillis(20);
        final FileReference file = getFileReference(ioManager);
        final int fileId = bufferCache.createFile(file);
        final int numPages = 16;
        bufferCache.openFile(fileId);
        final ICompressedPageWriter writer = bufferCache.getCompressedPageWriter(fileId);
        final IFIFOPageWriter pageWriter =
                bufferCache.createFIFOWriter(NoOpPageWriteCallback.INSTANCE, HaltOnFailureCallback.INSTANCE);
        for (int i = 0; i < numPages; i++) {
            long dpid = BufferedFileHandle.getDiskPageId(fileId, i);
            ICachedPage page = bufferCache.confiscatePage(dpid);
            writer.prepareWrite(page);
            page.getBuffer().putInt(0, i);
            pageWriter.write(page);
        }
        writer.endWriting();
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

    /**
     * Compressed files are immutable.
     */
    @Test
    public void simpleOpenPinCloseTest() throws HyracksException {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, ACTUAL_MAX_OPEN_FILE);
        IBufferCache bufferCache =
                TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());

        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
        FileReference file = getFileReference(ioManager);
        int fileId = bufferCache.createFile(file);
        int num = 10;
        int testPageId = 0;

        bufferCache.openFile(fileId);
        final ICompressedPageWriter writer = bufferCache.getCompressedPageWriter(fileId);

        ICachedPage page = null;

        // confiscating a page should succeed
        page = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, testPageId));
        writer.prepareWrite(page);

        for (int i = 0; i < num; i++) {
            page.getBuffer().putInt(i * 4, i);
        }
        final IFIFOPageWriter pageWriter =
                bufferCache.createFIFOWriter(NoOpPageWriteCallback.INSTANCE, HaltOnFailureCallback.INSTANCE);
        pageWriter.write(page);
        writer.endWriting();
        bufferCache.closeFile(fileId);

        // open file again
        bufferCache.openFile(fileId);

        // tryPin should succeed because page should still be cached
        page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, testPageId), false);
        Assert.assertNotNull(page);
        try {
            // verify contents of page
            for (int i = 0; i < num; i++) {
                Assert.assertEquals(page.getBuffer().getInt(i * 4), i);
            }
        } finally {
            bufferCache.unpin(page);
        }

        bufferCache.closeFile(fileId);
        bufferCache.close();
    }

    @Test
    public void contentCheckingMaxOpenFilesTest() throws HyracksException {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, ACTUAL_MAX_OPEN_FILE);
        IBufferCache bufferCache =
                TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();

        List<Integer> fileIds = new ArrayList<>();
        Map<Integer, ArrayList<Integer>> pageContents = new HashMap<>();
        int num = 10;
        int testPageId = 0;

        // open max number of files and write some stuff into their first page
        for (int i = 0; i < MAX_OPEN_DATA_FILES; i++) {
            FileReference file = getFileReference(ioManager);
            int fileId = bufferCache.createFile(file);
            fileIds.add(fileId);
            bufferCache.openFile(fileId);
            final ICompressedPageWriter writer = bufferCache.getCompressedPageWriter(fileId);
            ICachedPage page = null;
            page = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, testPageId));
            writer.prepareWrite(page);
            ArrayList<Integer> values = new ArrayList<>();
            for (int j = 0; j < num; j++) {
                int x = Math.abs(rnd.nextInt());
                page.getBuffer().putInt(j * 4, x);
                values.add(x);
            }
            pageContents.put(fileId, values);
            final IFIFOPageWriter pageWriter =
                    bufferCache.createFIFOWriter(NoOpPageWriteCallback.INSTANCE, HaltOnFailureCallback.INSTANCE);
            pageWriter.write(page);
            writer.endWriting();
        }

        boolean exceptionThrown = false;

        // since all files are open, next open should fail
        try {
            FileReference file = getFileReference(ioManager);
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
            FileReference file = getFileReference(ioManager);
            int fileId = bufferCache.createFile(file);
            bufferCache.openFile(fileId);
            fileIds.add(fileId);
        }

        // since all files are open, next open should fail
        exceptionThrown = false;
        try {
            FileReference file = getFileReference(ioManager);
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
            try {
                ArrayList<Integer> values = pageContents.get(closedFileId);
                for (int j = 0; j < values.size(); j++) {
                    Assert.assertEquals(values.get(j).intValue(), page.getBuffer().getInt(j * 4));
                }
            } finally {
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
        TestStorageManagerComponentHolder.init(PAGE_SIZE, 200, ACTUAL_MAX_OPEN_FILE);
        IBufferCache bufferCache =
                TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
        FileReference file = getFileReference(ioManager);
        int fileId = bufferCache.createFile(file);
        int testPageId = 0;
        bufferCache.openFile(fileId);
        bufferCache.getCompressedPageWriter(fileId).endWriting();

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
        for (String s : openFiles) {
            File f = new File(s);
            f.deleteOnExit();
        }
    }

}
