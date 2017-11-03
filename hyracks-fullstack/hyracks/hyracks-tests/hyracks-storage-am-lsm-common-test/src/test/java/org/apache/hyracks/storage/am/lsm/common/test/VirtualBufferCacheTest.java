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
package org.apache.hyracks.storage.am.lsm.common.test;

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.SingleThreadEventProcessor;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.junit.Assert;
import org.junit.Test;

public class VirtualBufferCacheTest {
    /*
     * Missing tests:
     * 0. concurrent pinnings for a single file from multiple threads
     * 1. concurrent create file
     * 2. file deletes while pages are pinned? Note that currently, the vbc doesn't keep track of number of pinnings
     */
    private static class TestExtraPageBlockHelper implements IExtraPageBlockHelper {
        private final int fileId;
        private int pinCount;
        private Set<ICachedPage> pinnedPages;
        private int totalNumPages;

        public TestExtraPageBlockHelper(int fileId) {
            this.fileId = fileId;
            pinCount = 0;
            pinnedPages = new HashSet<>();
        }

        @Override
        public int getFreeBlock(int size) throws HyracksDataException {
            int before = totalNumPages;
            totalNumPages += size - 1;
            return before;
        }

        @Override
        public void returnFreePageBlock(int blockPageId, int size) throws HyracksDataException {
            // Do nothing. we don't reclaim large pages from file in this test
        }

        public void pin(VirtualBufferCache vbc, int multiplier) throws HyracksDataException {
            ICachedPage p = vbc.pin(BufferedFileHandle.getDiskPageId(fileId, pinCount), true);
            pinnedPages.add(p);
            pinCount++;
            totalNumPages++;
            if (multiplier > 1) {
                vbc.resizePage(p, multiplier, this);
            }
        }

    }

    private static class FileState {
        private final VirtualBufferCache vbc;
        private final int fileId;
        private final TestExtraPageBlockHelper helper;
        private FileReference fileRef;

        public FileState(VirtualBufferCache vbc, String fileName) throws HyracksDataException {
            this.vbc = vbc;
            IOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
            fileRef = ioManager.resolve(fileName);
            vbc.createFile(fileRef);
            fileId = vbc.getFileMapProvider().lookupFileId(fileRef);
            helper = new TestExtraPageBlockHelper(fileId);
        }

        public void pin(int multiplier) throws HyracksDataException {
            helper.pin(vbc, multiplier);
        }
    }

    private static class Request {
        private enum Type {
            PIN_PAGE,
            CALLBACK
        }

        private final Type type;
        private boolean done;

        public Request(Type type) {
            this.type = type;
            done = false;
        }

        Type getType() {
            return type;
        }

        synchronized void complete() {
            done = true;
            notifyAll();
        }

        synchronized void await() throws InterruptedException {
            while (!done) {
                wait();
            }
        }
    }

    public class User extends SingleThreadEventProcessor<Request> {
        private final VirtualBufferCache vbc;
        private final FileState fileState;

        public User(String name, VirtualBufferCache vbc, FileState fileState) throws HyracksDataException {
            super(name);
            this.vbc = vbc;
            this.fileState = fileState;
        }

        @Override
        protected void handle(Request req) throws Exception {
            try {
                switch (req.getType()) {
                    case PIN_PAGE:
                        ICachedPage p = vbc.pin(
                                BufferedFileHandle.getDiskPageId(fileState.fileId, fileState.helper.pinCount), true);
                        fileState.helper.pinnedPages.add(p);
                        ++fileState.helper.pinCount;
                        break;
                    default:
                        break;
                }
            } finally {
                req.complete();
            }
        }
    }

    /**
     * Pins NUM_PAGES randomly distributed across NUM_FILES and checks that each
     * set of cached pages pinned on behalf of a file are disjoint from all other sets of
     * cached pages pinned on behalf of other files.
     * Additionally, the test perform the same test when pinning over soft cap (NUM_PAGES)
     * of pages.
     */
    @Test
    public void testDisjointPins() throws Exception {
        final int numOverpin = 128;
        final int pageSize = 256;
        final int numFiles = 10;
        final int numPages = 1000;
        Random random = new Random();
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        VirtualBufferCache vbc = new VirtualBufferCache(allocator, pageSize, numPages);
        vbc.open();
        FileState[] fileStates = new FileState[numFiles];
        for (int i = 0; i < numFiles; i++) {
            fileStates[i] = new FileState(vbc, String.format("f%d", i));
        }

        kPins(numPages, numFiles, fileStates, vbc, random);
        assertTrue(pagesDisjointed(numFiles, fileStates));

        kPins(numOverpin, numFiles, fileStates, vbc, random);
        assertTrue(pagesDisjointed(numFiles, fileStates));

        deleteFilesAndCheckMemory(numFiles, fileStates, vbc);
        vbc.close();
    }

    @Test
    public void testConcurrentUsersDifferentFiles() throws Exception {
        final int numOverpin = 128;
        final int pageSize = 256;
        final int numFiles = 10;
        final int numPages = 1000;
        Random random = new Random();
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        VirtualBufferCache vbc = new VirtualBufferCache(allocator, pageSize, numPages);
        vbc.open();
        FileState[] fileStates = new FileState[numFiles];
        User[] users = new User[numFiles];
        for (int i = 0; i < numFiles; i++) {
            fileStates[i] = new FileState(vbc, String.format("f%d", i));
            users[i] = new User("User-" + i, vbc, fileStates[i]);
        }
        for (int i = 0; i < numPages; i++) {
            int fsIdx = random.nextInt(numFiles);
            users[fsIdx].add(new Request(Request.Type.PIN_PAGE));
        }
        // ensure all are done
        wait(users);
        assertTrue(pagesDisjointed(numFiles, fileStates));
        for (int i = 0; i < numOverpin; i++) {
            int fsIdx = random.nextInt(numFiles);
            users[fsIdx].add(new Request(Request.Type.PIN_PAGE));
        }
        // ensure all are done
        wait(users);
        assertTrue(pagesDisjointed(numFiles, fileStates));
        // shutdown users
        shutdown(users);
        deleteFilesAndCheckMemory(numFiles, fileStates, vbc);
        vbc.close();
    }

    private void shutdown(User[] users) throws HyracksDataException, InterruptedException {
        for (int i = 0; i < users.length; i++) {
            users[i].stop();
        }
    }

    private void wait(User[] users) throws InterruptedException {
        for (int i = 0; i < users.length; i++) {
            Request callback = new Request(Request.Type.CALLBACK);
            users[i].add(callback);
            callback.await();
        }
    }

    @Test
    public void testLargePages() throws Exception {
        final int pageSize = 256;
        final int numFiles = 3;
        final int numPages = 1000;
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        VirtualBufferCache vbc = new VirtualBufferCache(allocator, pageSize, numPages);
        vbc.open();
        FileState[] fileStates = new FileState[numFiles];
        for (int i = 0; i < numFiles; i++) {
            fileStates[i] = new FileState(vbc, String.format("f%d", i));
        }
        // Get a large page that is 52 pages size
        int fileIdx = 0;
        FileState f = fileStates[fileIdx];
        f.pin(52);
        // Assert that 52 pages are accounted for
        Assert.assertEquals(52, vbc.getUsage());
        // Delete file
        vbc.deleteFile(f.fileId);
        // Assert that usage fell down to 0
        Assert.assertEquals(0, vbc.getUsage());
        // Assert that no pages are pre-allocated
        Assert.assertEquals(0, vbc.getPreAllocatedPages());
        // Next file
        fileIdx++;
        f = fileStates[fileIdx];
        // Pin small pages to capacity
        int count = 0;
        while (vbc.getUsage() <= vbc.getPageBudget()) {
            f.pin(1);
            count++;
            Assert.assertEquals(count, vbc.getUsage());
        }
        // Delete file
        vbc.deleteFile(f.fileRef);
        // Assert that usage fell down to 0
        Assert.assertEquals(0, vbc.getUsage());
        // Assert that small pages are available
        Assert.assertEquals(vbc.getPreAllocatedPages(), vbc.getPageBudget());
        // Next file
        fileIdx++;
        f = fileStates[fileIdx];
        count = 0;
        int sizeOfLargePage = 4;
        while (vbc.getUsage() <= vbc.getPageBudget()) {
            f.pin(sizeOfLargePage);
            count += sizeOfLargePage;
            Assert.assertEquals(count, vbc.getUsage());
            Assert.assertEquals(Integer.max(0, vbc.getPageBudget() - count), vbc.getPreAllocatedPages());
        }
        // Delete file
        vbc.deleteFile(f.fileId);
        // Assert that usage fell down to 0
        Assert.assertEquals(0, vbc.getUsage());
        // Assert that no pages are pre-allocated
        Assert.assertEquals(0, vbc.getPreAllocatedPages());
        vbc.close();
    }

    private boolean pagesDisjointed(int numFiles, FileState[] fileStates) {
        boolean disjoint = true;
        for (int i = 0; i < numFiles; i++) {
            FileState fi = fileStates[i];
            for (int j = i + 1; j < numFiles; j++) {
                FileState fj = fileStates[j];
                disjoint = disjoint && Collections.disjoint(fi.helper.pinnedPages, fj.helper.pinnedPages);
            }
        }
        return disjoint;
    }

    private void deleteFilesAndCheckMemory(int numFiles, FileState[] fileStates, VirtualBufferCache vbc)
            throws Exception {
        // Get the size of the buffer cache
        int totalInStates = 0;
        for (int i = 0; i < numFiles; i++) {
            totalInStates += fileStates[i].helper.pinnedPages.size();
        }
        Assert.assertEquals(totalInStates, vbc.getUsage());
        int totalFree = 0;
        Assert.assertEquals(totalFree, vbc.getPreAllocatedPages());
        boolean hasLargePages = vbc.getLargePages() > 0;
        for (int i = 0; i < numFiles; i++) {
            int expectedToBeReclaimed = 0;
            for (ICachedPage page : fileStates[i].helper.pinnedPages) {
                expectedToBeReclaimed += page.getFrameSizeMultiplier();
            }
            vbc.deleteFile(fileStates[i].fileId);
            totalFree += expectedToBeReclaimed;
            Assert.assertEquals(totalInStates - totalFree, vbc.getUsage());
            if (!hasLargePages) {
                Assert.assertEquals(Integer.max(0, vbc.getPageBudget() - vbc.getUsage()), vbc.getPreAllocatedPages());
            }
        }
    }

    private void kPins(int k, int numFiles, FileState[] fileStates, VirtualBufferCache vbc, Random random)
            throws Exception {
        int numPinned = 0;
        while (numPinned < k) {
            int fsIdx = random.nextInt(numFiles);
            FileState f = fileStates[fsIdx];
            f.pin(1);
            ++numPinned;
        }
    }
}
