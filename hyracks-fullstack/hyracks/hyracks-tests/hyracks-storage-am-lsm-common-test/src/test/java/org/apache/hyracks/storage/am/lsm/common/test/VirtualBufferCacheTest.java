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

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.junit.Test;

public class VirtualBufferCacheTest {
    private static final long SEED = 123456789L;
    private static final int NUM_OVERPIN = 128;
    private static final int PAGE_SIZE = 256;
    private static final int NUM_FILES = 10;
    private static final int NUM_PAGES = 1000;

    private final Random random;
    private final FileState[] fileStates;

    private VirtualBufferCache vbc;
    private IOManager ioManager;

    public VirtualBufferCacheTest() {
        fileStates = new FileState[NUM_FILES];
        for (int i = 0; i < NUM_FILES; i++) {
            fileStates[i] = new FileState();
        }
        random = new Random(SEED);
        vbc = null;
    }

    private static class FileState {
        private int fileId;
        private FileReference fileRef;
        private int pinCount;
        private Set<ICachedPage> pinnedPages;

        public FileState() {
            fileId = -1;
            fileRef = null;
            pinCount = 0;
            pinnedPages = new HashSet<>();
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
    public void test01() throws Exception {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, NUM_FILES);
        ioManager = TestStorageManagerComponentHolder.getIOManager();
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        vbc = new VirtualBufferCache(allocator, PAGE_SIZE, NUM_PAGES);
        vbc.open();
        createFiles();

        kPins(NUM_PAGES);
        assertTrue(pagesDisjointed());

        kPins(NUM_OVERPIN);
        assertTrue(pagesDisjointed());

        deleteFiles();
        vbc.close();
    }

    private boolean pagesDisjointed() {
        boolean disjoint = true;
        for (int i = 0; i < NUM_FILES; i++) {
            FileState fi = fileStates[i];
            for (int j = i + 1; j < NUM_FILES; j++) {
                FileState fj = fileStates[j];
                disjoint = disjoint && Collections.disjoint(fi.pinnedPages, fj.pinnedPages);
            }
        }
        return disjoint;
    }

    private void createFiles() throws Exception {
        for (int i = 0; i < NUM_FILES; i++) {
            FileState f = fileStates[i];
            String fName = String.format("f%d", i);
            f.fileRef = ioManager.resolve(fName);
            vbc.createFile(f.fileRef);
            f.fileId = vbc.getFileMapProvider().lookupFileId(f.fileRef);
        }
    }

    private void deleteFiles() throws Exception {
        for (int i = 0; i < NUM_FILES; i++) {
            vbc.deleteFile(fileStates[i].fileId);
        }
    }

    private void kPins(int k) throws Exception {
        int numPinned = 0;
        while (numPinned < k) {
            int fsIdx = random.nextInt(NUM_FILES);
            FileState f = fileStates[fsIdx];
            ICachedPage p = vbc.pin(BufferedFileHandle.getDiskPageId(f.fileId, f.pinCount), true);
            f.pinnedPages.add(p);
            ++f.pinCount;
            ++numPinned;
        }
    }
}
