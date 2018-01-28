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

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.junit.Test;

public class VirtualFreePageManagerTest {

    private final int NUM_PAGES = 100;

    private void testInMemoryFreePageManager(VirtualFreePageManager virtualFreePageManager)
            throws HyracksDataException {
        // The first two pages are reserved for the BTree's metadata page and
        // root page.
        int capacity = NUM_PAGES - 2;
        for (int i = 0; i < capacity; i++) {
            int pageId = virtualFreePageManager.takePage(null);
            // The free pages start from page 2;
            assertEquals(i + 2, pageId);
        }
        // Start asking for 100 pages above the capacity.
        // Asking for pages above the capacity should be very rare, but
        // nevertheless succeed.
        // We expect isFull() to return true.
        for (int i = 0; i < 100; i++) {
            int pageId = virtualFreePageManager.takePage(null);
            assertEquals(capacity + i + 2, pageId);
        }
    }

    @Test
    public void test01() throws HyracksDataException {
        VirtualBufferCache bufferCache = new VirtualBufferCache(new HeapBufferAllocator(), 4096, 128);
        bufferCache.open();
        FileReference fileRef = new FileReference(new IODeviceHandle(new File("target"), "workspace"), "tempfile.tmp");
        bufferCache.createFile(fileRef);
        int fileId = bufferCache.getFileMapProvider().lookupFileId(fileRef);
        bufferCache.openFile(fileId);
        VirtualFreePageManager virtualFreePageManager = new VirtualFreePageManager(bufferCache);
        virtualFreePageManager.open(fileId);
        virtualFreePageManager.init(null, null);
        testInMemoryFreePageManager(virtualFreePageManager);
        // We expect exactly the same behavior after a reset().
        virtualFreePageManager.init(null, null);
        testInMemoryFreePageManager(virtualFreePageManager);
    }
}
