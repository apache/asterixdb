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

package edu.uci.ics.hyracks.storage.am.lsm.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashSet;

import org.junit.Test;

import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class InMemoryBufferCacheTest{
    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 100;
    private HashSet<ICachedPage> pinnedPages = new HashSet<ICachedPage>();
    
    @Test
    public void test01() throws Exception {
        InMemoryBufferCache memBufferCache = new InMemoryBufferCache(new HeapBufferAllocator(), PAGE_SIZE, NUM_PAGES);
        int dummyFileId = 0;
        // Pin all pages, and make sure they return unique ICachedPages.
        // We expect no overflow pages.
        for (int i = 0; i < NUM_PAGES; i++) {
            ICachedPage page = memBufferCache.pin(BufferedFileHandle.getDiskPageId(dummyFileId, i), false);
            if (pinnedPages.contains(page)) {
                fail("Id collision for ICachedPage, caused by id: " + i);
            }
            pinnedPages.add(page);
            assertEquals(0, memBufferCache.getNumOverflowPages());
        }
        // Pin pages above capacity. We expect to be given new overflow pages.
        // Going above capacity should be very rare, but nevertheless succeed.
        for (int i = 0; i < 100; i++) {
            ICachedPage page = memBufferCache.pin(BufferedFileHandle.getDiskPageId(dummyFileId, i + NUM_PAGES), false);
            if (pinnedPages.contains(page)) {
                fail("Id collision for ICachedPage, caused by overflow id: " + i);
            }
            pinnedPages.add(page);
            assertEquals(i + 1, memBufferCache.getNumOverflowPages());
        }
    }
}
