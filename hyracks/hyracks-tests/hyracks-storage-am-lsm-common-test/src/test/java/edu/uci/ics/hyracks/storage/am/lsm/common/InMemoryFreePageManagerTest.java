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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;

public class InMemoryFreePageManagerTest {

    private final int NUM_PAGES = 100;
    
    private void testInMemoryFreePageManager(InMemoryFreePageManager memFreePageManager) throws HyracksDataException {
        // The first two pages are reserved for the BTree's metadata page and
        // root page.
        // The "actual" capacity is therefore numPages - 2.
        int capacity = memFreePageManager.getCapacity();
        assertEquals(capacity, NUM_PAGES - 2);
        for (int i = 0; i < capacity; i++) {
            int pageId = memFreePageManager.getFreePage(null);
            // The free pages start from page 2;
            assertEquals(i + 2, pageId);
            assertFalse(memFreePageManager.isFull());
        }
        // Start asking for 100 pages above the capacity.
        // Asking for pages above the capacity should be very rare, but
        // nevertheless succeed.
        // We expect isFull() to return true.
        for (int i = 0; i < 100; i++) {
            int pageId = memFreePageManager.getFreePage(null);
            assertEquals(capacity + i + 2, pageId);
            assertTrue(memFreePageManager.isFull());
        }
    }
    
    @Test
    public void test01() throws HyracksDataException {
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        InMemoryFreePageManager memFreePageManager = new InMemoryFreePageManager(NUM_PAGES, metaFrameFactory);
        testInMemoryFreePageManager(memFreePageManager);
        // We expect exactly the same behavior after a reset().
        memFreePageManager.reset();
        testInMemoryFreePageManager(memFreePageManager);
    }
}
