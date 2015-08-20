/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;

public class VirtualFreePageManagerTest {

    private final int NUM_PAGES = 100;

    private void testInMemoryFreePageManager(VirtualFreePageManager virtualFreePageManager) throws HyracksDataException {
        // The first two pages are reserved for the BTree's metadata page and
        // root page.
        // The "actual" capacity is therefore numPages - 2.
        int capacity = virtualFreePageManager.getCapacity();
        assertEquals(capacity, NUM_PAGES - 2);
        for (int i = 0; i < capacity; i++) {
            int pageId = virtualFreePageManager.getFreePage(null);
            // The free pages start from page 2;
            assertEquals(i + 2, pageId);
        }
        // Start asking for 100 pages above the capacity.
        // Asking for pages above the capacity should be very rare, but
        // nevertheless succeed.
        // We expect isFull() to return true.
        for (int i = 0; i < 100; i++) {
            int pageId = virtualFreePageManager.getFreePage(null);
            assertEquals(capacity + i + 2, pageId);
        }
    }

    @Test
    public void test01() throws HyracksDataException {
        VirtualFreePageManager virtualFreePageManager = new VirtualFreePageManager(NUM_PAGES);
        testInMemoryFreePageManager(virtualFreePageManager);
        // We expect exactly the same behavior after a reset().
        virtualFreePageManager.reset();
        testInMemoryFreePageManager(virtualFreePageManager);
    }
}
