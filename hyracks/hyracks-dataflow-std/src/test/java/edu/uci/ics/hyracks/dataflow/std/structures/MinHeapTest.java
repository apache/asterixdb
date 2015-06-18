/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.dataflow.std.structures;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class MinHeapTest extends AbstracHeapTest{

    @Test
    public void testInitialMinHeap() {
        int capacity = 10;
        MinHeap minHeap = new MinHeap(new IntFactory(), capacity);
        assertTrue(minHeap.isEmpty());
        assertEquals(0, minHeap.getNumEntries());
    }

    @Test
    public void testInsertSmallAmountElements() {
        int capacity = 10;
        MinHeap minHeap = new MinHeap(new IntFactory(), capacity);
        for (int i = 0; i < capacity; i++) {
            minHeap.insert(new Int(capacity - i));
        }
        assertEquals(capacity, minHeap.getNumEntries());
        assertFalse(minHeap.isEmpty());

        assertGetMinHeapIsSorted(minHeap);

        for (int i = 0; i < capacity; i++) {
            minHeap.insert(new Int(random.nextInt()));
        }
        assertEquals(capacity, minHeap.getNumEntries());
        assertFalse(minHeap.isEmpty());
        assertGetMinHeapIsSorted(minHeap);
    }

    @Test
    public void testInsertLargerThanCapacityElements() {
        int capacity = 10;
        MinHeap minHeap = new MinHeap(new IntFactory(), capacity);
        for (int i = 0; i < capacity; i++) {
            minHeap.insert(new Int(capacity - i));
        }
        assertEquals(capacity, minHeap.getNumEntries());
        assertFalse(minHeap.isEmpty());
        assertGetMinHeapIsSorted(minHeap);

        for (int i = 0; i < capacity * 10; i++) {
            minHeap.insert(new Int(random.nextInt()));
        }
        assertEquals(capacity * 10, minHeap.getNumEntries());
        assertFalse(minHeap.isEmpty());
        assertGetMinHeapIsSorted(minHeap);

    }

    @Test
    public void testReplaceMin() {
        int capacity = 10;
        MinHeap minHeap = new MinHeap(new IntFactory(), capacity);
        for (int i = 0; i < capacity; i++) {
            minHeap.insert(new Int(i));
        }
        assertEquals(capacity, minHeap.getNumEntries());
        assertFalse(minHeap.isEmpty());

        for (int i = capacity; i < capacity * 2; i++) {
            minHeap.replaceMin(new Int(i));
        }
        assertEquals(capacity, minHeap.getNumEntries());
        assertFalse(minHeap.isEmpty());

        Int minI = new Int();
        Int peekI = new Int();
        int i = 0;
        while (!minHeap.isEmpty()) {
            minHeap.peekMin(peekI);
            minHeap.getMin(minI);
            assertTrue(peekI.compareTo(minI) == 0);
            assertEquals(i++ + capacity, minI.i);
        }
    }


}