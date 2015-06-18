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

public class MinMaxHeapTest extends AbstracHeapTest {

    @Test
    public void testInitialMinMaxHeap() {
        int capacity = 10;
        MinMaxHeap minHeap = new MinMaxHeap(new IntFactory(), capacity);
        assertTrue(minHeap.isEmpty());
        assertEquals(0, minHeap.getNumEntries());
    }

    @Test
    public void testInsertElements() {
        int capacity = 10;
        MinMaxHeap minMaxHeap = new MinMaxHeap(new IntFactory(), capacity);
        for (int i = 0; i < capacity * 10; i++) {
            minMaxHeap.insert(new Int(random.nextInt()));
        }
        assertEquals(capacity * 10, minMaxHeap.getNumEntries());
        assertFalse(minMaxHeap.isEmpty());
        assertGetMinHeapIsSorted(minMaxHeap);

        for (int i = 0; i < capacity * 10; i++) {
            minMaxHeap.insert(new Int(random.nextInt()));
        }
        assertEquals(capacity * 10, minMaxHeap.getNumEntries());
        assertGetMaxHeapIsSorted(minMaxHeap);
    }

    @Test
    public void testReplaceMin() {
        int capacity = 10;
        MinMaxHeap minMaxHeap = new MinMaxHeap(new IntFactory(), capacity);
        for (int i = 0; i < capacity; i++) {
            minMaxHeap.insert(new Int(i));
        }
        assertEquals(capacity, minMaxHeap.getNumEntries());
        assertFalse(minMaxHeap.isEmpty());

        for (int i = capacity; i < capacity * 2; i++) {
            minMaxHeap.replaceMin(new Int(i));
        }
        assertEquals(capacity, minMaxHeap.getNumEntries());
        assertFalse(minMaxHeap.isEmpty());

        Int minI = new Int();
        Int peekI = new Int();
        int i = 0;
        while (!minMaxHeap.isEmpty()) {
            minMaxHeap.peekMin(peekI);
            minMaxHeap.getMin(minI);
            assertTrue(peekI.compareTo(minI) == 0);
            assertEquals(i++ + capacity, minI.i);
        }
    }

    @Test
    public void testReplaceMax() {
        int capacity = 10;
        MinMaxHeap minMaxHeap = new MinMaxHeap(new IntFactory(), capacity);
        for (int i = 0; i < capacity; i++) {
            minMaxHeap.insert(new Int(i + capacity));
        }
        assertEquals(capacity, minMaxHeap.getNumEntries());
        assertFalse(minMaxHeap.isEmpty());

        Int maxI = new Int();
        for (int i = capacity; i < capacity * 2; i++) {
            minMaxHeap.peekMax(maxI);
            minMaxHeap.replaceMax(new Int(i - capacity));
        }
        assertEquals(capacity, minMaxHeap.getNumEntries());
        assertFalse(minMaxHeap.isEmpty());

        System.out.println();
        Int peekI = new Int();
        int i = 0;
        while (!minMaxHeap.isEmpty()) {
            minMaxHeap.peekMax(peekI);
            minMaxHeap.getMax(maxI);
            assertTrue(peekI.compareTo(maxI) == 0);
            assertEquals(capacity - i - 1, maxI.i);
            i++;
        }
    }

}