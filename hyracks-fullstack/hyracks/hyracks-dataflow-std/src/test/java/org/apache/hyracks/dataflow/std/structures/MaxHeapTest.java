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

package org.apache.hyracks.dataflow.std.structures;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class MaxHeapTest extends AbstracHeapTest {
    @Test
    public void testInitialMinHeap() {
        int capacity = 10;
        MaxHeap maxHeap = new MaxHeap(new IntFactory(), capacity);
        assertTrue(maxHeap.isEmpty());
        assertEquals(0, maxHeap.getNumEntries());
    }

    @Test
    public void testInsertSmallAmountElements() {
        int capacity = 10;
        MaxHeap maxHeap = new MaxHeap(new IntFactory(), capacity);
        for (int i = 0; i < capacity; i++) {
            maxHeap.insert(new Int(capacity - i));
        }
        assertEquals(capacity, maxHeap.getNumEntries());
        assertFalse(maxHeap.isEmpty());

        assertGetMaxHeapIsSorted(maxHeap);

        for (int i = 0; i < capacity; i++) {
            maxHeap.insert(new Int(random.nextInt()));
        }
        assertEquals(capacity, maxHeap.getNumEntries());
        assertFalse(maxHeap.isEmpty());
        assertGetMaxHeapIsSorted(maxHeap);
    }

    @Test
    public void testInsertLargerThanCapacityElements() {
        int capacity = 10;
        MaxHeap maxHeap = new MaxHeap(new IntFactory(), capacity);
        for (int i = 0; i < capacity; i++) {
            maxHeap.insert(new Int(capacity - i));
        }
        assertEquals(capacity, maxHeap.getNumEntries());
        assertFalse(maxHeap.isEmpty());
        assertGetMaxHeapIsSorted(maxHeap);

        for (int i = 0; i < capacity * 10; i++) {
            maxHeap.insert(new Int(random.nextInt()));
        }
        assertEquals(capacity * 10, maxHeap.getNumEntries());
        assertFalse(maxHeap.isEmpty());
        assertGetMaxHeapIsSorted(maxHeap);

    }

    @Test
    public void testReplaceMax() {
        int capacity = 10;
        MaxHeap maxHeap = new MaxHeap(new IntFactory(), capacity);
        for (int i = capacity; i < capacity * 2; i++) {
            maxHeap.insert(new Int(i));
        }
        assertEquals(capacity, maxHeap.getNumEntries());
        assertFalse(maxHeap.isEmpty());

        for (int i = 0; i < capacity; i++) {
            maxHeap.replaceMax(new Int(i));
        }
        assertEquals(capacity, maxHeap.getNumEntries());
        assertFalse(maxHeap.isEmpty());

        Int maxI = new Int();
        Int peekI = new Int();
        int i = 0;
        while (!maxHeap.isEmpty()) {
            maxHeap.peekMax(peekI);
            maxHeap.getMax(maxI);
            assertTrue(peekI.compareTo(maxI) == 0);
            assertEquals(i++, capacity - 1 - maxI.i);
        }
    }
}
