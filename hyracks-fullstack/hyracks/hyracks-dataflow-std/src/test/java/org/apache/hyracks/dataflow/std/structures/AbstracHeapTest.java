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
import static org.junit.Assert.assertTrue;

import java.util.Random;

public class AbstracHeapTest {
    Random random = new Random(System.currentTimeMillis());

    class IntFactory implements IResetableComparableFactory<Int> {
        @Override
        public IResetableComparable<Int> createResetableComparable() {
            return new Int();
        }
    }

    class Int implements IResetableComparable<Int> {
        int i;

        public Int() {
            i = 0;
        }

        public Int(int i) {
            this.i = i;
        }

        @Override
        public void reset(Int other) {
            i = other.i;
        }

        @Override
        public int compareTo(Int o) {
            return Integer.compare(i, o.i);
        }
    }

    protected void assertGetMinHeapIsSorted(IMinHeap minHeap) {
        int count = minHeap.getNumEntries();
        Int minI = new Int();
        Int peekI = new Int();
        int preI = Integer.MIN_VALUE;
        while (!minHeap.isEmpty()) {
            count--;
            minHeap.peekMin(peekI);
            minHeap.getMin(minI);
            assertTrue(peekI.compareTo(minI) == 0);
            assertTrue(preI <= minI.i);
            preI = minI.i;
        }
        assertEquals(0, count);
    }

    protected void assertGetMaxHeapIsSorted(IMaxHeap maxHeap) {
        int count = maxHeap.getNumEntries();
        Int maxI = new Int();
        Int peekI = new Int();
        int preI = Integer.MAX_VALUE;
        while (!maxHeap.isEmpty()) {
            count--;
            maxHeap.peekMax(peekI);
            maxHeap.getMax(maxI);
            assertTrue(peekI.compareTo(maxI) == 0);
            assertTrue(preI >= maxI.i);
            preI = maxI.i;
        }
        assertEquals(0, count);
    }
}
