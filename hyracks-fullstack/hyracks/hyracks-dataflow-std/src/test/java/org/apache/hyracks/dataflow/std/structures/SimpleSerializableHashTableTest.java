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

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.junit.Before;
import org.junit.Test;

public class SimpleSerializableHashTableTest {

    SimpleSerializableHashTable nsTable;
    final int NUM_PART = 101;
    TuplePointer pointer = new TuplePointer(0, 0);
    final int num = 10000;
    private IHyracksFrameMgrContext ctx;

    @Before
    public void setup() throws HyracksDataException {
        ctx = new FrameManager(256);
        nsTable = new SimpleSerializableHashTable(NUM_PART, ctx);
    }

    @Test
    public void testBatchDeletePartition() throws Exception {
        testInsert();
        for (int i = 0; i < NUM_PART; i++) {
            nsTable.delete(i);
            assertFalse(nsTable.getTuplePointer(i, 0, pointer));
            assertEquals(0, nsTable.getTupleCount(i));

            for (int j = i; j < num; j += NUM_PART) {
                pointer.reset(j, j);
                nsTable.insert(i, pointer);
            }

            assertGetValue();
        }
    }

    @Test
    public void testInsert() throws Exception {
        for (int i = 0; i < num; i++) {
            pointer.reset(i, i);
            nsTable.insert(i % NUM_PART, pointer);
        }
        assertGetValue();
    }

    private void assertGetValue() {
        int loop = 0;
        for (int i = 0; i < num; i++) {
            assertTrue(nsTable.getTuplePointer(i % NUM_PART, loop, pointer));
            assertTrue(pointer.getFrameIndex() == i);
            if (i % NUM_PART == NUM_PART - 1) {
                loop++;
            }
        }
        int tupleCntPerPart = (int) Math.ceil((double) num / NUM_PART);
        for (int i = 0; i < NUM_PART; i++) {
            assertTrue(nsTable.getTupleCount(i) == tupleCntPerPart || nsTable.getTupleCount(i) == tupleCntPerPart - 1);
        }

    }

    @Test
    public void testGetCount() throws Exception {
        assertAllPartitionsCountIsZero();
    }

    private void assertAllPartitionsCountIsZero() {
        for (int i = 0; i < NUM_PART; i++) {
            assertEquals(0, nsTable.getTupleCount(i));
        }
    }
}