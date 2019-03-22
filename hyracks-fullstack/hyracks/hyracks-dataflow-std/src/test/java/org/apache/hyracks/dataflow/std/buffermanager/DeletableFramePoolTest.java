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

package org.apache.hyracks.dataflow.std.buffermanager;

import static org.apache.hyracks.dataflow.std.buffermanager.Common.BUDGET;
import static org.apache.hyracks.dataflow.std.buffermanager.Common.MIN_FRAME_SIZE;
import static org.apache.hyracks.dataflow.std.buffermanager.Common.commonFrameManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Before;
import org.junit.Test;

public class DeletableFramePoolTest extends AbstractFramePoolTest {

    @Before
    public void setUp() {
        pool = new DeallocatableFramePool(commonFrameManager, BUDGET);
    }

    DeallocatableFramePool getPool() {
        return (DeallocatableFramePool) pool;
    }

    @Test
    public void testAllocateBuffers() throws HyracksDataException {
        testAllocateAllSpacesWithMinFrames();
    }

    @Test
    public void testCanNotAllocateMore() throws HyracksDataException {
        testAllocateAllSpacesWithMinFrames();
        assertNull(pool.allocateFrame(MIN_FRAME_SIZE));
    }

    @Test
    public void testReusePreAllocatedBuffer() throws HyracksDataException {
        HashSet<ByteBufferPtr> set = testAllocateAllSpacesWithMinFrames();
        for (ByteBufferPtr ptr : set) {
            getPool().deAllocateBuffer(ptr.bytebuffer);
        }
        HashSet<ByteBufferPtr> set2 = testAllocateAllSpacesWithMinFrames();
        assertEquals(set, set2);
    }

    @Test
    public void testMergeCase() throws HyracksDataException {
        HashSet<ByteBufferPtr> set = testAllocateAllSpacesWithMinFrames();
        for (ByteBufferPtr ptr : set) {
            getPool().deAllocateBuffer(ptr.bytebuffer);
        }
        set.clear();
        int i = 1;
        for (int sum = 0; sum + MIN_FRAME_SIZE * i <= BUDGET; i++) {
            sum += MIN_FRAME_SIZE * i;
            testAllocateNewBuffer(set, MIN_FRAME_SIZE * i);
        }
        assertNull(pool.allocateFrame(MIN_FRAME_SIZE * i));
        for (ByteBufferPtr ptr : set) {
            getPool().deAllocateBuffer(ptr.bytebuffer);
        }
        set.clear();
        testAllocateNewBuffer(set, BUDGET);
    }

}
