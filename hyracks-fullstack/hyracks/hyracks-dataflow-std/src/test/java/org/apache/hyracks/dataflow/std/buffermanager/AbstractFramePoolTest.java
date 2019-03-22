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
import static org.apache.hyracks.dataflow.std.buffermanager.Common.NUM_MIN_FRAME;
import static org.apache.hyracks.dataflow.std.buffermanager.Common.commonFrameManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Test;

public abstract class AbstractFramePoolTest {
    IFramePool pool;

    @Test
    public void testGetMinFrameSize() throws Exception {
        assertEquals(MIN_FRAME_SIZE, commonFrameManager.getInitialFrameSize());
        assertEquals(MIN_FRAME_SIZE, pool.getMinFrameSize());
    }

    @Test
    public void testGetMemoryBudgetBytes() throws Exception {
        assertEquals(BUDGET, pool.getMemoryBudgetBytes());
    }

    protected void testAllocateShouldFailAfterAllSpaceGetUsed() throws HyracksDataException {
        for (int i = 0; i < NUM_MIN_FRAME; i++) {
            assertNull(pool.allocateFrame(MIN_FRAME_SIZE));
        }
    }

    protected HashSet<ByteBufferPtr> testAllocateAllSpacesWithMinFrames() throws HyracksDataException {
        HashSet<ByteBufferPtr> set = new HashSet<>();
        for (int i = 0; i < NUM_MIN_FRAME; i++) {
            testAllocateNewBuffer(set, MIN_FRAME_SIZE);
        }
        return set;
    }

    protected void testAllocateNewBuffer(HashSet<ByteBufferPtr> set, int frameSize) throws HyracksDataException {
        ByteBuffer buffer = pool.allocateFrame(frameSize);
        assertNotNull(buffer);
        assertEquals(buffer.capacity(), frameSize);
        assertTrue(!set.contains(new ByteBufferPtr(buffer)));
        set.add(new ByteBufferPtr(buffer));
    }

    /**
     * Pool will become 1,2,3,4,5
     *
     * @throws HyracksDataException
     */
    protected Set<ByteBufferPtr> testAllocateVariableFrames() throws HyracksDataException {
        int budget = BUDGET;
        int allocate = 0;
        int i = 1;
        Set<ByteBufferPtr> set = new HashSet<>();
        while (budget - allocate >= i * MIN_FRAME_SIZE) {
            ByteBuffer buffer = pool.allocateFrame(i * MIN_FRAME_SIZE);
            assertNotNull(buffer);
            set.add(new ByteBufferPtr(buffer));
            allocate += i++ * MIN_FRAME_SIZE;
        }
        return set;
    }

    protected void testShouldFindTheMatchFrames(Set<?> set) throws HyracksDataException {
        pool.reset();
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        for (int i = 0; i < list.size(); i++) {
            ByteBuffer buffer = pool.allocateFrame(list.get(i) * MIN_FRAME_SIZE);
            assertNotNull(buffer);
            assertTrue(set.contains(new ByteBufferPtr(buffer)));
            assertEquals(list.get(i) * MIN_FRAME_SIZE, buffer.capacity());
        }
        pool.reset();
        for (int i = list.size() - 1; i >= 0; i--) {
            ByteBuffer buffer = pool.allocateFrame(list.get(i) * MIN_FRAME_SIZE);
            assertNotNull(buffer);
            assertTrue(set.contains(new ByteBufferPtr(buffer)));
            assertEquals(list.get(i) * MIN_FRAME_SIZE, buffer.capacity());
        }

        Collections.shuffle(list);
        pool.reset();
        for (int i = 0; i < list.size(); i++) {
            ByteBuffer buffer = pool.allocateFrame(list.get(i) * MIN_FRAME_SIZE);
            assertNotNull(buffer);
            assertTrue(set.contains(new ByteBufferPtr(buffer)));
            assertEquals(list.get(i) * MIN_FRAME_SIZE, buffer.capacity());
        }

    }

    public static class ByteBufferPtr {
        ByteBuffer bytebuffer;

        public ByteBufferPtr(ByteBuffer buffer) {
            bytebuffer = buffer;
        }

        @Override
        public int hashCode() {
            return bytebuffer.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return this.bytebuffer == ((ByteBufferPtr) obj).bytebuffer;
        }
    }

}
