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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Before;
import org.junit.Test;

public class VariableFramePoolTest extends AbstractFramePoolTest {

    @Before
    public void setUp() throws Exception {
        pool = new VariableFramePool(commonFrameManager, BUDGET);
    }

    @Test
    public void testAllocateUniformFrameShouldSuccess() throws Exception {
        testAllocateAllSpacesWithMinFrames();
        testAllocateShouldFailAfterAllSpaceGetUsed();
        pool.reset();
        testAllocateAllSpacesWithMinFrames();
        pool.close();
    }

    @Test
    public void testResetShouldReuseExistingFrames() throws HyracksDataException {
        Set<?> set1 = testAllocateAllSpacesWithMinFrames();
        pool.reset();
        Set<?> set2 = testAllocateAllSpacesWithMinFrames();
        assertEquals(set1, set2);
        pool.close();
    }

    @Test
    public void testCloseShouldNotReuseExistingFrames() throws HyracksDataException {
        Set<?> set1 = testAllocateAllSpacesWithMinFrames();
        pool.close();
        Set<?> set2 = testAllocateAllSpacesWithMinFrames();
        assertFalse(set1.equals(set2));
        pool.close();
    }

    @Test
    public void testShouldReturnLargerFramesIfFitOneIsUsed() throws HyracksDataException {
        Set<?> set = testAllocateVariableFrames();
        pool.reset();
        testShouldFindTheMatchFrames(set);
        pool.reset();

        // allocate seq: 1, 1, 2, 3, 4
        ByteBuffer placeBuffer = pool.allocateFrame(MIN_FRAME_SIZE);
        assertTrue(set.contains(new ByteBufferPtr(placeBuffer)));
        for (int i = 1; i <= 4; i++) {
            ByteBuffer buffer = pool.allocateFrame(i * MIN_FRAME_SIZE);
            assertNotNull(buffer);
            assertTrue(set.contains(new ByteBufferPtr(buffer)));
        }
        assertNull(pool.allocateFrame(MIN_FRAME_SIZE));
        pool.close();
    }

    @Test
    public void testShouldMergeIfNoLargerFrames() throws HyracksDataException {
        Set<?> set = testAllocateAllSpacesWithMinFrames();
        pool.reset();
        int chunks = 5;
        for (int i = 0; i < NUM_MIN_FRAME; i += chunks) {
            ByteBuffer buffer = pool.allocateFrame(chunks * MIN_FRAME_SIZE);
            assertNotNull(buffer);
            assertTrue(!set.contains(new ByteBufferPtr(buffer)));
        }
    }

    @Test
    public void testUseMiddleSizeFrameAndNeedToMergeSmallAndBigger() throws HyracksDataException {
        Set<?> set = testAllocateVariableFrames();
        pool.reset();
        // allocate seq: 3, 6, 1;
        ByteBuffer buffer = pool.allocateFrame(3 * MIN_FRAME_SIZE);
        assertTrue(set.contains(new ByteBufferPtr(buffer)));
        buffer = pool.allocateFrame(6 * MIN_FRAME_SIZE);
        assertFalse(set.contains(new ByteBufferPtr(buffer)));
        buffer = pool.allocateFrame(1 * MIN_FRAME_SIZE);
        assertTrue(set.contains(new ByteBufferPtr(buffer)));
        assertEquals(5 * MIN_FRAME_SIZE, buffer.capacity());
        pool.reset();
    }
}
