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

package edu.uci.ics.hyracks.storage.am.lsm.btree;

import java.util.Random;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.storage.am.btree.OrderedIndexBulkLoadTest;
import edu.uci.ics.hyracks.storage.am.btree.OrderedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;

@SuppressWarnings("rawtypes")
public class LSMBTreeMultiBulkLoadTest extends OrderedIndexBulkLoadTest {
    public LSMBTreeMultiBulkLoadTest() {
        // Using 5 bulk load rounds.
        super(LSMBTreeTestHarness.LEAF_FRAMES_TO_TEST, AccessMethodTestsConfig.LSM_BTREE_BULKLOAD_ROUNDS);
    }

    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();

    @Before
    public void setUp() throws HyracksException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected OrderedIndexTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            BTreeLeafFrameType leafType) throws Exception {
        return LSMBTreeTestContext.create(harness.getVirtualBufferCaches(), harness.getFileReference(),
                harness.getDiskBufferCache(), harness.getDiskFileMapProvider(), fieldSerdes, numKeys,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallback());
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }
}