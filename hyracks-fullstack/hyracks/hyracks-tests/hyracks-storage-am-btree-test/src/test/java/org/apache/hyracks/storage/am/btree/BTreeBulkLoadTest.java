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

package org.apache.hyracks.storage.am.btree;

import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.util.BTreeTestContext;
import org.apache.hyracks.storage.am.btree.util.BTreeTestHarness;
import org.junit.After;
import org.junit.Before;

public class BTreeBulkLoadTest extends OrderedIndexBulkLoadTest {

    private final BTreeTestHarness harness = new BTreeTestHarness();

    public BTreeBulkLoadTest() {
        super(BTreeTestHarness.LEAF_FRAMES_TO_TEST, 1);
    }

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected OrderedIndexTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            BTreeLeafFrameType leafType, boolean filtered) throws Exception {
        return BTreeTestContext.create(harness.getBufferCache(), harness.getFileReference(), fieldSerdes, numKeys,
                leafType, harness.getPageManagerFactory().createPageManager(harness.getBufferCache()));
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }
}
