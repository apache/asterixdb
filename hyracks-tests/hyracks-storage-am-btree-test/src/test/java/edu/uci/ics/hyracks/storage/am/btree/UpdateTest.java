/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.btree;

import java.util.Random;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.tests.IOrderedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.btree.tests.OrderedIndexUpdateTest;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestContext;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestHarness;

@SuppressWarnings("rawtypes")
public class UpdateTest extends OrderedIndexUpdateTest {
    
    public UpdateTest() {
        super(BTreeTestHarness.LEAF_FRAMES_TO_TEST);
    }

    private final BTreeTestHarness harness = new BTreeTestHarness();

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected IOrderedIndexTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys, BTreeLeafFrameType leafType) throws Exception {
        return BTreeTestContext.create(harness.getBufferCache(),
                harness.getBTreeFileId(), fieldSerdes, numKeys, leafType);
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }
}
