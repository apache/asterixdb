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

package edu.uci.ics.hyracks.storage.am.btree.multithread;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.test.TestOperationSelector.TestOperation;
import edu.uci.ics.hyracks.storage.am.common.test.TreeIndexMultiThreadTestDriver;

@SuppressWarnings("rawtypes")
public class BTreeMultiThreadTest {    
    
    private final int PAGE_SIZE = 8192;
    //private final int PAGE_SIZE = 64;
    private final int NUM_PAGES = 1000;
    private final int MAX_OPEN_FILES = 10;
    private final int HYRACKS_FRAME_SIZE = 32768;
    
    private final BTreeTestWorkerFactory workerFactory = new BTreeTestWorkerFactory();
    private final BTreeTestHarness harness = new BTreeTestHarness(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES, HYRACKS_FRAME_SIZE);

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }
    
    @Test
    public void firstTest() throws InterruptedException, HyracksDataException, TreeIndexException {        
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        int numKeyFields = 1;
        IBinaryComparatorFactory[] cmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeyFields);     
        
        int btreeFileId = harness.getBTreeFileId();
        BTree btree = BTreeUtils.createBTree(harness.getBufferCache(), harness.getBTreeFileId(), typeTraits, cmpFactories, BTreeLeafFrameType.REGULAR_NSM);
        btree.create(btreeFileId);
        btree.open(btreeFileId);
        
        TestOperation[] ops = new TestOperation[] { TestOperation.INSERT };
        float[] opProbs = new float[] { 1.0f };
        
        TreeIndexMultiThreadTestDriver driver = new TreeIndexMultiThreadTestDriver(btree, workerFactory, fieldSerdes, ops, opProbs);
        
        driver.run(10, 1, 10000000, 1000);
        
        btree.close();
    }
}
