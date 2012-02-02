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

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.test.TestOperationSelector.TestOperation;
import edu.uci.ics.hyracks.storage.am.common.test.TestWorkloadConf;
import edu.uci.ics.hyracks.storage.am.common.test.TreeIndexMultiThreadTestDriver;

@SuppressWarnings("rawtypes")
public class BTreeMultiThreadTest {    
    
    protected final Logger LOGGER = Logger.getLogger(BTreeMultiThreadTest.class.getName());
    
    private final BTreeTestWorkerFactory workerFactory = new BTreeTestWorkerFactory();
    private final BTreeTestHarness harness = new BTreeTestHarness();

    // Machine-specific number of threads to use for testing.
    private final int REGULAR_NUM_THREADS = Runtime.getRuntime().availableProcessors();
    // Excessive number of threads for testing.
    private final int EXCESSIVE_NUM_THREADS = Runtime.getRuntime().availableProcessors() * 4;
    private final int NUM_OPERATIONS = 20000;
    
    private ArrayList<TestWorkloadConf> workloadConfs = getTestWorkloadConf();

    private static ArrayList<TestWorkloadConf> getTestWorkloadConf() {
        ArrayList<TestWorkloadConf> workloadConfs = new ArrayList<TestWorkloadConf>();
        
        // Insert only workload.
        TestOperation[] insertOnlyOps = new TestOperation[] { TestOperation.INSERT };
        workloadConfs.add(new TestWorkloadConf(insertOnlyOps, getUniformOpProbs(insertOnlyOps)));
        
        // Inserts mixed with point searches and scans.
        TestOperation[] insertSearchOnlyOps = new TestOperation[] { TestOperation.INSERT, TestOperation.POINT_SEARCH, TestOperation.ORDERED_SCAN, TestOperation.DISKORDER_SCAN };
        workloadConfs.add(new TestWorkloadConf(insertSearchOnlyOps, getUniformOpProbs(insertSearchOnlyOps)));
        
        // Inserts, updates, and deletes.        
        TestOperation[] insertDeleteUpdateOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE, TestOperation.UPDATE };
        workloadConfs.add(new TestWorkloadConf(insertDeleteUpdateOps, getUniformOpProbs(insertDeleteUpdateOps)));
        
        // All operations mixed.
        TestOperation[] allOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE, TestOperation.UPDATE, TestOperation.POINT_SEARCH, TestOperation.ORDERED_SCAN, TestOperation.DISKORDER_SCAN };
        workloadConfs.add(new TestWorkloadConf(allOps, getUniformOpProbs(allOps)));
        
        return workloadConfs;
    }
    
    private static float[] getUniformOpProbs(TestOperation[] ops) {
        float[] opProbs = new float[ops.length];
        for (int i = 0; i < ops.length; i++) {
            opProbs[i] = 1.0f / (float) ops.length;
        }
        return opProbs;
    }
    
    private void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, int numThreads, TestWorkloadConf conf, String dataMsg) throws HyracksDataException, InterruptedException, TreeIndexException {
        harness.setUp();
        
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("BTree MultiThread Test:\nData: " + dataMsg + "; Threads: " + numThreads + "; Workload: " + conf.toString() + ".");
        }
        
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] cmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeys);     
        
        BTree btree = BTreeUtils.createBTree(harness.getBufferCache(), harness.getBTreeFileId(), typeTraits, cmpFactories, BTreeLeafFrameType.REGULAR_NSM);
        
        // 4 batches per thread.
        int batchSize = (NUM_OPERATIONS / numThreads) / 4;
        
        TreeIndexMultiThreadTestDriver driver = new TreeIndexMultiThreadTestDriver(btree, workerFactory, fieldSerdes, conf.ops, conf.opProbs);
        driver.init(harness.getBTreeFileId());
        long[] times = driver.run(numThreads, 1, NUM_OPERATIONS, batchSize);
        driver.deinit();
        
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("BTree MultiThread Test Time: " + times[0] + "ms");
        }
        
        harness.tearDown();
    }
    
    @Test
    public void oneIntKeyAndValueRegular() throws InterruptedException, HyracksDataException, TreeIndexException {        
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        int numKeys = 1;
        String dataMsg = "One Int Key And Value";
        
        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, numKeys, REGULAR_NUM_THREADS, conf, dataMsg);
            runTest(fieldSerdes, numKeys, EXCESSIVE_NUM_THREADS, conf, dataMsg);
        }
    }
    
    @Test
    public void oneStringKeyAndValueRegular() throws InterruptedException, HyracksDataException, TreeIndexException {        
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
        int numKeys = 1;
        String dataMsg = "One String Key And Value";
        
        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, numKeys, REGULAR_NUM_THREADS, conf, dataMsg);
            runTest(fieldSerdes, numKeys, EXCESSIVE_NUM_THREADS, conf, dataMsg);
        }
    }
    
}
