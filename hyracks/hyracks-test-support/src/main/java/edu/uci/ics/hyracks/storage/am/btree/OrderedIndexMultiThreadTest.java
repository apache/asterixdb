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

package edu.uci.ics.hyracks.storage.am.btree;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.common.IIndexTestWorkerFactory;
import edu.uci.ics.hyracks.storage.am.common.IndexMultiThreadTestDriver;
import edu.uci.ics.hyracks.storage.am.common.TestWorkloadConf;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;

@SuppressWarnings("rawtypes")
public abstract class OrderedIndexMultiThreadTest {

    protected final Logger LOGGER = Logger.getLogger(OrderedIndexMultiThreadTest.class.getName());

    // Machine-specific number of threads to use for testing.
    protected final int REGULAR_NUM_THREADS = Runtime.getRuntime().availableProcessors();
    // Excessive number of threads for testing.
    protected final int EXCESSIVE_NUM_THREADS = Runtime.getRuntime().availableProcessors() * 4;
    protected final int NUM_OPERATIONS = AccessMethodTestsConfig.BTREE_MULTITHREAD_NUM_OPERATIONS;

    protected ArrayList<TestWorkloadConf> workloadConfs = getTestWorkloadConf();

    protected abstract void setUp() throws HyracksException;

    protected abstract void tearDown() throws HyracksDataException;

    protected abstract IIndex createIndex(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields) throws TreeIndexException;

    protected abstract IIndexTestWorkerFactory getWorkerFactory();

    protected abstract ArrayList<TestWorkloadConf> getTestWorkloadConf();

    protected abstract String getIndexTypeName();

    protected void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, int numThreads, TestWorkloadConf conf,
            String dataMsg) throws InterruptedException, TreeIndexException, HyracksException {
        setUp();

        if (LOGGER.isLoggable(Level.INFO)) {
            String indexTypeName = getIndexTypeName();
            LOGGER.info(indexTypeName + " MultiThread Test:\nData: " + dataMsg + "; Threads: " + numThreads
                    + "; Workload: " + conf.toString() + ".");
        }

        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] cmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeys);

        // This is only used for the LSM-BTree.
        int[] bloomFilterKeyFields = new int[numKeys];
        for (int i = 0; i < numKeys; ++i) {
            bloomFilterKeyFields[i] = i;
        }

        IIndex index = createIndex(typeTraits, cmpFactories, bloomFilterKeyFields);
        IIndexTestWorkerFactory workerFactory = getWorkerFactory();

        // 4 batches per thread.
        int batchSize = (NUM_OPERATIONS / numThreads) / 4;

        IndexMultiThreadTestDriver driver = new IndexMultiThreadTestDriver(index, workerFactory, fieldSerdes, conf.ops,
                conf.opProbs);
        driver.init();
        long[] times = driver.run(numThreads, 1, NUM_OPERATIONS, batchSize);
        index.validate();
        driver.deinit();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("BTree MultiThread Test Time: " + times[0] + "ms");
        }

        tearDown();
    }

    @Test
    public void oneIntKeyAndValue() throws InterruptedException, TreeIndexException, HyracksException {
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };
        int numKeys = 1;
        String dataMsg = "One Int Key And Value";

        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, numKeys, REGULAR_NUM_THREADS, conf, dataMsg);
            runTest(fieldSerdes, numKeys, EXCESSIVE_NUM_THREADS, conf, dataMsg);
        }
    }

    @Test
    public void oneStringKeyAndValue() throws InterruptedException, TreeIndexException, HyracksException {
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
        int numKeys = 1;
        String dataMsg = "One String Key And Value";

        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, numKeys, REGULAR_NUM_THREADS, conf, dataMsg);
            runTest(fieldSerdes, numKeys, EXCESSIVE_NUM_THREADS, conf, dataMsg);
        }
    }
}
