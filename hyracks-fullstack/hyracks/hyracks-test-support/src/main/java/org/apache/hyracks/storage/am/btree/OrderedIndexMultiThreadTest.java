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

import java.util.ArrayList;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.storage.am.common.IIndexTestWorkerFactory;
import org.apache.hyracks.storage.am.common.IndexMultiThreadTestDriver;
import org.apache.hyracks.storage.am.common.TestWorkloadConf;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public abstract class OrderedIndexMultiThreadTest {

    protected final Logger LOGGER = LogManager.getLogger();

    // Machine-specific number of threads to use for testing.
    protected final int REGULAR_NUM_THREADS = Runtime.getRuntime().availableProcessors();
    // Excessive number of threads for testing.
    protected final int EXCESSIVE_NUM_THREADS = Runtime.getRuntime().availableProcessors() * 4;
    protected final int NUM_OPERATIONS = AccessMethodTestsConfig.BTREE_MULTITHREAD_NUM_OPERATIONS;

    protected ArrayList<TestWorkloadConf> workloadConfs = getTestWorkloadConf();

    protected abstract void setUp() throws HyracksDataException;

    protected abstract void tearDown() throws HyracksDataException;

    protected abstract IIndex createIndex(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields) throws HyracksDataException;

    protected abstract IIndexTestWorkerFactory getWorkerFactory();

    protected abstract ArrayList<TestWorkloadConf> getTestWorkloadConf();

    protected abstract String getIndexTypeName();

    protected void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, int numThreads, TestWorkloadConf conf,
            String dataMsg) throws InterruptedException, HyracksDataException {
        setUp();

        if (LOGGER.isInfoEnabled()) {
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
        IndexMultiThreadTestDriver driver =
                new IndexMultiThreadTestDriver(index, workerFactory, fieldSerdes, conf.ops, conf.opProbs);
        driver.init();
        long[] times = driver.run(numThreads, 1, NUM_OPERATIONS, batchSize);
        index.validate();
        driver.deinit();

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("BTree MultiThread Test Time: " + times[0] + "ms");
        }

        tearDown();
    }

    @Test
    public void oneIntKeyAndValue() throws InterruptedException, HyracksDataException {
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
    public void oneStringKeyAndValue() throws InterruptedException, HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer() };
        int numKeys = 1;
        String dataMsg = "One String Key And Value";

        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, numKeys, REGULAR_NUM_THREADS, conf, dataMsg);
            runTest(fieldSerdes, numKeys, EXCESSIVE_NUM_THREADS, conf, dataMsg);
        }
    }
}
