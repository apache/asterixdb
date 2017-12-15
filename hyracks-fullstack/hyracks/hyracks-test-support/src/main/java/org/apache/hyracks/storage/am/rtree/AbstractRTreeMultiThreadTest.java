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

package org.apache.hyracks.storage.am.rtree;

import java.util.ArrayList;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.storage.am.common.IIndexTestWorkerFactory;
import org.apache.hyracks.storage.am.common.IndexMultiThreadTestDriver;
import org.apache.hyracks.storage.am.common.TestWorkloadConf;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.am.rtree.AbstractRTreeExamplesTest.RTreeType;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.am.rtree.util.RTreeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public abstract class AbstractRTreeMultiThreadTest {

    protected final boolean testRstarPolicy;
    protected final RTreeType rTreeType;

    public AbstractRTreeMultiThreadTest(boolean testRstarPolicy, RTreeType rTreeType) {
        this.testRstarPolicy = testRstarPolicy;
        this.rTreeType = rTreeType;
    }

    protected final Logger LOGGER = LogManager.getLogger();

    // Machine-specific number of threads to use for testing.
    protected final int REGULAR_NUM_THREADS = Runtime.getRuntime().availableProcessors();
    // Excessive number of threads for testing.
    protected final int EXCESSIVE_NUM_THREADS = Runtime.getRuntime().availableProcessors() * 4;
    protected final int NUM_OPERATIONS = AccessMethodTestsConfig.RTREE_MULTITHREAD_NUM_OPERATIONS;

    protected ArrayList<TestWorkloadConf> workloadConfs = getTestWorkloadConf();

    protected abstract void setUp() throws HyracksDataException;

    protected abstract void tearDown() throws HyracksDataException;

    protected abstract ITreeIndex createTreeIndex(ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType, int[] btreeFields)
            throws HyracksDataException;

    protected abstract IIndexTestWorkerFactory getWorkerFactory();

    protected abstract ArrayList<TestWorkloadConf> getTestWorkloadConf();

    protected abstract String getIndexTypeName();

    protected void runTest(ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeys, RTreePolicyType rtreePolicyType,
            int numThreads, TestWorkloadConf conf, String dataMsg) throws HyracksDataException, InterruptedException {
        setUp();

        if (LOGGER.isInfoEnabled()) {
            String indexTypeName = getIndexTypeName();
            LOGGER.info(indexTypeName + " MultiThread Test:\nData: " + dataMsg + "; Threads: " + numThreads
                    + "; Workload: " + conf.toString() + ".");
        }

        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] rtreeCmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeys);
        int[] btreeFields = null;
        IBinaryComparatorFactory[] btreeCmpFactories;
        if (rTreeType == RTreeType.LSMRTREE) {
            int numBtreeFields = fieldSerdes.length - numKeys;
            ISerializerDeserializer[] btreeFieldSerdes = new ISerializerDeserializer[numBtreeFields];
            btreeFields = new int[numBtreeFields];
            for (int i = 0; i < numBtreeFields; i++) {
                btreeFields[i] = numKeys + i;
                btreeFieldSerdes[i] = fieldSerdes[numKeys + i];
            }
            btreeCmpFactories = SerdeUtils.serdesToComparatorFactories(btreeFieldSerdes, numBtreeFields);
        } else {
            btreeCmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, fieldSerdes.length);
        }

        ITreeIndex index = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories,
                rtreePolicyType, btreeFields);
        IIndexTestWorkerFactory workerFactory = getWorkerFactory();

        // 4 batches per thread.
        int batchSize = (NUM_OPERATIONS / numThreads) / 4;

        IndexMultiThreadTestDriver driver =
                new IndexMultiThreadTestDriver(index, workerFactory, fieldSerdes, conf.ops, conf.opProbs);
        driver.init();
        long[] times = driver.run(numThreads, 1, NUM_OPERATIONS, batchSize);
        driver.deinit();

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("RTree MultiThread Test Time: " + times[0] + "ms");
        }

        tearDown();
    }

    @Test
    public void rtreeTwoDimensionsInt() throws InterruptedException, HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        int numKeys = 4;
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(numKeys, IntegerPointable.FACTORY);

        String dataMsg = "Two Dimensions Of Integer Values";

        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RTREE, REGULAR_NUM_THREADS, conf,
                    dataMsg);
            runTest(fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RTREE, EXCESSIVE_NUM_THREADS, conf,
                    dataMsg);
        }
    }

    @Test
    public void rtreeTwoDimensionsDouble() throws Exception {
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };

        int numKeys = 4;
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(numKeys, DoublePointable.FACTORY);

        String dataMsg = "Two Dimensions Of Double Values";

        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RTREE, REGULAR_NUM_THREADS, conf,
                    dataMsg);
            runTest(fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RTREE, EXCESSIVE_NUM_THREADS, conf,
                    dataMsg);
        }

    }

    @Test
    public void rtreeFourDimensionsDouble() throws InterruptedException, HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };

        int numKeys = 8;
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(numKeys, DoublePointable.FACTORY);

        String dataMsg = "Four Dimensions Of Double Values";

        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RTREE, REGULAR_NUM_THREADS, conf,
                    dataMsg);
            runTest(fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RTREE, EXCESSIVE_NUM_THREADS, conf,
                    dataMsg);
        }
    }

    @Test
    public void rstartreeTwoDimensionsInt() throws InterruptedException, HyracksDataException {
        if (!testRstarPolicy) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Ignoring RTree Multithread Test With Two Dimensions With Integer Keys.");
            }
            return;
        }

        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        int numKeys = 4;
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(numKeys, IntegerPointable.FACTORY);

        String dataMsg = "Two Dimensions Of Integer Values";

        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RSTARTREE, REGULAR_NUM_THREADS, conf,
                    dataMsg);
            runTest(fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RSTARTREE, EXCESSIVE_NUM_THREADS,
                    conf, dataMsg);
        }
    }

    @Test
    public void rstartreeTwoDimensionsDouble() throws Exception {
        if (!testRstarPolicy) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Ignoring RTree Multithread Test With Two Dimensions With Double Keys.");
            }
            return;
        }

        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };

        int numKeys = 4;
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(numKeys, DoublePointable.FACTORY);

        String dataMsg = "Two Dimensions Of Double Values";

        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RSTARTREE, REGULAR_NUM_THREADS, conf,
                    dataMsg);
            runTest(fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RSTARTREE, EXCESSIVE_NUM_THREADS,
                    conf, dataMsg);
        }

    }

    @Test
    public void rstartreeFourDimensionsDouble() throws InterruptedException, HyracksDataException {
        if (!testRstarPolicy) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Ignoring RTree Multithread Test With Four Dimensions With Double Keys.");
            }
            return;
        }

        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };

        int numKeys = 8;
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(numKeys, DoublePointable.FACTORY);

        String dataMsg = "Four Dimensions Of Double Values";

        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RSTARTREE, REGULAR_NUM_THREADS, conf,
                    dataMsg);
            runTest(fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RSTARTREE, EXCESSIVE_NUM_THREADS,
                    conf, dataMsg);
        }
    }

}
