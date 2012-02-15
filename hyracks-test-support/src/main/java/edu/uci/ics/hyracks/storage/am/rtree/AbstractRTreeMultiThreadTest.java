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

package edu.uci.ics.hyracks.storage.am.rtree;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.common.ITreeIndexTestWorkerFactory;
import edu.uci.ics.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import edu.uci.ics.hyracks.storage.am.common.TestWorkloadConf;
import edu.uci.ics.hyracks.storage.am.common.TreeIndexMultiThreadTestDriver;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;

@SuppressWarnings("rawtypes")
public abstract class AbstractRTreeMultiThreadTest {

    protected final Logger LOGGER = Logger.getLogger(AbstractRTreeMultiThreadTest.class.getName());

    // Machine-specific number of threads to use for testing.
    protected final int REGULAR_NUM_THREADS = Runtime.getRuntime().availableProcessors();
    // Excessive number of threads for testing.
    protected final int EXCESSIVE_NUM_THREADS = Runtime.getRuntime().availableProcessors() * 4;
    protected final int NUM_OPERATIONS = 10000;

    protected ArrayList<TestWorkloadConf> workloadConfs = getTestWorkloadConf();

    protected abstract void setUp() throws HyracksException;

    protected abstract void tearDown() throws HyracksDataException;

    protected abstract ITreeIndex createTreeIndex(ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories) throws TreeIndexException;

    protected abstract int getFileId();

    protected abstract ITreeIndexTestWorkerFactory getWorkerFactory();

    protected abstract ArrayList<TestWorkloadConf> getTestWorkloadConf();

    protected abstract String getIndexTypeName();

    protected static float[] getUniformOpProbs(TestOperation[] ops) {
        float[] opProbs = new float[ops.length];
        for (int i = 0; i < ops.length; i++) {
            opProbs[i] = 1.0f / (float) ops.length;
        }
        return opProbs;
    }

    protected void runTest(ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeys, int numThreads,
            TestWorkloadConf conf, String dataMsg) throws HyracksException, InterruptedException, TreeIndexException {
        setUp();

        if (LOGGER.isLoggable(Level.INFO)) {
            String indexTypeName = getIndexTypeName();
            LOGGER.info(indexTypeName + " MultiThread Test:\nData: " + dataMsg + "; Threads: " + numThreads
                    + "; Workload: " + conf.toString() + ".");
        }

        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] rtreeCmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeys);
        IBinaryComparatorFactory[] btreeCmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes,
                fieldSerdes.length);

        ITreeIndex index = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories);
        ITreeIndexTestWorkerFactory workerFactory = getWorkerFactory();

        // 4 batches per thread.
        int batchSize = (NUM_OPERATIONS / numThreads) / 4;

        TreeIndexMultiThreadTestDriver driver = new TreeIndexMultiThreadTestDriver(index, workerFactory, fieldSerdes,
                conf.ops, conf.opProbs);
        driver.init(getFileId());
        long[] times = driver.run(numThreads, 1, NUM_OPERATIONS, batchSize);
        driver.deinit();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("RTree MultiThread Test Time: " + times[0] + "ms");
        }

        tearDown();
    }

    @Test
    public void twoDimensionsInt() throws InterruptedException, HyracksException, TreeIndexException {
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        int numKeys = 4;
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                numKeys, IntegerPointable.FACTORY);

        String dataMsg = "Two Dimensions Of Integer Values";

        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, valueProviderFactories, numKeys, REGULAR_NUM_THREADS, conf, dataMsg);
            runTest(fieldSerdes, valueProviderFactories, numKeys, EXCESSIVE_NUM_THREADS, conf, dataMsg);
        }
    }

    //@Test
    public void fourDimensionsDouble() throws InterruptedException, HyracksException, TreeIndexException {
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };

        int numKeys = 8;
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                numKeys, DoublePointable.FACTORY);

        String dataMsg = "Four Dimensions Of Double Values";

        for (TestWorkloadConf conf : workloadConfs) {
            runTest(fieldSerdes, valueProviderFactories, numKeys, REGULAR_NUM_THREADS, conf, dataMsg);
            runTest(fieldSerdes, valueProviderFactories, numKeys, EXCESSIVE_NUM_THREADS, conf, dataMsg);
        }
    }
}
