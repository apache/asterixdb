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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.multithread;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import edu.uci.ics.hyracks.storage.am.common.TestWorkloadConf;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.ProbabilityHelper;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleGenerator;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.common.LSMInvertedIndexTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext.InvertedIndexType;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestUtils;

public class LSMInvertedIndexMultiThreadTest {

    protected final Logger LOGGER = Logger.getLogger(LSMInvertedIndexMultiThreadTest.class.getName());

    // Machine-specific number of threads to use for testing.
    protected final int REGULAR_NUM_THREADS = Runtime.getRuntime().availableProcessors();
    // Excessive number of threads for testing.
    protected final int EXCESSIVE_NUM_THREADS = Runtime.getRuntime().availableProcessors() * 4;
    protected final int NUM_OPERATIONS = AccessMethodTestsConfig.LSM_INVINDEX_MULTITHREAD_NUM_OPERATIONS;

    protected final LSMInvertedIndexTestHarness harness = new LSMInvertedIndexTestHarness();
    protected final LSMInvertedIndexWorkerFactory workerFactory = new LSMInvertedIndexWorkerFactory();
    protected final ArrayList<TestWorkloadConf> workloadConfs = getTestWorkloadConf();

    protected void setUp() throws HyracksException {
        harness.setUp();
    }

    protected void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    protected void runTest(LSMInvertedIndexTestContext testCtx, TupleGenerator tupleGen, int numThreads,
            TestWorkloadConf conf, String dataMsg) throws InterruptedException, TreeIndexException, HyracksException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("LSMInvertedIndex MultiThread Test:\nData: " + dataMsg + "; Threads: " + numThreads
                    + "; Workload: " + conf.toString() + ".");
        }

        // 4 batches per thread.
        int batchSize = (NUM_OPERATIONS / numThreads) / 4;

        LSMInvertedIndexMultiThreadTestDriver driver = new LSMInvertedIndexMultiThreadTestDriver(testCtx.getIndex(),
                workerFactory, tupleGen.getFieldSerdes(), tupleGen.getFieldGens(), conf.ops, conf.opProbs);
        driver.init();
        long[] times = driver.run(numThreads, 1, NUM_OPERATIONS, batchSize);
        testCtx.getIndex().validate();
        driver.deinit();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("LSMInvertedIndex MultiThread Test Time: " + times[0] + "ms");
        }
    }

    protected ArrayList<TestWorkloadConf> getTestWorkloadConf() {
        ArrayList<TestWorkloadConf> workloadConfs = new ArrayList<TestWorkloadConf>();

        // Insert only workload.
        TestOperation[] insertOnlyOps = new TestOperation[] { TestOperation.INSERT };
        workloadConfs.add(new TestWorkloadConf(insertOnlyOps, ProbabilityHelper
                .getUniformProbDist(insertOnlyOps.length)));

        // Insert and merge workload.
        TestOperation[] insertMergeOps = new TestOperation[] { TestOperation.INSERT, TestOperation.MERGE };
        workloadConfs.add(new TestWorkloadConf(insertMergeOps, ProbabilityHelper
                .getUniformProbDist(insertMergeOps.length)));

        // Inserts mixed with point searches and scans.
        TestOperation[] insertSearchOnlyOps = new TestOperation[] { TestOperation.INSERT, TestOperation.POINT_SEARCH,
                TestOperation.SCAN };
        workloadConfs.add(new TestWorkloadConf(insertSearchOnlyOps, ProbabilityHelper
                .getUniformProbDist(insertSearchOnlyOps.length)));

        // Inserts, and deletes.
        TestOperation[] insertDeleteUpdateOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE };
        workloadConfs.add(new TestWorkloadConf(insertDeleteUpdateOps, ProbabilityHelper
                .getUniformProbDist(insertDeleteUpdateOps.length)));

        // Inserts, deletes and merges.
        TestOperation[] insertDeleteUpdateMergeOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE,
                TestOperation.MERGE };
        workloadConfs.add(new TestWorkloadConf(insertDeleteUpdateMergeOps, ProbabilityHelper
                .getUniformProbDist(insertDeleteUpdateMergeOps.length)));

        // All operations except merge.
        TestOperation[] allNoMergeOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE,
                TestOperation.POINT_SEARCH, TestOperation.SCAN };
        workloadConfs.add(new TestWorkloadConf(allNoMergeOps, ProbabilityHelper
                .getUniformProbDist(allNoMergeOps.length)));

        // All operations.
        TestOperation[] allOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE,
                TestOperation.POINT_SEARCH, TestOperation.SCAN, TestOperation.MERGE };
        workloadConfs.add(new TestWorkloadConf(allOps, ProbabilityHelper.getUniformProbDist(allOps.length)));

        return workloadConfs;
    }

    @Test
    public void wordTokensInvIndexTest() throws IOException, IndexException, InterruptedException {
        String dataMsg = "Documents";
        int[] numThreads = new int[] { REGULAR_NUM_THREADS, EXCESSIVE_NUM_THREADS };
        for (int i = 0; i < numThreads.length; i++) {
            for (TestWorkloadConf conf : workloadConfs) {
                setUp();
                LSMInvertedIndexTestContext testCtx = LSMInvertedIndexTestUtils.createWordInvIndexTestContext(harness,
                        getIndexType());
                TupleGenerator tupleGen = LSMInvertedIndexTestUtils.createStringDocumentTupleGen(harness.getRandom());
                runTest(testCtx, tupleGen, numThreads[i], conf, dataMsg);
                tearDown();
            }
        }
    }

    @Test
    public void hashedNGramTokensInvIndexTest() throws IOException, IndexException, InterruptedException {
        String dataMsg = "Person Names";
        int[] numThreads = new int[] { REGULAR_NUM_THREADS, EXCESSIVE_NUM_THREADS };
        for (int i = 0; i < numThreads.length; i++) {
            for (TestWorkloadConf conf : workloadConfs) {
                setUp();
                LSMInvertedIndexTestContext testCtx = LSMInvertedIndexTestUtils.createHashedNGramInvIndexTestContext(
                        harness, getIndexType());
                TupleGenerator tupleGen = LSMInvertedIndexTestUtils.createPersonNamesTupleGen(harness.getRandom());
                runTest(testCtx, tupleGen, numThreads[i], conf, dataMsg);
                tearDown();
            }
        }
    }

    protected InvertedIndexType getIndexType() {
        return InvertedIndexType.LSM;
    }
}
