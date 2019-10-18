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

package org.apache.hyracks.storage.am.lsm.btree.multithread;

import java.util.ArrayList;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.OrderedIndexMultiThreadTest;
import org.apache.hyracks.storage.am.common.IIndexTestWorkerFactory;
import org.apache.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import org.apache.hyracks.storage.am.common.TestWorkloadConf;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.datagen.ProbabilityHelper;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.btree.utils.LSMBTreeUtil;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.util.trace.ITracer;

public class LSMBTreeMultiThreadTest extends OrderedIndexMultiThreadTest {

    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();

    private final LSMBTreeTestWorkerFactory workerFactory = new LSMBTreeTestWorkerFactory();

    @Override
    protected void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @Override
    protected void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected ITreeIndex createIndex(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields) throws HyracksDataException {
        return LSMBTreeUtil.createLSMTree(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), typeTraits, cmpFactories,
                bloomFilterKeyFields, harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(),
                harness.getOperationTracker(), harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), true, null, null, null, null, true,
                harness.getMetadataPageManagerFactory(), false, ITracer.NONE,
                NoOpCompressorDecompressorFactory.INSTANCE, bloomFilterKeyFields != null);
    }

    @Override
    protected IIndexTestWorkerFactory getWorkerFactory() {
        return workerFactory;
    }

    @Override
    protected ArrayList<TestWorkloadConf> getTestWorkloadConf() {
        ArrayList<TestWorkloadConf> workloadConfs = new ArrayList<>();

        // Insert only workload.
        TestOperation[] insertOnlyOps = new TestOperation[] { TestOperation.INSERT };
        workloadConfs
                .add(new TestWorkloadConf(insertOnlyOps, ProbabilityHelper.getUniformProbDist(insertOnlyOps.length)));

        // Insert and merge workload.
        TestOperation[] insertMergeOps = new TestOperation[] { TestOperation.INSERT, TestOperation.MERGE };
        workloadConfs
                .add(new TestWorkloadConf(insertMergeOps, ProbabilityHelper.getUniformProbDist(insertMergeOps.length)));

        // Inserts mixed with point searches and scans.
        TestOperation[] insertSearchOnlyOps =
                new TestOperation[] { TestOperation.INSERT, TestOperation.POINT_SEARCH, TestOperation.SCAN };
        workloadConfs.add(new TestWorkloadConf(insertSearchOnlyOps,
                ProbabilityHelper.getUniformProbDist(insertSearchOnlyOps.length)));

        // Inserts, updates, and deletes.
        TestOperation[] insertDeleteUpdateOps =
                new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE, TestOperation.UPDATE };
        workloadConfs.add(new TestWorkloadConf(insertDeleteUpdateOps,
                ProbabilityHelper.getUniformProbDist(insertDeleteUpdateOps.length)));

        // Inserts, updates, deletes and merges.
        TestOperation[] insertDeleteUpdateMergeOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE,
                TestOperation.UPDATE, TestOperation.MERGE };
        workloadConfs.add(new TestWorkloadConf(insertDeleteUpdateMergeOps,
                ProbabilityHelper.getUniformProbDist(insertDeleteUpdateMergeOps.length)));

        // All operations except merge.
        TestOperation[] allNoMergeOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE,
                TestOperation.UPDATE, TestOperation.POINT_SEARCH, TestOperation.SCAN };
        workloadConfs
                .add(new TestWorkloadConf(allNoMergeOps, ProbabilityHelper.getUniformProbDist(allNoMergeOps.length)));

        // All operations.
        TestOperation[] allOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE, TestOperation.UPDATE,
                TestOperation.POINT_SEARCH, TestOperation.SCAN, TestOperation.MERGE };
        workloadConfs.add(new TestWorkloadConf(allOps, ProbabilityHelper.getUniformProbDist(allOps.length)));

        return workloadConfs;
    }

    @Override
    protected String getIndexTypeName() {
        return "LSMBTree";
    }
}
