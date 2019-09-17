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

package org.apache.hyracks.storage.am.lsm.rtree.multithread;

import java.util.ArrayList;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.IIndexTestWorkerFactory;
import org.apache.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import org.apache.hyracks.storage.am.common.TestWorkloadConf;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.datagen.ProbabilityHelper;
import org.apache.hyracks.storage.am.lsm.rtree.util.LSMRTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import org.apache.hyracks.storage.am.rtree.AbstractRTreeExamplesTest.RTreeType;
import org.apache.hyracks.storage.am.rtree.AbstractRTreeMultiThreadTest;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;

public class LSMRTreeWithAntiMatterTuplesMultiThreadTest extends AbstractRTreeMultiThreadTest {

    private final LSMRTreeTestHarness harness = new LSMRTreeTestHarness();

    private final LSMRTreeWithAntiMatterTuplesTestWorkerFactory workerFactory =
            new LSMRTreeWithAntiMatterTuplesTestWorkerFactory();

    public LSMRTreeWithAntiMatterTuplesMultiThreadTest() {
        super(false, RTreeType.LSMRTREE_WITH_ANTIMATTER);
    }

    @Override
    protected void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @Override
    protected void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected ITreeIndex createTreeIndex(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, int[] btreeFields) throws HyracksDataException {
        return LSMRTreeUtils.createLSMTreeWithAntiMatterTuples(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), typeTraits, rtreeCmpFactories,
                btreeCmpFactories, valueProviderFactories, rtreePolicyType, harness.getMergePolicy(),
                harness.getOperationTracker(), harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(),
                LSMRTreeUtils.proposeBestLinearizer(typeTraits, rtreeCmpFactories.length), null, null, null, null, true,
                false, harness.getMetadataPageManagerFactory());
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

        // Inserts mixed with scans.
        TestOperation[] insertSearchOnlyOps = new TestOperation[] { TestOperation.INSERT, TestOperation.SCAN };
        workloadConfs.add(new TestWorkloadConf(insertSearchOnlyOps,
                ProbabilityHelper.getUniformProbDist(insertSearchOnlyOps.length)));

        // Inserts and deletes.
        TestOperation[] insertDeleteOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE };
        workloadConfs.add(
                new TestWorkloadConf(insertDeleteOps, ProbabilityHelper.getUniformProbDist(insertDeleteOps.length)));

        // Inserts, deletes and merges.
        TestOperation[] insertDeleteMergeOps =
                new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE, TestOperation.MERGE };
        workloadConfs.add(new TestWorkloadConf(insertDeleteMergeOps,
                ProbabilityHelper.getUniformProbDist(insertDeleteMergeOps.length)));

        // All operations except merge.
        TestOperation[] allNoMergeOps =
                new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE, TestOperation.SCAN };
        workloadConfs
                .add(new TestWorkloadConf(allNoMergeOps, ProbabilityHelper.getUniformProbDist(allNoMergeOps.length)));

        // All operations.
        TestOperation[] allOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE, TestOperation.SCAN,
                TestOperation.MERGE };
        workloadConfs.add(new TestWorkloadConf(allOps, ProbabilityHelper.getUniformProbDist(allOps.length)));

        return workloadConfs;
    }

    @Override
    protected String getIndexTypeName() {
        return "LSMRTree";
    }

}
