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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.multithread;

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.storage.am.common.IIndexTestWorkerFactory;
import edu.uci.ics.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import edu.uci.ics.hyracks.storage.am.common.TestWorkloadConf;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.ProbabilityHelper;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.util.LSMRTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import edu.uci.ics.hyracks.storage.am.rtree.AbstractRTreeMultiThreadTest;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;

public class LSMRTreeWithAntiMatterTuplesMultiThreadTest extends AbstractRTreeMultiThreadTest {

    private LSMRTreeTestHarness harness = new LSMRTreeTestHarness();

    private LSMRTreeWithAntiMatterTuplesTestWorkerFactory workerFactory = new LSMRTreeWithAntiMatterTuplesTestWorkerFactory();

    public LSMRTreeWithAntiMatterTuplesMultiThreadTest() {
        super(false);
    }

    @Override
    protected void setUp() throws HyracksException {
        harness.setUp();
    }

    @Override
    protected void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected ITreeIndex createTreeIndex(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType) throws TreeIndexException {
        return LSMRTreeUtils.createLSMTreeWithAntiMatterTuples(harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), harness.getDiskFileMapProvider(), typeTraits,
                rtreeCmpFactories, btreeCmpFactories, valueProviderFactories, rtreePolicyType,
                harness.getMergePolicy(), harness.getOperationTracker(), harness.getIOScheduler(),
                harness.getIOOperationCallback(),
                LSMRTreeUtils.proposeBestLinearizer(typeTraits, rtreeCmpFactories.length));

    }

    @Override
    protected IIndexTestWorkerFactory getWorkerFactory() {
        return workerFactory;
    }

    @Override
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

        // Inserts mixed with scans.
        TestOperation[] insertSearchOnlyOps = new TestOperation[] { TestOperation.INSERT, TestOperation.SCAN };
        workloadConfs.add(new TestWorkloadConf(insertSearchOnlyOps, ProbabilityHelper
                .getUniformProbDist(insertSearchOnlyOps.length)));

        // Inserts and deletes.
        TestOperation[] insertDeleteOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE };
        workloadConfs.add(new TestWorkloadConf(insertDeleteOps, ProbabilityHelper
                .getUniformProbDist(insertDeleteOps.length)));

        // Inserts, deletes and merges.
        TestOperation[] insertDeleteMergeOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE,
                TestOperation.MERGE };
        workloadConfs.add(new TestWorkloadConf(insertDeleteMergeOps, ProbabilityHelper
                .getUniformProbDist(insertDeleteMergeOps.length)));

        // All operations except merge.
        TestOperation[] allNoMergeOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE,
                TestOperation.SCAN };
        workloadConfs.add(new TestWorkloadConf(allNoMergeOps, ProbabilityHelper
                .getUniformProbDist(allNoMergeOps.length)));

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
