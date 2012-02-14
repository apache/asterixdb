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

package edu.uci.ics.hyracks.storage.am.rtree.multithread;

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.ITreeIndexTestWorkerFactory;
import edu.uci.ics.hyracks.storage.am.common.TestWorkloadConf;
import edu.uci.ics.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.rtree.AbstractRTreeMultiThreadTest;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;
import edu.uci.ics.hyracks.storage.am.rtree.utils.RTreeTestHarness;

public class RTreeMultiThreadTest extends AbstractRTreeMultiThreadTest {

    private RTreeTestHarness harness = new RTreeTestHarness();

    private RTreeTestWorkerFactory workerFactory = new RTreeTestWorkerFactory();

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
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories)
            throws TreeIndexException {
        return RTreeUtils.createRTree(harness.getBufferCache(), typeTraits,
                valueProviderFactories, rtreeCmpFactories);

    }

    @Override
    protected ITreeIndexTestWorkerFactory getWorkerFactory() {
        return workerFactory;
    }

    @Override
    protected ArrayList<TestWorkloadConf> getTestWorkloadConf() {
        ArrayList<TestWorkloadConf> workloadConfs = new ArrayList<TestWorkloadConf>();

        // Insert only workload.
        TestOperation[] insertOnlyOps = new TestOperation[] { TestOperation.INSERT };
        workloadConfs.add(new TestWorkloadConf(insertOnlyOps, getUniformOpProbs(insertOnlyOps)));

        // Inserts mixed with scans.
        TestOperation[] insertSearchOnlyOps = new TestOperation[] { TestOperation.INSERT, TestOperation.SCAN,
                TestOperation.DISKORDER_SCAN };
        workloadConfs.add(new TestWorkloadConf(insertSearchOnlyOps, getUniformOpProbs(insertSearchOnlyOps)));

        // Inserts and deletes.
        TestOperation[] insertDeleteOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE };
        workloadConfs.add(new TestWorkloadConf(insertDeleteOps, getUniformOpProbs(insertDeleteOps)));

        // All operations mixed.
        TestOperation[] allOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE, TestOperation.SCAN,
                TestOperation.DISKORDER_SCAN };
        workloadConfs.add(new TestWorkloadConf(allOps, getUniformOpProbs(allOps)));

        return workloadConfs;
    }

    @Override
    protected int getFileId() {
        return harness.getTreeFileId();
    }

    @Override
    protected String getIndexTypeName() {
        return "RTree";
    }
}
