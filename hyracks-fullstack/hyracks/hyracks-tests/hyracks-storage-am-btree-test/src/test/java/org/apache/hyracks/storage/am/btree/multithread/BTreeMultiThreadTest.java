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

package org.apache.hyracks.storage.am.btree.multithread;

import java.util.ArrayList;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.OrderedIndexMultiThreadTest;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.util.BTreeTestHarness;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.IIndexTestWorkerFactory;
import org.apache.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import org.apache.hyracks.storage.am.common.TestWorkloadConf;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.datagen.ProbabilityHelper;

public class BTreeMultiThreadTest extends OrderedIndexMultiThreadTest {

    private final BTreeTestHarness harness = new BTreeTestHarness();
    private final BTreeTestWorkerFactory workerFactory = new BTreeTestWorkerFactory();

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
        return BTreeUtils.createBTree(harness.getBufferCache(), typeTraits, cmpFactories,
                BTreeLeafFrameType.REGULAR_NSM, harness.getFileReference(),
                harness.getPageManagerFactory().createPageManager(harness.getBufferCache()));
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

        // Inserts mixed with point searches and scans.
        TestOperation[] insertSearchOnlyOps = new TestOperation[] { TestOperation.INSERT, TestOperation.POINT_SEARCH,
                TestOperation.SCAN, TestOperation.DISKORDER_SCAN };
        workloadConfs.add(new TestWorkloadConf(insertSearchOnlyOps,
                ProbabilityHelper.getUniformProbDist(insertSearchOnlyOps.length)));

        // Inserts, updates, deletes, and upserts.
        TestOperation[] insertDeleteUpdateUpsertOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE,
                TestOperation.UPDATE, TestOperation.UPSERT };
        workloadConfs.add(new TestWorkloadConf(insertDeleteUpdateUpsertOps,
                ProbabilityHelper.getUniformProbDist(insertDeleteUpdateUpsertOps.length)));

        // All operations mixed.
        TestOperation[] allOps = new TestOperation[] { TestOperation.INSERT, TestOperation.DELETE, TestOperation.UPDATE,
                TestOperation.UPSERT, TestOperation.POINT_SEARCH, TestOperation.SCAN, TestOperation.DISKORDER_SCAN };
        workloadConfs.add(new TestWorkloadConf(allOps, ProbabilityHelper.getUniformProbDist(allOps.length)));

        return workloadConfs;
    }

    @Override
    protected String getIndexTypeName() {
        return "BTree";
    }
}
