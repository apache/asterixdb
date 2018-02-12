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
package org.apache.hyracks.storage.am.lsm.btree.cursor;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.test.IIndexCursorTest;
import org.apache.hyracks.storage.am.lsm.btree.LSMBTreeExamplesTest;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class LSMBTreeSearchCursorTest extends IIndexCursorTest {

    private static final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();
    private static LSMBTree lsmBtree;

    @BeforeClass
    public static void setup() throws HyracksDataException {
        harness.setUp();
        lsmBtree = LSMBTreeExamplesTest.createTreeIndex(harness, LSMBTreePointSearchCursorTest.TYPE_TRAITS,
                LSMBTreePointSearchCursorTest.CMP_FACTORIES, LSMBTreePointSearchCursorTest.BLOOM_FILTER_KEY_FIELDS,
                null, null, null, null);
        lsmBtree.create();
        lsmBtree.activate();
        LSMBTreePointSearchCursorTest.insertData(lsmBtree);
    }

    @AfterClass
    public static void teardown() throws HyracksDataException {
        try {
            lsmBtree.deactivate();
            lsmBtree.destroy();
        } finally {
            harness.tearDown();
        }
    }

    @Override
    protected List<ISearchPredicate> createSearchPredicates() throws Exception {
        // exact and windows of length = 50
        List<ISearchPredicate> predicates = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            // Build low key.
            ArrayTupleBuilder lowKeyTb = new ArrayTupleBuilder(LSMBTreePointSearchCursorTest.KEY_FIELD_COUNT);
            ArrayTupleReference lowKey = new ArrayTupleReference();
            TupleUtils.createIntegerTuple(lowKeyTb, lowKey, -100 + (i * 50));
            // Build high key.
            ArrayTupleBuilder highKeyTb = new ArrayTupleBuilder(LSMBTreePointSearchCursorTest.KEY_FIELD_COUNT);
            ArrayTupleReference highKey = new ArrayTupleReference();
            TupleUtils.createIntegerTuple(highKeyTb, highKey, -100 + (i * 50) + 50);
            MultiComparator lowKeySearchCmp =
                    BTreeUtils.getSearchMultiComparator(LSMBTreePointSearchCursorTest.CMP_FACTORIES, lowKey);
            MultiComparator highKeySearchCmp =
                    BTreeUtils.getSearchMultiComparator(LSMBTreePointSearchCursorTest.CMP_FACTORIES, highKey);
            predicates.add(new RangePredicate(lowKey, highKey, true, true, lowKeySearchCmp, highKeySearchCmp));
            lowKeyTb = new ArrayTupleBuilder(LSMBTreePointSearchCursorTest.KEY_FIELD_COUNT);
            lowKey = new ArrayTupleReference();
            TupleUtils.createIntegerTuple(lowKeyTb, lowKey, -100 + (i * 50) + 25);
            // Build high key.
            highKeyTb = new ArrayTupleBuilder(LSMBTreePointSearchCursorTest.KEY_FIELD_COUNT);
            highKey = new ArrayTupleReference();
            TupleUtils.createIntegerTuple(highKeyTb, highKey, -100 + (i * 50) + 25);
            lowKeySearchCmp = BTreeUtils.getSearchMultiComparator(LSMBTreePointSearchCursorTest.CMP_FACTORIES, lowKey);
            highKeySearchCmp =
                    BTreeUtils.getSearchMultiComparator(LSMBTreePointSearchCursorTest.CMP_FACTORIES, highKey);
            predicates.add(new RangePredicate(lowKey, highKey, true, true, lowKeySearchCmp, highKeySearchCmp));
        }
        return predicates;
    }

    @Override
    protected IIndexAccessor createAccessor() throws Exception {
        return lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
    }
}
