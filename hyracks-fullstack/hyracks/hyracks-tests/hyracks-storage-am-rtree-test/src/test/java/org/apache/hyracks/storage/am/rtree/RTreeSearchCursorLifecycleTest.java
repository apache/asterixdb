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
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetaDataPageManager;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.test.IIndexCursorTest;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.utils.RTreeTestHarness;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class RTreeSearchCursorLifecycleTest extends IIndexCursorTest {

    private static final RTreeTestHarness harness = new RTreeTestHarness();
    private static RTree rtree;

    @BeforeClass
    public static void setup() throws HyracksDataException {
        harness.setUp();
        IBufferCache bufferCache = harness.getBufferCache();
        rtree = new RTree(bufferCache,
                new LinkedMetaDataPageManager(bufferCache, RTreeSearchCursorTest.META_FRAME_FACTORY),
                RTreeSearchCursorTest.INTERIOR_FRAME_FACTORY, RTreeSearchCursorTest.LEAF_FRAME_FACTORY,
                RTreeSearchCursorTest.CMP_FACTORIES, RTreeSearchCursorTest.FIELD_COUNT, harness.getFileReference(),
                false);
        rtree.create();
        rtree.activate();
        RTreeSearchCursorTest.insert(rtree);
    }

    @AfterClass
    public static void teardown() throws HyracksDataException {
        try {
            rtree.deactivate();
            rtree.destroy();
        } finally {
            harness.tearDown();
        }
    }

    @Override
    protected List<ISearchPredicate> createSearchPredicates() throws Exception {
        List<ISearchPredicate> predicates = new ArrayList<>();
        predicates.add(RTreeSearchCursorTest.createSearchPredicate(new ArrayTupleReference(), -100, -100, 100, 100));
        predicates.add(RTreeSearchCursorTest.createSearchPredicate(new ArrayTupleReference(), -200, -200, 200, 200));
        predicates.add(RTreeSearchCursorTest.createSearchPredicate(new ArrayTupleReference(), -300, -300, 300, 300));
        predicates.add(RTreeSearchCursorTest.createSearchPredicate(new ArrayTupleReference(), -400, -400, 400, 400));
        predicates.add(RTreeSearchCursorTest.createSearchPredicate(new ArrayTupleReference(), -500, -500, 500, 500));
        predicates.add(RTreeSearchCursorTest.createSearchPredicate(new ArrayTupleReference(), -600, -600, 600, 600));
        predicates.add(RTreeSearchCursorTest.createSearchPredicate(new ArrayTupleReference(), -700, -700, 700, 700));
        predicates.add(RTreeSearchCursorTest.createSearchPredicate(new ArrayTupleReference(), -800, -800, 800, 800));
        predicates.add(RTreeSearchCursorTest.createSearchPredicate(new ArrayTupleReference(), -900, -900, 900, 900));
        predicates
                .add(RTreeSearchCursorTest.createSearchPredicate(new ArrayTupleReference(), -1000, -1000, 1000, 1000));
        return predicates;
    }

    @Override
    protected IIndexAccessor createAccessor() throws Exception {
        return rtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
    }

}
