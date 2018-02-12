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
package org.apache.hyracks.storage.am.btree;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.DiskBTree;
import org.apache.hyracks.storage.am.btree.util.BTreeTestHarness;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetaDataPageManager;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.test.IIndexCursorTest;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class DiskBTreeRangeSearchCursorTest extends IIndexCursorTest {
    private static final BTreeTestHarness harness = new BTreeTestHarness();
    private static DiskBTree btree;

    @BeforeClass
    public static void setup() throws HyracksDataException {
        harness.setUp();
        IBufferCache bufferCache = harness.getBufferCache();
        IMetadataPageManager freePageManager =
                new LinkedMetaDataPageManager(bufferCache, BTreeSearchCursorTest.META_FRAME_FACTORY);
        btree = new DiskBTree(bufferCache, freePageManager, BTreeSearchCursorTest.INTERIOR_FRAME_FACTORY,
                BTreeSearchCursorTest.LEAF_FRAME_FACTORY, BTreeSearchCursorTest.CMP_FACTORIES,
                BTreeSearchCursorTest.FIELD_COUNT, harness.getFileReference());
        btree.create();
        btree.activate();

        TreeSet<Integer> uniqueKeys = new TreeSet<>();
        ArrayList<Integer> keys = new ArrayList<>();
        // generate keys
        int numKeys = 50;
        int maxKey = 1000;
        while (uniqueKeys.size() < numKeys) {
            int key = BTreeSearchCursorTest.RANDOM.nextInt() % maxKey;
            uniqueKeys.add(key);
        }
        for (Integer i : uniqueKeys) {
            keys.add(i);
        }
        DiskBTreeSearchCursorTest.bulkLoadBTree(keys, btree);
    }

    @AfterClass
    public static void tearDown() throws HyracksDataException {
        try {
            btree.deactivate();
            btree.destroy();
        } finally {
            harness.tearDown();
        }
    }

    @Override
    protected List<ISearchPredicate> createSearchPredicates() throws HyracksDataException {
        List<ISearchPredicate> predicates = new ArrayList<>();
        int minKey = -10;
        int maxKey = 10;
        for (int i = minKey; i < maxKey; i++) {
            for (int j = minKey; j < maxKey; j++) {
                int lowKey = i;
                int highKey = j;
                predicates.add(BTreeSearchCursorTest.createRangePredicate(lowKey, highKey, true, true));
            }
        }
        return predicates;
    }

    @Override
    protected IIndexAccessor createAccessor() throws Exception {
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        return btree.createAccessor(actx);
    }
}
