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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.DiskBTree;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetaDataPageManager;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DiskBTreeSearchCursorTest extends BTreeSearchCursorTest {

    @Test
    public void batchPointLookupSingleLevelTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("TESTING POINT LOOKUP CURSOR ON UNIQUE 1 LEVEL INDEX");
        }
        // only 4 keys
        batchPointLookupTest(4, 4, -8, 8);
    }

    @Test
    public void batchPointLookupMultiLevelTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("TESTING POINT LOOKUP CURSOR ON UNIQUE MULTI-LEVEL INDEX");
        }
        // only 10000 keys
        batchPointLookupTest(10000, 20000, -1000, 1000);
    }

    @Test
    public void batchPointLookupMultiLevelOutRangeTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("TESTING POINT LOOKUP CURSOR ON UNIQUE 1 LEVEL INDEX");
        }
        // only 100 keys
        batchPointLookupTest(100, 200, -1000, 1000);
    }

    private void batchPointLookupTest(int numKeys, int maxKey, int minSearchKey, int maxSearchKey) throws Exception {

        IBufferCache bufferCache = harness.getBufferCache();
        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) LEAF_FRAME_FACTORY.createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) INTERIOR_FRAME_FACTORY.createFrame();
        IMetadataPageManager freePageManager = new LinkedMetaDataPageManager(bufferCache, META_FRAME_FACTORY);
        DiskBTree btree = new DiskBTree(bufferCache, freePageManager, INTERIOR_FRAME_FACTORY, LEAF_FRAME_FACTORY,
                CMP_FACTORIES, FIELD_COUNT, harness.getFileReference());
        btree.create();
        btree.activate();

        TreeSet<Integer> uniqueKeys = new TreeSet<>();
        ArrayList<Integer> keys = new ArrayList<>();
        while (uniqueKeys.size() < numKeys) {
            int key = RANDOM.nextInt() % maxKey;
            uniqueKeys.add(key);
        }
        for (Integer i : uniqueKeys) {
            keys.add(i);
        }

        insertBTree(keys, btree);

        // forward searches
        Assert.assertTrue(performBatchLookups(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey));

        btree.deactivate();
        btree.destroy();
    }

    private boolean performBatchLookups(ArrayList<Integer> keys, BTree btree, IBTreeLeafFrame leafFrame,
            IBTreeInteriorFrame interiorFrame, int minKey, int maxKey) throws Exception {

        ArrayList<Integer> results = new ArrayList<>();
        ArrayList<Integer> expectedResults = new ArrayList<>();
        BTreeAccessor indexAccessor = btree.createAccessor(
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE));
        IIndexCursor pointCursor = indexAccessor.createPointCursor(false);
        try {
            for (int i = minKey; i < maxKey; i++) {
                results.clear();
                expectedResults.clear();
                int lowKey = i;
                int highKey = i;
                RangePredicate rangePred = createRangePredicate(lowKey, highKey, true, true);
                indexAccessor.search(pointCursor, rangePred);
                try {
                    while (pointCursor.hasNext()) {
                        pointCursor.next();
                        ITupleReference frameTuple = pointCursor.getTuple();
                        ByteArrayInputStream inStream = new ByteArrayInputStream(frameTuple.getFieldData(0),
                                frameTuple.getFieldStart(0), frameTuple.getFieldLength(0));
                        DataInput dataIn = new DataInputStream(inStream);
                        Integer res = IntegerSerializerDeserializer.INSTANCE.deserialize(dataIn);
                        results.add(res);
                    }
                } finally {
                    pointCursor.close();
                }
                getExpectedResults(expectedResults, keys, lowKey, highKey, true, true);
                if (results.size() == expectedResults.size()) {
                    for (int k = 0; k < results.size(); k++) {
                        if (!results.get(k).equals(expectedResults.get(k))) {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("DIFFERENT RESULTS AT: i=" + i + " k=" + k);
                                LOGGER.info(results.get(k) + " " + expectedResults.get(k));
                            }
                            return false;
                        }
                    }
                } else {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("UNEQUAL NUMBER OF RESULTS AT: i=" + i);
                        LOGGER.info("RESULTS: " + results.size());
                        LOGGER.info("EXPECTED RESULTS: " + expectedResults.size());
                    }
                    return false;
                }
            }
        } finally {
            pointCursor.destroy();
        }
        return true;
    }

    @Override
    protected void insertBTree(List<Integer> keys, BTree btree) throws HyracksDataException {
        bulkLoadBTree(keys, btree);
    }

    public static void bulkLoadBTree(List<Integer> keys, BTree btree) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(FIELD_COUNT);
        ArrayTupleReference tuple = new ArrayTupleReference();

        IIndexBulkLoader bulkloader = btree.createBulkLoader(1, true, 0, true, NoOpPageWriteCallback.INSTANCE);
        // insert keys into btree
        for (int i = 0; i < keys.size(); i++) {
            TupleUtils.createIntegerTuple(tupleBuilder, tuple, keys.get(i), i);
            tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

            bulkloader.add(tuple);
        }
        bulkloader.end();
        if (bulkloader.hasFailed()) {
            throw HyracksDataException.create(bulkloader.getFailure());
        }
    }

}
