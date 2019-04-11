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

package org.apache.hyracks.storage.am.lsm.rtree;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.rtree.AbstractRTreeExamplesTest;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.am.rtree.util.RTreeUtils;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.junit.Test;

public abstract class AbstractLSMRTreeExamplesTest extends AbstractRTreeExamplesTest {

    /**
     * Test the LSM component filters.
     */
    @Test
    public void additionalFilteringingExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing LSMRTree or LSMRTreeWithAntiMatterTuples component filters.");
        }

        // Declare fields.
        int fieldCount = 6;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;
        typeTraits[3] = IntegerPointable.TYPE_TRAITS;
        typeTraits[4] = IntegerPointable.TYPE_TRAITS;
        typeTraits[5] = IntegerPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        // Declare RTree keys.
        int rtreeKeyFieldCount = 4;
        IBinaryComparatorFactory[] rtreeCmpFactories = new IBinaryComparatorFactory[rtreeKeyFieldCount];
        rtreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[2] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[3] = IntegerBinaryComparatorFactory.INSTANCE;

        // Declare BTree keys, this will only be used for LSMRTree
        int btreeKeyFieldCount;
        IBinaryComparatorFactory[] btreeCmpFactories;
        int[] btreeFields = null;
        if (rTreeType == RTreeType.LSMRTREE) {
            //Parameters look different for LSM RTREE from LSM RTREE WITH ANTI MATTER TUPLES
            btreeKeyFieldCount = 1;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeFields = new int[btreeKeyFieldCount];
            for (int i = 0; i < btreeKeyFieldCount; i++) {
                btreeFields[i] = rtreeKeyFieldCount + i;
            }

        } else {
            btreeKeyFieldCount = 6;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[2] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[3] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[4] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[5] = IntegerBinaryComparatorFactory.INSTANCE;
        }

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(rtreeCmpFactories.length, IntegerPointable.FACTORY);

        int[] rtreeFields = { 0, 1, 2, 3, 4 };
        ITypeTraits[] filterTypeTraits = { IntegerPointable.TYPE_TRAITS };
        IBinaryComparatorFactory[] filterCmpFactories = { IntegerBinaryComparatorFactory.INSTANCE };
        int[] filterFields = { 5 };

        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories,
                RTreePolicyType.RTREE, rtreeFields, btreeFields, filterTypeTraits, filterCmpFactories, filterFields);
        treeIndex.create();
        treeIndex.activate();

        long start = System.currentTimeMillis();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Inserting into tree...");
        }
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor indexAccessor = treeIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        int numInserts = 10000;
        for (int i = 0; i < numInserts; i++) {
            int p1x = rnd.nextInt();
            int p1y = rnd.nextInt();
            int p2x = rnd.nextInt();
            int p2y = rnd.nextInt();

            int pk = 5;
            int filter = i;

            TupleUtils.createIntegerTuple(tb, tuple, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                    Math.max(p1y, p2y), pk, filter);
            try {
                indexAccessor.insert(tuple);
            } catch (HyracksDataException e) {
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw e;
                }
            }
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(numInserts + " inserts in " + (end - start) + "ms");
        }

        scan(indexAccessor, fieldSerdes);
        diskOrderScan(indexAccessor, fieldSerdes);

        // Build key.
        ArrayTupleBuilder keyTb = new ArrayTupleBuilder(rtreeKeyFieldCount);
        ArrayTupleReference key = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(keyTb, key, -1000, -1000, 1000, 1000);

        // Build min filter key.
        ArrayTupleBuilder minFilterTb = new ArrayTupleBuilder(filterFields.length);
        ArrayTupleReference minTuple = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(minFilterTb, minTuple, 400);

        // Build max filter key.
        ArrayTupleBuilder maxFilterTb = new ArrayTupleBuilder(filterFields.length);
        ArrayTupleReference maxTuple = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(maxFilterTb, maxTuple, 500);

        rangeSearch(rtreeCmpFactories, indexAccessor, fieldSerdes, key, minTuple, maxTuple);

        treeIndex.deactivate();
        treeIndex.destroy();
    }

}
