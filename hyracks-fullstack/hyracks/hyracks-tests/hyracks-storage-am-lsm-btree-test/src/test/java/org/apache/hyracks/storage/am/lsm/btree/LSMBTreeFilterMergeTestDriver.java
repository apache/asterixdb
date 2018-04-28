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

package org.apache.hyracks.storage.am.lsm.btree;

import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestContext;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestDriver;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestUtils;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.common.TreeIndexTestUtils;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.junit.Assert;

/**
 * This test is the LSMBTreeMergeTest but using a filter, and at each step of the filter's lifecycle its value is
 * checked against computed min/maxes from the check tuples.
 */

@SuppressWarnings("rawtypes")
public abstract class LSMBTreeFilterMergeTestDriver extends OrderedIndexTestDriver {

    private final OrderedIndexTestUtils orderedIndexTestUtils;

    public LSMBTreeFilterMergeTestDriver(BTreeLeafFrameType[] leafFrameTypesToTest) {
        super(leafFrameTypesToTest);
        this.orderedIndexTestUtils = new OrderedIndexTestUtils();
    }

    private Pair<ITupleReference, ITupleReference> filterToMinMax(ILSMComponentFilter f) throws HyracksDataException {
        ArrayTupleBuilder builder = new ArrayTupleBuilder(1);
        builder.addField(f.getMinTuple().getFieldData(0), f.getMinTuple().getFieldStart(0),
                f.getMinTuple().getFieldLength(0));
        ArrayTupleReference minCopy = new ArrayTupleReference();
        minCopy.reset(builder.getFieldEndOffsets(), builder.getByteArray());
        builder = new ArrayTupleBuilder(1);
        builder.addField(f.getMaxTuple().getFieldData(0), f.getMaxTuple().getFieldStart(0),
                f.getMaxTuple().getFieldLength(0));
        ArrayTupleReference maxCopy = new ArrayTupleReference();
        maxCopy.reset(builder.getFieldEndOffsets(), builder.getByteArray());
        builder.reset();
        return Pair.of(minCopy, maxCopy);
    }

    @Override
    protected void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, BTreeLeafFrameType leafType,
            ITupleReference lowKey, ITupleReference highKey, ITupleReference prefixLowKey,
            ITupleReference prefixHighKey) throws Exception {
        OrderedIndexTestContext ctx = createTestContext(fieldSerdes, numKeys, leafType, true);
        ctx.getIndex().create();
        ctx.getIndex().activate();
        // Start off with one tree bulk loaded.
        // We assume all fieldSerdes are of the same type. Check the first one
        // to determine which field types to generate.
        if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
            orderedIndexTestUtils.bulkLoadIntTuples(ctx, numTuplesToInsert, true, getRandom());
        } else if (fieldSerdes[0] instanceof UTF8StringSerializerDeserializer) {
            orderedIndexTestUtils.bulkLoadStringTuples(ctx, numTuplesToInsert, true, getRandom());
        }

        int maxTreesToMerge = AccessMethodTestsConfig.LSM_BTREE_MAX_TREES_TO_MERGE;
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) ctx.getIndexAccessor();
        IBinaryComparator comp = ctx.getComparatorFactories()[0].createBinaryComparator();
        for (int i = 0; i < maxTreesToMerge; i++) {
            int flushed = 0;
            for (; flushed < i; flushed++) {
                Pair<ITupleReference, ITupleReference> minMax = null;
                if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
                    minMax = orderedIndexTestUtils.insertIntTuples(ctx, numTuplesToInsert, true, getRandom());
                } else {
                    minMax = orderedIndexTestUtils.insertStringTuples(ctx, numTuplesToInsert, true, getRandom());
                }
                if (minMax != null) {
                    ILSMComponentFilter f =
                            ((LSMBTree) ctx.getIndex()).getCurrentMemoryComponent().getLSMComponentFilter();
                    Pair<ITupleReference, ITupleReference> obsMinMax = filterToMinMax(f);
                    Assert.assertEquals(0,
                            TreeIndexTestUtils.compareFilterTuples(obsMinMax.getLeft(), minMax.getLeft(), comp));
                    Assert.assertEquals(0,
                            TreeIndexTestUtils.compareFilterTuples(obsMinMax.getRight(), minMax.getRight(), comp));
                }

                ILSMIOOperation flush = accessor.scheduleFlush();
                flush.sync();
                if (flush.getStatus() == LSMIOOperationStatus.FAILURE) {
                    throw HyracksDataException.create(flush.getFailure());
                }
                if (minMax != null) {
                    Pair<ITupleReference, ITupleReference> obsMinMax =
                            filterToMinMax(flush.getNewComponent().getLSMComponentFilter());
                    Assert.assertEquals(0,
                            TreeIndexTestUtils.compareFilterTuples(obsMinMax.getLeft(), minMax.getLeft(), comp));
                    Assert.assertEquals(0,
                            TreeIndexTestUtils.compareFilterTuples(obsMinMax.getRight(), minMax.getRight(), comp));
                }
            }

            List<ILSMDiskComponent> flushedComponents = ((LSMBTree) ctx.getIndex()).getDiskComponents();
            MutablePair<ITupleReference, ITupleReference> expectedMergeMinMax = null;
            for (ILSMDiskComponent f : flushedComponents) {
                Pair<ITupleReference, ITupleReference> componentMinMax = filterToMinMax(f.getLSMComponentFilter());
                if (expectedMergeMinMax == null) {
                    expectedMergeMinMax = MutablePair.of(componentMinMax.getLeft(), componentMinMax.getRight());
                }
                if (TreeIndexTestUtils.compareFilterTuples(expectedMergeMinMax.getLeft(), componentMinMax.getLeft(),
                        comp) > 0) {
                    expectedMergeMinMax.setLeft(componentMinMax.getLeft());
                }
                if (TreeIndexTestUtils.compareFilterTuples(expectedMergeMinMax.getRight(), componentMinMax.getRight(),
                        comp) < 0) {
                    expectedMergeMinMax.setRight(componentMinMax.getRight());
                }
            }
            accessor.scheduleMerge(((LSMBTree) ctx.getIndex()).getDiskComponents());

            flushedComponents = ((LSMBTree) ctx.getIndex()).getDiskComponents();
            Pair<ITupleReference, ITupleReference> mergedMinMax =
                    filterToMinMax(flushedComponents.get(0).getLSMComponentFilter());
            Assert.assertEquals(0, TreeIndexTestUtils.compareFilterTuples(expectedMergeMinMax.getLeft(),
                    mergedMinMax.getLeft(), comp));
            Assert.assertEquals(0, TreeIndexTestUtils.compareFilterTuples(expectedMergeMinMax.getRight(),
                    mergedMinMax.getRight(), comp));

            orderedIndexTestUtils.checkPointSearches(ctx);
            orderedIndexTestUtils.checkScan(ctx);
            orderedIndexTestUtils.checkDiskOrderScan(ctx);
            orderedIndexTestUtils.checkRangeSearch(ctx, lowKey, highKey, true, true);
            if (prefixLowKey != null && prefixHighKey != null) {
                orderedIndexTestUtils.checkRangeSearch(ctx, prefixLowKey, prefixHighKey, true, true);
            }
        }
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    @Override
    protected String getTestOpName() {
        return "LSM Merge";
    }
}
