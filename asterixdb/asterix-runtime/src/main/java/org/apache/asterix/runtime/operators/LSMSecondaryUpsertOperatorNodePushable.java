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
package org.apache.asterix.runtime.operators;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback.Operation;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspector;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMIndexInsertUpdateDeleteOperatorNodePushable;

/**
 * This operator node is used for secondary indexes with upsert operations.
 * It works in the following way:
 * For each incoming tuple
 * -If old secondary keys == new secondary keys
 * --do nothing
 * -else
 * --If old secondary keys are null?
 * ---do nothing
 * --else
 * ---delete old secondary keys
 * --If new keys are null?
 * ---do nothing
 * --else
 * ---insert new keys
 */
public class LSMSecondaryUpsertOperatorNodePushable extends LSMIndexInsertUpdateDeleteOperatorNodePushable {

    private static final int NULL_MISSING_FIELD_INDEX = 0;
    private final PermutingFrameTupleReference prevValueTuple = new PermutingFrameTupleReference();
    private final int upsertIndicatorFieldIndex;
    private final IBinaryBooleanInspector upsertIndicatorInspector;
    private final int numberOfFields;
    private AbstractIndexModificationOperationCallback abstractModCallback;
    private final boolean isPrimaryKeyIndex;

    public LSMSecondaryUpsertOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            IIndexDataflowHelperFactory indexHelperFactory, IModificationOperationCallbackFactory modCallbackFactory,
            ITupleFilterFactory tupleFilterFactory, int[] fieldPermutation, RecordDescriptor inputRecDesc,
            int upsertIndicatorFieldIndex, IBinaryBooleanInspectorFactory upsertIndicatorInspectorFactory,
            int[] prevValuePermutation) throws HyracksDataException {
        super(ctx, partition, indexHelperFactory, fieldPermutation, inputRecDesc, IndexOperation.UPSERT,
                modCallbackFactory, tupleFilterFactory);
        this.prevValueTuple.setFieldPermutation(prevValuePermutation);
        this.upsertIndicatorFieldIndex = upsertIndicatorFieldIndex;
        this.upsertIndicatorInspector = upsertIndicatorInspectorFactory.createBinaryBooleanInspector(ctx);
        this.numberOfFields = prevValuePermutation.length;
        // a primary key index only has primary keys, and thus these two permutations are the same
        this.isPrimaryKeyIndex = Arrays.equals(fieldPermutation, prevValuePermutation);
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        frameTuple = new FrameTupleReference();
        abstractModCallback = (AbstractIndexModificationOperationCallback) modCallback;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) indexAccessor;
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            try {
                frameTuple.reset(accessor, i);
                boolean isUpsert =
                        upsertIndicatorInspector.getBooleanValue(frameTuple.getFieldData(upsertIndicatorFieldIndex),
                                frameTuple.getFieldStart(upsertIndicatorFieldIndex),
                                frameTuple.getFieldLength(upsertIndicatorFieldIndex));
                // if both previous value and new value are null, then we skip
                tuple.reset(accessor, i);
                prevValueTuple.reset(accessor, i);

                boolean isNewValueNullOrMissing = isNullOrMissing(tuple);
                boolean isOldValueNullOrMissing = isNullOrMissing(prevValueTuple);
                if (isNewValueNullOrMissing && isOldValueNullOrMissing) {
                    // No op
                    continue;
                }
                // At least, one is not null
                if (!isPrimaryKeyIndex && TupleUtils.equalTuples(tuple, prevValueTuple, numberOfFields)) {
                    // For a secondary index, if the secondary key values do not change, we can skip upserting it.
                    // However, for a primary key index, we cannot do this because it only contains primary keys
                    // which are always the same
                    continue;
                }
                if (!isOldValueNullOrMissing) {
                    // We need to delete previous
                    abstractModCallback.setOp(Operation.DELETE);
                    lsmAccessor.forceDelete(prevValueTuple);
                }
                if (isUpsert && !isNewValueNullOrMissing) {
                    // we need to insert the new value
                    abstractModCallback.setOp(Operation.INSERT);
                    lsmAccessor.forceInsert(tuple);
                }
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }
        // No partial flushing was necessary. Forward entire frame.
        writeBuffer.ensureFrameSize(buffer.capacity());
        FrameUtils.copyAndFlip(buffer, writeBuffer.getBuffer());
        FrameUtils.flushFrame(writeBuffer.getBuffer(), writer);
    }

    private static boolean isNullOrMissing(PermutingFrameTupleReference tuple) {
        return TypeTagUtil.isType(tuple, NULL_MISSING_FIELD_INDEX, ATypeTag.SERIALIZED_NULL_TYPE_TAG)
                || TypeTagUtil.isType(tuple, NULL_MISSING_FIELD_INDEX, ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
    }
}
