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

package org.apache.hyracks.storage.am.lsm.btree.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMIndexSampleCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

public class BTreeSampleCollectorOperatorDescriptorNodePushable extends BTreeSearchOperatorNodePushable {

    public static final String ESTIMATE_CARDINALITY = "ESTIMATE_CARDINALITY";
    private final int sampleCardinalityTargetPerPartition;
    private final long sampleSeed;
    private final int maxSampleLeafAttempts;
    private final int sampleLeafDrawBatchSize;
    private final int columnSamplesPerPage;
    private final IHyracksTaskContext ctx;

    public BTreeSampleCollectorOperatorDescriptorNodePushable(IHyracksTaskContext ctx, int partition,
            RecordDescriptor inputRecDesc, int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive,
            boolean highKeyInclusive, int[] minFilterKeyFields, int[] maxFilterKeyFields,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, ITupleProjectorFactory tupleProjectorFactory,
            ITuplePartitionerFactory tuplePartitionerFactory, int[][] partitionsMap,
            int sampleCardinalityTargetPerPartition, long sampleSeed, int maxSampleLeafAttempts,
            int sampleLeafDrawBatchSize, int columnSamplesPerPage) throws HyracksDataException {
        super(ctx, partition, inputRecDesc, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive,
                minFilterKeyFields, maxFilterKeyFields, indexHelperFactory, retainInput, retainMissing,
                missingWriterFactory, searchCallbackFactory, false, null, tupleFilterFactory, outputLimit, false, null,
                null, tupleProjectorFactory, tuplePartitionerFactory, partitionsMap);
        this.sampleCardinalityTargetPerPartition = sampleCardinalityTargetPerPartition;
        this.sampleSeed = sampleSeed;
        this.maxSampleLeafAttempts = maxSampleLeafAttempts;
        this.sampleLeafDrawBatchSize = sampleLeafDrawBatchSize;
        this.columnSamplesPerPage = columnSamplesPerPage;
        this.ctx = ctx;
    }

    @Override
    protected void addAdditionalIndexAccessorParams(IIndexAccessParameters iap) throws HyracksDataException {
        super.addAdditionalIndexAccessorParams(iap);
        iap.getParameters().put(LSMIndexSampleCursor.SAMPLE_CARDINALITY, sampleCardinalityTargetPerPartition);
        iap.getParameters().put(LSMIndexSampleCursor.SAMPLE_SEED, sampleSeed);
        iap.getParameters().put(LSMIndexSampleCursor.SAMPLE_MAX_LEAF_ATTEMPTS, maxSampleLeafAttempts);
        iap.getParameters().put(LSMIndexSampleCursor.SAMPLE_LEAF_DRAW_BATCH_SIZE, sampleLeafDrawBatchSize);
        iap.getParameters().put(LSMIndexSampleCursor.SAMPLE_COLUMN_SAMPLES_PER_PAGE, columnSamplesPerPage);
    }

    @Override
    protected IIndexCursor createCursor(IIndex idx, IIndexAccessor idxAccessor) throws HyracksDataException {
        ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) idxAccessor;
        return ((LSMBTree) idx).createSampleCollectorCursor(lsmAccessor.getOpContext());
    }

    @Override
    protected void writeSearchResults(int tupleIndex, IIndexCursor cursor) throws Exception {
        long matchingTupleCount = 0;
        while (cursor.hasNext()) {
            cursor.next();
            matchingTupleCount++;
            ITupleReference tuple = cursor.getTuple();
            tb.reset();

            if (retainInput) {
                frameTuple.reset(accessor, tupleIndex);
                for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                    dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    tb.addFieldEndOffset();
                }
            }

            // tuple must be written first before the filter is applied to
            // assemble columnar tuples
            tuple = writeTupleToOutput(tuple);
            if (tuple == null) {
                continue;
            }

            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            if (outputLimit >= 0 && ++outputCount >= outputLimit) {
                finished = true;
                break;
            }
        }
        stats.getInputTupleCounter().update(matchingTupleCount);
    }

    protected void searchPartition(int tupleCount) throws Exception {
        for (int i = 0; i < tupleCount && !finished; i++) {
            int storagePartition = tuplePartitioner.partition(accessor, i);
            int pIdx = storagePartitionId2Index.get(storagePartition);
            cursors[pIdx].close();
            ((ILSMIndexAccessor) indexAccessors[pIdx]).scanDiskComponentsForSample(cursors[pIdx]);
            writeSearchResults(i, cursors[pIdx]);
        }
    }

    protected void searchAllPartitions(int tupleCount) throws Exception {
        long estimatedCardinality = 0;
        for (int p = 0; p < partitions.length; p++) {
            for (int i = 0; i < tupleCount && !finished; i++) {
                cursors[p].close();
                ((ILSMIndexAccessor) indexAccessors[p]).scanDiskComponentsForSample(cursors[p]);
                writeSearchResults(i, cursors[p]);
            }
            estimatedCardinality += ((LSMIndexSampleCursor) cursors[p]).getEstimatedCardinality();
        }
        TaskUtil.put(ESTIMATE_CARDINALITY, estimatedCardinality, ctx);
    }
}
