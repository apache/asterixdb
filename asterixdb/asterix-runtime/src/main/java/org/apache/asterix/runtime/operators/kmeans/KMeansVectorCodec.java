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
package org.apache.asterix.runtime.operators.kmeans;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorListDecoder;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY k-means‖ initialization loop — codec bridging the loop's boundaries to the shipped
 * CLUSTER BY formats, used only by the Cost/Controller operator (Op1). Two pieces, both kept byte-compatible with
 * {@code KMeansStageRuntime}, which the merge Score stage is built on:
 * <ul>
 * <li>{@link ListVectorDecoder} — decodes an input vector column (an ordered list of doubles) into a
 * {@code double[]}. Op1's StoreVectors uses it ONCE per resident to materialize the vector run file as raw
 * doubles, and StoreSeed to seed the pool; the per-round cost/sample passes then read raw (no re-decode).</li>
 * <li>{@link PoolEnvelopeWriter} — emits pool members as the inter-stage {@code [kind, partition, seq, score,
 * vector]} open-list envelope (kind = 0 = pool) that the downstream merge consumes unchanged. Op1 uses it, on
 * partition 0, to emit the final pool downstream.</li>
 * </ul>
 * This logic is duplicated (not shared) from {@code KMeansStageRuntime}. The two encodings must stay
 * byte-compatible, since the loop's output is consumed by the merge stage built on that class; the
 * cluster-by runtime tests pin it. A later cleanup may extract a single source of truth.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Decoder bound to the declared dimension; admits only numeric arrays of that width")
public final class KMeansVectorCodec {

    /** Envelope kind fields (match KMeansStageRuntime.KIND_*). */
    private static final double KIND_POOL = 0.0d;
    public static final double KIND_PARTIAL = 2.0d;

    private KMeansVectorCodec() {
    }

    /**
     * Reusable decoder for an ordered-list-of-doubles vector column; one instance per operator task, bound to
     * the width the query declared.
     */
    public static final class ListVectorDecoder {
        private final VoidPointable fieldPtr = new VoidPointable();
        private final ListAccessor listAccessor = new ListAccessor();
        private final VectorListDecoder decoder = new VectorListDecoder();
        // The declared Dimension. Every value of another width belongs to a different vector space and takes
        // no part in this clustering.
        private final int dimension;

        public ListVectorDecoder(int dimension) {
            this.dimension = dimension;
        }

        /**
         * Decodes the vector column, or returns {@code null} -- the caller's cue to skip the row with a
         * warning -- for anything unusable: not an ordered list, wrong width, or a non-numeric/NaN element.
         * The width is enforced here, on the assembled value, rather than as a desugared WHERE conjunct: the
         * columnar filter pushdown can separate such a conjunct from its is-array guard and evaluate it once
         * per array ELEMENT inside the scan.
         */
        public double[] decode(FrameTupleReference tuple, int col) throws HyracksDataException {
            try {
                fieldPtr.set(tuple.getFieldData(col), tuple.getFieldStart(col), tuple.getFieldLength(col));
                byte[] bytes = fieldPtr.getByteArray();
                int offset = fieldPtr.getStartOffset();
                if (bytes[offset] != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                    return null;
                }
                listAccessor.reset(bytes, offset);
                if (listAccessor.size() != dimension) {
                    return null;
                }
                double[] arr = new double[listAccessor.size()];
                decoder.createArrayFromList(listAccessor, arr);
                for (double v : arr) {
                    if (Double.isNaN(v)) {
                        return null;
                    }
                }
                return arr;
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    /**
     * Emits pool members as {@code KIND_POOL} open-list envelopes to a writer — the exact format
     * {@code readInput} decodes. All items are tagged doubles; the vector is a nested open list.
     */
    public static final class PoolEnvelopeWriter {
        private final IFrameWriter writer;
        private final FrameTupleAppender appender;
        private final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        private final OrderedListBuilder listBuilder = new OrderedListBuilder();
        private final OrderedListBuilder vecBuilder = new OrderedListBuilder();
        private final ArrayBackedValueStorage itemStorage = new ArrayBackedValueStorage();
        private final ArrayBackedValueStorage vecStorage = new ArrayBackedValueStorage();
        private final AOrderedListType openList = new AOrderedListType(BuiltinType.ANY, null);

        public PoolEnvelopeWriter(IHyracksTaskContext ctx, IFrameWriter writer) throws HyracksDataException {
            this.writer = writer;
            this.appender = new FrameTupleAppender(new VSizeFrame(ctx));
        }

        /** Appends one pool-member echo envelope {@code [KIND_POOL, 0, seq, 0.0, vec]} (partition 0). */
        public void poolMember(int seq, double[] vec) throws HyracksDataException {
            envelope(KIND_POOL, 0, seq, 0.0d, vec);
        }

        /**
         * Appends one general inter-stage envelope {@code [kind, partition, seq, score, vec]} — the exact format
         * the weighing pass emits and RECLUSTER decodes. For a partial: {@code kind=KIND_PARTIAL}, {@code seq}=pool position,
         * {@code score}=count, {@code vec}=running sum.
         */
        public void envelope(double kind, int partition, int seq, double score, double[] vec)
                throws HyracksDataException {
            try {
                tb.reset();
                listBuilder.reset(openList);
                addDoubleItem(listBuilder, kind);
                addDoubleItem(listBuilder, partition);
                addDoubleItem(listBuilder, seq);
                addDoubleItem(listBuilder, score);
                vecBuilder.reset(openList);
                for (double d : vec) {
                    addDoubleItem(vecBuilder, d);
                }
                vecStorage.reset();
                vecBuilder.write(vecStorage.getDataOutput(), true);
                listBuilder.addItem(vecStorage);
                listBuilder.write(tb.getDataOutput(), true);
                tb.addFieldEndOffset();
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Appends one bare vector (no envelope wrapper) as a single ordered-list field — the shape a merge stage
         * emits for a centroid set, and what a consumer expecting plain vectors decodes.
         */
        public void plainVector(double[] vec) throws HyracksDataException {
            try {
                tb.reset();
                vecBuilder.reset(openList);
                for (double d : vec) {
                    addDoubleItem(vecBuilder, d);
                }
                vecBuilder.write(tb.getDataOutput(), true);
                tb.addFieldEndOffset();
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }

        public void flush() throws HyracksDataException {
            appender.write(writer, true);
        }

        private void addDoubleItem(OrderedListBuilder builder, double value) throws HyracksDataException {
            try {
                itemStorage.reset();
                itemStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                itemStorage.getDataOutput().writeDouble(value);
                builder.addItem(itemStorage);
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }
    }
}
