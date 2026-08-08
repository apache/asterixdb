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
package org.apache.hyracks.storage.am.lsm.vector.dataflow;

import java.io.IOException;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.vector.impls.IVectorSearchCursor;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeTopKSearchCursor;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunctionFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizerFactory;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchPredicate;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Runtime operator for vector index search (ANN search).
 * Extends IndexSearchOperatorNodePushable which handles the heavy lifting:
 * - Opening/closing indexes
 * - Frame/tuple iteration
 * - Output buffering
 * - Transaction callbacks
 *
 * This class implements the schema-agnostic pattern using IVTreeBinaryAccessor
 * to abstract over different vector serialization formats (e.g., AOrderedList).
 *
 * This class only needs to implement:
 * 1. createSearchPredicate() - Create a VTreeSearchPredicate (query passed via IAP, not deserialized here)
 * 2. resetSearchPredicate() - Point the predicate at the current input tuple
 * 3. getFieldCount() - Return number of output fields
 * 4. addAdditionalIndexAccessorParams() - Add vector-specific params (accessor/distance/quantizer factories)
 */
public class VTreeSearchOperatorNodePushable extends IndexSearchOperatorNodePushable {

    private static final Logger LOGGER = LogManager.getLogger();

    // Field indexes in input tuple: [query_vector_field, k_field, metric_field]
    protected final int[] queryFields;

    // Factory for creating vector accessors (passed from AsterixDB layer)
    protected final IVTreeBinaryAccessorFactory vectorAccessorFactory;

    // Factory for creating distance functions (injected from the AsterixDB layer)
    protected final IVTreeDistanceFunctionFactory distanceFunctionFactory;

    // Factory for creating per-query quantizers (passed from AsterixDB layer). Nullable for
    // non-quantized indexes and for test contexts that pre-inject a pre-built IVTreeQuantizer.
    protected final IVTreeQuantizerFactory quantizerFactory;

    // Tuple reference for extracting query parameters
    protected PermutingFrameTupleReference queryParamsTuple;

    // Factory for creating tuple filters for INCLUDE field predicates (e.g., year > 2000)
    // When set, the filter is pushed down to the cursor level for proper K counting
    protected final ITupleFilterFactory tupleFilterFactory;

    // The actual tuple filter, created from the factory
    protected ITupleFilter tupleFilter;

    // Multiplier for candidate limit: K * kMultiplier candidates sent to PK for reranking
    protected final int kMultiplier;

    /** Epsilon from vector index metadata (default 0.3 when absent in catalog). */
    protected final double indexEpsilon;

    /**
     * Number of primary-key fields in the dataset. Needed by {@link #getFieldCount} and by the
     * index-only write path to know where the projector's PK bytes end before appending the
     * distance field.
     */
    protected final int numPrimaryKeys;

    /**
     * Index-only ANN flag. When true the pushable emits {@code [pk..., D(q,x)]} per candidate by
     * stashing the active cursor and reaching into {@link IVectorSearchCursor} for the per-tuple
     * distance, then appending it as an ADOUBLE after the PK bytes the existing PKOnlyTupleProjector
     * writes. When false the pushable behaves exactly as before.
     */
    protected final boolean indexOnly;

    /**
     * Stashed by {@link #writeSearchResults} so the overridden {@link #writeTupleToOutput} can read
     * the per-tuple distance from the cursor that produced the current tuple.
     */
    private IIndexCursor activeCursor;

    public VTreeSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition, RecordDescriptor inputRecDesc,
            int[] queryFields, IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput,
            ISearchOperationCallbackFactory searchCallbackFactory, ITupleProjectorFactory projectorFactory,
            IVTreeBinaryAccessorFactory vectorAccessorFactory, IVTreeDistanceFunctionFactory distanceFunctionFactory,
            IVTreeQuantizerFactory quantizerFactory, int[][] partitionsMap, ITupleFilterFactory tupleFilterFactory,
            int kMultiplier, double indexEpsilon, int numPrimaryKeys, boolean indexOnly) throws HyracksDataException {
        // Vector search does its filtering in the cursor, so the operator passes no filter fields,
        // tuple filter, output limit, or search-callback proceed result (see the args below).
        super(ctx, inputRecDesc, partition, null, // minFilterFieldIndexes
                null, // maxFilterFieldIndexes
                indexHelperFactory, retainInput, false, // retainMissing
                null, // nonMatchWriterFactory
                searchCallbackFactory, false, // appendIndexFilter
                null, // nonFilterWriterFactory
                null, // tupleFilterFactory - we handle this at cursor level, not operator level
                -1, // outputLimit
                false, // appendOpCallbackProceedResult
                null, // searchCallbackProceedResultFalseValue
                null, // searchCallbackProceedResultTrueValue
                projectorFactory, // ← PKOnlyTupleProjectorFactory (extracts only PK fields)
                null, // tuplePartitionerFactory
                partitionsMap);

        this.queryFields = queryFields;
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.distanceFunctionFactory = distanceFunctionFactory;
        this.quantizerFactory = quantizerFactory;
        this.tupleFilterFactory = tupleFilterFactory;
        this.kMultiplier = kMultiplier;
        this.indexEpsilon = indexEpsilon;
        this.numPrimaryKeys = numPrimaryKeys;
        this.indexOnly = indexOnly;

        // Setup permuting tuple reference to extract query parameters
        if (queryFields != null && queryFields.length > 0) {
            queryParamsTuple = new PermutingFrameTupleReference();
            queryParamsTuple.setFieldPermutation(queryFields);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();

        // Create tuple filter from factory if available
        // This filter is pushed down to the cursor level for proper K counting
        if (tupleFilterFactory != null) {
            tupleFilter = tupleFilterFactory.createTupleFilter(ctx);
        }
    }

    @Override
    protected ISearchPredicate createSearchPredicate(IIndex index) {
        // Create simple marker predicate
        // The actual query vector is passed via IIndexAccessParameters in addAdditionalIndexAccessorParams()
        return new VTreeSearchPredicate();
    }

    // Field layout of the query-parameters tuple, positionally packed by the Asterix optimizer side:
    // [0]=query vector, [1]=K, [2]=metric, [3]=min_probe_fraction, [4]=k_multiplier. The metric (position 2)
    // is resolved during compilation and is not read at runtime here, so this method reads 0,1,3,4 and skips 2.
    private static final int QP_FIELD_QUERY_VECTOR = 0;
    private static final int QP_FIELD_K = 1;
    private static final int QP_FIELD_MIN_PROBE_FRACTION = 3;
    private static final int QP_FIELD_K_MULTIPLIER = 4;

    // Serialized ADM integer type tags, matching ATypeTag.{TINYINT,SMALLINT,INTEGER,BIGINT}.serialize() in
    // asterix-om. Hardcoded for the same reason as ADOUBLE_TYPE_TAG below: Hyracks cannot depend on
    // asterix-om, and the optimizer side that packs this tuple uses those types.
    private static final byte AINT8_TYPE_TAG = 1;
    private static final byte AINT16_TYPE_TAG = 2;
    private static final byte AINT32_TYPE_TAG = 3;
    private static final byte AINT64_TYPE_TAG = 4;

    @Override
    protected void resetSearchPredicate(int tupleIndex) {
        // Update queryParamsTuple to point to current input tuple
        if (queryParamsTuple != null) {
            queryParamsTuple.reset(accessor, tupleIndex);

            // Update predicate with current tuple reference
            // Following RTree pattern: predicate holds reference, updated per-tuple
            VTreeSearchPredicate vectorPred = (VTreeSearchPredicate) searchPred;
            vectorPred.setQueryTuple(queryParamsTuple);
            vectorPred.setQueryFieldIndex(QP_FIELD_QUERY_VECTOR);

            // EVERY slot below is set unconditionally, on every input row. The predicate instance is reused
            // across rows, so a slot left untouched silently inherits the previous row's value — which would
            // turn "0 means use the default" into "0 means whatever the previous row used".

            // K (+1 to skip the ADM type tag). 0 when the plan did not supply the slot.
            vectorPred.setK(queryFields.length > QP_FIELD_K ? readIntegerQueryParam(QP_FIELD_K) : 0);

            // min_probe_fraction (double, +1 to skip type tag), a fraction in (0, 1] of the epsilon-filtered
            // candidate clusters. setMinProbeFraction() maps <= 0 to its own default, so pass 0 when absent.
            vectorPred.setMinProbeFraction(queryFields.length > QP_FIELD_MIN_PROBE_FRACTION
                    ? DoublePointable.getDouble(queryParamsTuple.getFieldData(QP_FIELD_MIN_PROBE_FRACTION),
                            queryParamsTuple.getFieldStart(QP_FIELD_MIN_PROBE_FRACTION) + 1)
                    : 0.0);

            // k_multiplier: the per-query value, unless the session config compiler.vector.kmultiplier is set
            // (> 1), which currently wins.
            int queryKMultiplier = queryFields.length > QP_FIELD_K_MULTIPLIER
                    ? Math.max(1, readIntegerQueryParam(QP_FIELD_K_MULTIPLIER)) : 1;
            vectorPred.setKMultiplier(kMultiplier > 1 ? kMultiplier : queryKMultiplier);

            // Tuple filter for INCLUDE field predicates (e.g. year > 2000), applied at cursor level so that K
            // counts only passing tuples. null when this search has no pushed filter.
            vectorPred.setTupleFilter(tupleFilter);

            vectorPred.setEpsilon(indexEpsilon);
        }
    }

    /**
     * Read an integer query parameter, honouring its ADM type tag.
     * <p>
     * A fixed 4-byte {@code IntegerPointable} read is not safe on its own: a SQL++ integer literal is an
     * {@code AInt32} today ({@code LIMIT 3}), but a literal that does not fit in 32 bits, and any folded or
     * parameterised expression that types as BIGINT, arrives as an 8-byte {@code AInt64} — whose first four
     * big-endian bytes are 0 for every small value. That produced a silently wrong {@code k} (0) rather than
     * an error, so switch on the tag instead of assuming the width.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    private int readIntegerQueryParam(int fieldIndex) {
        byte[] data = queryParamsTuple.getFieldData(fieldIndex);
        int tagOffset = queryParamsTuple.getFieldStart(fieldIndex);
        int valueOffset = tagOffset + 1; // skip the ADM type tag
        byte typeTag = data[tagOffset];
        long value;
        switch (typeTag) {
            case AINT8_TYPE_TAG:
                value = data[valueOffset];
                break;
            case AINT16_TYPE_TAG:
                value = ShortPointable.getShort(data, valueOffset);
                break;
            case AINT32_TYPE_TAG:
                value = IntegerPointable.getInteger(data, valueOffset);
                break;
            case AINT64_TYPE_TAG:
                value = LongPointable.getLong(data, valueOffset);
                break;
            default:
                throw new IllegalStateException("Unexpected type tag " + typeTag + " for integer query parameter at "
                        + "position " + fieldIndex + " of the vector-search query-parameters tuple");
        }
        // Clamp rather than overflow: k / k_multiplier are counts used to size buffers.
        return (int) Math.max(Integer.MIN_VALUE, Math.min(Integer.MAX_VALUE, value));
    }

    /**
     * Serialized ADOUBLE type tag (value 12), matching {@code ATypeTag.DOUBLE.serialize()} in
     * {@code asterix-om}. Hardcoded here because Hyracks cannot depend on asterix-om; the Asterix
     * optimizer side that builds this operator chain also declares the corresponding algebra
     * variable as BuiltinType.ADOUBLE, so the on-wire convention stays in sync.
     */
    private static final byte ADOUBLE_TYPE_TAG = 12;

    @Override
    protected int getFieldCount(IIndex index) {
        // numPrimaryKeys is supplied by the descriptor from dataset metadata (no more hardcoded 1).
        // Index-only mode appends one extra ADOUBLE field per emitted tuple (the per-candidate
        // D(q,x) read from the cursor in writeTupleToOutput).
        return numPrimaryKeys + (indexOnly ? 1 : 0);
    }

    /**
     * Stash the cursor that produced the tuples for this input row so {@link #writeTupleToOutput}
     * can reach into it for the per-tuple {@code D(q,x)} in index-only mode. Cleared on exit so the
     * stash never outlives the call that owns the cursor.
     */
    @Override
    protected void writeSearchResults(int tupleIndex, IIndexCursor cursor) throws Exception {
        this.activeCursor = cursor;
        try {
            super.writeSearchResults(tupleIndex, cursor);
        } finally {
            this.activeCursor = null;
        }
    }

    /**
     * Two responsibilities:
     * <ol>
     *   <li>Always run the configured projector (the {@code PKOnlyTupleProjector}) so the PK bytes
     *       from the cursor tuple land in {@code tb} exactly as in the legacy path.</li>
     *   <li>In index-only mode, append the distance field after the PK bytes by reading
     *       {@code D(q,x)} from {@link #activeCursor} via {@link IVectorSearchCursor}. The distance
     *       is written as {@code [ADOUBLE type tag (1 byte), IEEE-754 double (8 bytes)]} to match the
     *       algebra-side variable type ({@code BuiltinType.ADOUBLE}). Search always uses the pruned top-K
     *       cursor (the only {@link IVectorSearchCursor}), so the cast is safe.</li>
     * </ol>
     *
     * <p>A genuine {@link Double#NaN} distance (e.g. a zero-magnitude vector under cosine) is a real value
     * and flows through; it is not treated as an error. {@code getCurrentDistance()} is valid for the current
     * tuple both before and after projection (projection does not advance the cursor).
     */
    @Override
    protected ITupleReference writeTupleToOutput(ITupleReference tuple) throws IOException {
        double dqx = Double.NaN;
        if (indexOnly) {
            // Search always uses the pruned top-K cursor (addAdditionalIndexAccessorParams sets
            // USE_TOPK_SEARCH), which is the only IVectorSearchCursor; the streaming LSMVTreeSearchCursor
            // serves merges, full scans, and tests and never backs an index-only plan. A genuine NaN here
            // (e.g. the cosine distance of a zero-magnitude vector) is a real value and flows through.
            dqx = ((IVectorSearchCursor) activeCursor).getCurrentDistance();
        }
        ITupleReference projected = tupleProjector.project(tuple, dos, tb);
        if (projected == null) {
            return null;
        }
        if (indexOnly) {
            dos.writeByte(ADOUBLE_TYPE_TAG);
            dos.writeDouble(dqx);
            tb.addFieldEndOffset();
        }
        return projected;
    }

    @Override
    protected void addAdditionalIndexAccessorParams(IIndexAccessParameters iap) {
        // Vector accessor factory: storage layer uses this to extract the query vector from the
        // search predicate's tuple, keeping the extraction in the storage layer (no AsterixDB types
        // leak down).
        iap.getParameters().put(IVTreeBinaryAccessorFactory.IAP_KEY, vectorAccessorFactory);

        // Distance-function factory injected from AsterixDB so the VTree can build an
        // IVTreeDistanceFunction without depending on asterix-runtime types.
        iap.getParameters().put(IVTreeDistanceFunctionFactory.IAP_KEY, distanceFunctionFactory);

        // Quantizer factory (nullable). The VTree builds a per-query IVTreeQuantizer from the
        // float[6] params persisted on the index. Null for non-quantized indexes and for test
        // contexts that inject a pre-built IVTreeQuantizer under IVTreeQuantizer.IAP_KEY.
        if (quantizerFactory != null) {
            iap.getParameters().put(IVTreeQuantizerFactory.IAP_KEY, quantizerFactory);
        }

        // Task context for the spillable top-K buffer (follows inverted-index pattern).
        iap.getParameters().put(HyracksConstants.HYRACKS_TASK_CONTEXT, ctx);

        // Production ANN search always uses the quantized top-K cursor. Without this flag,
        // LSMVTreeIndexAccessor defaults to the streaming LSMVTreeSearchCursor (used by component
        // merges and by test fixtures that verify through full-scan iteration).
        iap.getParameters().put(LSMVTreeTopKSearchCursor.IAP_KEY, Boolean.TRUE);
    }
}
