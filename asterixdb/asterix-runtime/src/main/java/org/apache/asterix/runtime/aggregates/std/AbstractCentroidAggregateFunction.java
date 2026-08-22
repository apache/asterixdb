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
package org.apache.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.common.AccessibleByteArrayEval;
import org.apache.asterix.runtime.evaluators.common.ClosedRecordConstructorEvalFactory.ClosedRecordConstructorEval;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Shared logic for the CLUSTER BY {@code CENTROID} aggregate family: accumulates an element-wise vector sum and a
 * count, and averages them into a centroid. Two-step (local/global) distributed aggregation follows AVG exactly
 * ({@link AbstractAvgAggregateFunction}): the partial is a closed record {@code {sum, count}} built with
 * {@link ClosedRecordConstructorEval} and read back with {@link ARecordSerializerDeserializer#getFieldOffsetById},
 * except {@code sum} is a {@code [double]} list field instead of a scalar. One wrinkle: a closed record strips the
 * outer type tag of a list field on store, so {@code sum} is placed last and its tag is prepended back before
 * reading (see {@link #processPartialResults}). v1 supports DOUBLE vectors only.
 */
public abstract class AbstractCentroidAggregateFunction extends AbstractAggregateFunction {

    // sum is placed LAST so its serialized field length is (recordEnd - sumFieldOffset); see processPartialResults.
    private static final int COUNT_FIELD_ID = 0;
    private static final int SUM_FIELD_ID = 1;

    private final IEvaluatorContext context;
    private boolean isWarned;

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final IPointable inputVal = new VoidPointable();
    private final IScalarEvaluator eval;

    protected ATypeTag aggType;
    private double[] sum;
    // One row's components, committed to sum only once every component has decoded.
    private double[] scratch;
    private int dim;
    private long count;

    // Reading list items (input vector or the partial's sum-list field).
    private final ListAccessor listAccessor = new ListAccessor();
    private final IPointable itemVal = new VoidPointable();
    private final ArrayBackedValueStorage itemStorage = new ArrayBackedValueStorage();
    // Rebuilds a tagged ordered list from the partial's tag-stripped sum field before reading it.
    private final ArrayBackedValueStorage listReconstruct = new ArrayBackedValueStorage();

    // Building an ordered list of doubles (partial sum field / final centroid).
    // Typed [double] list: matches the declared centroid output type; items stored untagged (fixed-size).
    private final AOrderedListType doubleListType = new AOrderedListType(BuiltinType.ADOUBLE, null);
    private final OrderedListBuilder listBuilder = new OrderedListBuilder();
    private final ArrayBackedValueStorage itemOut = new ArrayBackedValueStorage();
    private final AMutableDouble aDouble = new AMutableDouble(0);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    // Partial record {sum:[double], count:int64} machinery (mirrors AbstractAvgAggregateFunction).
    private final ARecordType recType;
    private final IPointable recordResult = new VoidPointable();
    private final ByteArrayAccessibleOutputStream sumBytes = new ByteArrayAccessibleOutputStream();
    private final DataOutput sumBytesOutput = new DataOutputStream(sumBytes);
    private final ByteArrayAccessibleOutputStream countBytes = new ByteArrayAccessibleOutputStream();
    private final DataOutput countBytesOutput = new DataOutputStream(countBytes);
    private final IScalarEvaluator evalSum = new AccessibleByteArrayEval(sumBytes);
    private final IScalarEvaluator evalCount = new AccessibleByteArrayEval(countBytes);
    private final ClosedRecordConstructorEval recordEval;
    private final AMutableInt64 aInt64 = new AMutableInt64(0);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt64> longSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

    public AbstractCentroidAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        this.context = context;
        this.eval = args[0].createScalarEvaluator(context);
        // count first, then sum (an ANY list field placed last so its length is derivable on read).
        this.recType = new ARecordType(null, new String[] { "count", "sum" },
                new IAType[] { BuiltinType.AINT64, BuiltinType.ANY }, false);
        this.recordEval = new ClosedRecordConstructorEval(recType, new IScalarEvaluator[] { evalCount, evalSum });
    }

    @Override
    public void init() throws HyracksDataException {
        aggType = ATypeTag.SYSTEM_NULL;
        sum = null;
        dim = -1;
        count = 0;
        isWarned = false;
    }

    @Override
    public abstract void step(IFrameTupleReference tuple) throws HyracksDataException;

    @Override
    public abstract void finish(IPointable result) throws HyracksDataException;

    @Override
    public abstract void finishPartial(IPointable result) throws HyracksDataException;

    protected abstract void processNull();

    /** LOCAL raw step: the input is a vector (ordered list of doubles). Accumulate element-wise; count++. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    protected void processDataValues(IFrameTupleReference tuple) throws HyracksDataException {
        if (skipStep()) {
            return;
        }
        eval.evaluate(tuple, inputVal);
        byte[] data = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
            processNull();
            return;
        }
        if (!typeTag.isListType()) {
            warnOnce(typeTag);
            processNull();
            return;
        }
        listAccessor.reset(data, offset);
        int size = listAccessor.size();
        if (!ensureDim(size)) {
            warnOnce(typeTag);
            processNull();
            return;
        }
        try {
            for (int i = 0; i < size; i++) {
                listAccessor.getOrWriteItem(i, itemVal, itemStorage);
                byte[] ib = itemVal.getByteArray();
                int io = itemVal.getStartOffset();
                if (EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(ib[io]) != ATypeTag.DOUBLE) {
                    // Nothing of this row has reached sum: a rejected row leaves the running sum untouched.
                    warnOnce(ATypeTag.DOUBLE);
                    processNull();
                    return;
                }
                scratch[i] = ADoubleSerializerDeserializer.getDouble(ib, io + 1);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        for (int i = 0; i < size; i++) {
            sum[i] += scratch[i];
        }
        count++;
        aggType = ATypeTag.DOUBLE;
    }

    /** Emit the partial as a record {sum:[double], count:int64}. */
    protected void finishPartialResults(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            if (aggType == ATypeTag.SYSTEM_NULL) {
                resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
                result.set(resultStorage);
            } else if (aggType == ATypeTag.NULL || sum == null) {
                resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                result.set(resultStorage);
            } else {
                // sum field: a [double] list (its outer type tag is stripped when stored as a closed-record field).
                sumBytes.reset();
                listBuilder.reset(doubleListType);
                for (int i = 0; i < dim; i++) {
                    addDouble(sum[i]);
                }
                listBuilder.write(sumBytesOutput, true);
                // count field: untagged int64.
                countBytes.reset();
                aInt64.setValue(count);
                longSerde.serialize(aInt64, countBytesOutput);
                recordEval.evaluate(null, recordResult);
                result.set(recordResult);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /** MERGE step: the input is a partial record {sum:[double], count:int64}. */
    protected void processPartialResults(IFrameTupleReference tuple) throws HyracksDataException {
        if (skipStep()) {
            return;
        }
        eval.evaluate(tuple, inputVal);
        byte[] serBytes = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serBytes[offset]);
        if (typeTag == ATypeTag.NULL) {
            processNull();
            return;
        }
        if (typeTag == ATypeTag.SYSTEM_NULL) {
            return;
        }
        if (typeTag != ATypeTag.OBJECT) {
            throw new UnsupportedItemTypeException(sourceLoc, getIdentifier().getName(), serBytes[offset]);
        }
        try {
            // count field (untagged int64).
            int countOff = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, COUNT_FIELD_ID, 0, false);
            long partialCount = AInt64SerializerDeserializer.getLong(serBytes, countOff);
            // sum field: an ordered list stored WITHOUT its outer type tag (the closed-record field strips it).
            // sum is the last field, so its bytes run from sumOff to the record end; prepend the ORDEREDLIST tag
            // to reconstruct a self-describing list that ListAccessor can read.
            int sumOff = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, SUM_FIELD_ID, 0, false);
            int recEnd = offset + AInt32SerializerDeserializer.getInt(serBytes, offset + 1);
            int sumLen = recEnd - sumOff;
            listReconstruct.reset();
            listReconstruct.getDataOutput().writeByte(ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG);
            listReconstruct.getDataOutput().write(serBytes, sumOff, sumLen);
            listAccessor.reset(listReconstruct.getByteArray(), 0);
            int size = listAccessor.size();
            if (!ensureDim(size)) {
                processNull();
                return;
            }
            count += partialCount;
            for (int i = 0; i < size; i++) {
                listAccessor.getOrWriteItem(i, itemVal, itemStorage);
                sum[i] += readDouble(itemVal);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        aggType = ATypeTag.DOUBLE;
    }

    /** Final: centroid = sum / count, as an ordered list of doubles; NULL when the cluster is empty. */
    protected void finishFinalResults(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            if (count == 0 || aggType == ATypeTag.NULL || sum == null) {
                resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
            } else {
                listBuilder.reset(doubleListType);
                for (int i = 0; i < dim; i++) {
                    addDouble(sum[i] / count);
                }
                listBuilder.write(resultStorage.getDataOutput(), true);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    protected boolean skipStep() {
        return false;
    }

    /** Fix the vector dimension on first sight; reject a later mismatch. Returns false for an empty/invalid vector. */
    private boolean ensureDim(int d) {
        if (dim < 0) {
            dim = d;
            sum = new double[d];
            scratch = new double[d];
            return d > 0;
        }
        return d == dim;
    }

    private void addDouble(double v) throws IOException {
        // doubleSerde (from SerializerDeserializerProvider) already writes the DOUBLE type tag, so no manual tag.
        itemOut.reset();
        aDouble.setValue(v);
        doubleSerde.serialize(aDouble, itemOut.getDataOutput());
        listBuilder.addItem(itemOut);
    }

    private double readDouble(IPointable p) {
        return ADoubleSerializerDeserializer.getDouble(p.getByteArray(), p.getStartOffset() + 1);
    }

    private void warnOnce(ATypeTag typeTag) {
        if (!isWarned) {
            isWarned = true;
            ExceptionUtil.warnUnsupportedType(context, sourceLoc, getIdentifier().getName(), typeTag);
        }
    }

    private FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.CENTROID;
    }
}
