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
package org.apache.asterix.runtime.aggregates.serializable.std;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.common.AccessibleByteArrayEval;
import org.apache.asterix.runtime.evaluators.common.ClosedRecordConstructorEvalFactory.ClosedRecordConstructorEval;
import org.apache.asterix.runtime.exceptions.IncompatibleTypeException;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractSerializableAvgAggregateFunction extends AbstractSerializableAggregateFunction {
    private static final int SUM_FIELD_ID = 0;
    private static final int COUNT_FIELD_ID = 1;

    private static final int SUM_OFFSET = 0;
    private static final int COUNT_OFFSET = 8;
    protected static final int AGG_TYPE_OFFSET = 16;

    private IPointable inputVal = new VoidPointable();
    private IScalarEvaluator eval;
    private AMutableDouble aDouble = new AMutableDouble(0);
    private AMutableInt64 aInt64 = new AMutableInt64(0);

    private IPointable avgBytes = new VoidPointable();
    private ByteArrayAccessibleOutputStream sumBytes = new ByteArrayAccessibleOutputStream();
    private DataOutput sumBytesOutput = new DataOutputStream(sumBytes);
    private ByteArrayAccessibleOutputStream countBytes = new ByteArrayAccessibleOutputStream();
    private DataOutput countBytesOutput = new DataOutputStream(countBytes);
    private IScalarEvaluator evalSum = new AccessibleByteArrayEval(sumBytes);
    private IScalarEvaluator evalCount = new AccessibleByteArrayEval(countBytes);
    private ClosedRecordConstructorEval recordEval;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt64> longSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ANull> nullSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);

    public AbstractSerializableAvgAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        eval = args[0].createScalarEvaluator(context);
    }

    @Override
    public void init(DataOutput state) throws HyracksDataException {
        try {
            state.writeDouble(0.0);
            state.writeLong(0);
            state.writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public abstract void step(IFrameTupleReference tuple, byte[] state, int start, int len) throws HyracksDataException;

    @Override
    public abstract void finish(byte[] state, int start, int len, DataOutput result) throws HyracksDataException;

    @Override
    public abstract void finishPartial(byte[] state, int start, int len, DataOutput result) throws HyracksDataException;

    protected abstract void processNull(byte[] state, int start);

    protected void processDataValues(IFrameTupleReference tuple, byte[] state, int start, int len)
            throws HyracksDataException {
        if (skipStep(state, start)) {
            return;
        }
        eval.evaluate(tuple, inputVal);
        byte[] bytes = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();

        double sum = BufferSerDeUtil.getDouble(state, start + SUM_OFFSET);
        long count = BufferSerDeUtil.getLong(state, start + COUNT_OFFSET);
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
        ATypeTag aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);
        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
            processNull(state, start);
            return;
        } else if (aggType == ATypeTag.SYSTEM_NULL) {
            aggType = typeTag;
        } else if (typeTag != ATypeTag.SYSTEM_NULL && !ATypeHierarchy.isCompatible(typeTag, aggType)) {
            if (typeTag.ordinal() > aggType.ordinal()) {
                throw new IncompatibleTypeException(sourceLoc, BuiltinFunctions.AVG, bytes[offset],
                        aggType.serialize());
            } else {
                throw new IncompatibleTypeException(sourceLoc, BuiltinFunctions.AVG, aggType.serialize(),
                        bytes[offset]);
            }
        } else if (ATypeHierarchy.canPromote(aggType, typeTag)) {
            aggType = typeTag;
        }
        ++count;
        switch (typeTag) {
            case TINYINT: {
                byte val = AInt8SerializerDeserializer.getByte(bytes, offset + 1);
                sum += val;
                break;
            }
            case SMALLINT: {
                short val = AInt16SerializerDeserializer.getShort(bytes, offset + 1);
                sum += val;
                break;
            }
            case INTEGER: {
                int val = AInt32SerializerDeserializer.getInt(bytes, offset + 1);
                sum += val;
                break;
            }
            case BIGINT: {
                long val = AInt64SerializerDeserializer.getLong(bytes, offset + 1);
                sum += val;
                break;
            }
            case FLOAT: {
                float val = AFloatSerializerDeserializer.getFloat(bytes, offset + 1);
                sum += val;
                break;
            }
            case DOUBLE: {
                double val = ADoubleSerializerDeserializer.getDouble(bytes, offset + 1);
                sum += val;
                break;
            }
            default:
                throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.AVG, bytes[offset]);
        }
        BufferSerDeUtil.writeDouble(sum, state, start + SUM_OFFSET);
        BufferSerDeUtil.writeLong(count, state, start + COUNT_OFFSET);
        state[start + AGG_TYPE_OFFSET] = aggType.serialize();
    }

    protected void finishPartialResults(byte[] state, int start, int len, DataOutput result)
            throws HyracksDataException {
        double sum = BufferSerDeUtil.getDouble(state, start + SUM_OFFSET);
        long count = BufferSerDeUtil.getLong(state, start + COUNT_OFFSET);
        ATypeTag aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);
        if (recordEval == null) {
            ARecordType recType = new ARecordType(null, new String[] { "sum", "count" },
                    new IAType[] { BuiltinType.ADOUBLE, BuiltinType.AINT64 }, false);
            recordEval = new ClosedRecordConstructorEval(recType, new IScalarEvaluator[] { evalSum, evalCount });
        }

        try {
            if (aggType == ATypeTag.SYSTEM_NULL) {
                result.writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
            } else if (aggType == ATypeTag.NULL) {
                result.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
            } else {
                sumBytes.reset();
                aDouble.setValue(sum);
                doubleSerde.serialize(aDouble, sumBytesOutput);
                countBytes.reset();
                aInt64.setValue(count);
                longSerde.serialize(aInt64, countBytesOutput);
                recordEval.evaluate(null, avgBytes);
                result.write(avgBytes.getByteArray(), avgBytes.getStartOffset(), avgBytes.getLength());
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void processPartialResults(IFrameTupleReference tuple, byte[] state, int start, int len)
            throws HyracksDataException {
        if (skipStep(state, start)) {
            return;
        }
        double sum = BufferSerDeUtil.getDouble(state, start + SUM_OFFSET);
        long count = BufferSerDeUtil.getLong(state, start + COUNT_OFFSET);
        eval.evaluate(tuple, inputVal);
        byte[] serBytes = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();

        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serBytes[offset]);
        switch (typeTag) {
            case NULL: {
                processNull(state, start);
                break;
            }
            case SYSTEM_NULL: {
                // Ignore and return.
                break;
            }
            case OBJECT: {
                // Expected.
                ATypeTag aggType = ATypeTag.DOUBLE;
                int nullBitmapSize = 0;
                int offset1 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, SUM_FIELD_ID,
                        nullBitmapSize, false);
                sum += ADoubleSerializerDeserializer.getDouble(serBytes, offset1);
                int offset2 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, COUNT_FIELD_ID,
                        nullBitmapSize, false);
                count += AInt64SerializerDeserializer.getLong(serBytes, offset2);

                BufferSerDeUtil.writeDouble(sum, state, start + SUM_OFFSET);
                BufferSerDeUtil.writeLong(count, state, start + COUNT_OFFSET);
                state[start + AGG_TYPE_OFFSET] = aggType.serialize();
                break;
            }
            default:
                throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.AVG, serBytes[offset]);
        }
    }

    protected void finishFinalResults(byte[] state, int start, int len, DataOutput result) throws HyracksDataException {
        double sum = BufferSerDeUtil.getDouble(state, start + SUM_OFFSET);
        long count = BufferSerDeUtil.getLong(state, start + COUNT_OFFSET);
        ATypeTag aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);
        try {
            if (count == 0 || aggType == ATypeTag.NULL) {
                nullSerde.serialize(ANull.NULL, result);
            } else {
                aDouble.setValue(sum / count);
                doubleSerde.serialize(aDouble, result);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected boolean skipStep(byte[] state, int start) {
        return false;
    }

}
