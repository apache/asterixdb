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
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.aggregates.utils.SingleVarFunctionsUtil;
import org.apache.asterix.runtime.evaluators.common.AccessibleByteArrayEval;
import org.apache.asterix.runtime.evaluators.common.ClosedRecordConstructorEvalFactory.ClosedRecordConstructorEval;
import org.apache.asterix.runtime.exceptions.IncompatibleTypeException;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
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

public abstract class AbstractSerializableSingleVariableStatisticsAggregateFunction
        extends AbstractSerializableAggregateFunction {

    /*
    M1, M2, M3 and M4 are the 1st to 4th central moment of a data sample
     */
    private static final int M1_FIELD_ID = 0;
    private static final int M2_FIELD_ID = 1;
    private static final int M3_FIELD_ID = 2;
    private static final int M4_FIELD_ID = 3;
    private static final int COUNT_FIELD_ID = 4;

    private static final int M1_OFFSET = 0;
    private static final int M2_OFFSET = 8;
    private static final int M3_OFFSET = 16;
    private static final int M4_OFFSET = 24;
    private static final int COUNT_OFFSET = 32;
    protected static final int AGG_TYPE_OFFSET = 40;

    private IPointable inputVal = new VoidPointable();
    private IScalarEvaluator eval;
    private AMutableDouble aDouble = new AMutableDouble(0);
    private AMutableInt64 aInt64 = new AMutableInt64(0);
    private SingleVarFunctionsUtil moments = new SingleVarFunctionsUtil();

    private IPointable resultBytes = new VoidPointable();
    private ByteArrayAccessibleOutputStream m1Bytes = new ByteArrayAccessibleOutputStream();
    private DataOutput m1BytesOutput = new DataOutputStream(m1Bytes);
    private ByteArrayAccessibleOutputStream m2Bytes = new ByteArrayAccessibleOutputStream();
    private DataOutput m2BytesOutput = new DataOutputStream(m2Bytes);
    private ByteArrayAccessibleOutputStream m3Bytes = new ByteArrayAccessibleOutputStream();
    private DataOutput m3BytesOutput = new DataOutputStream(m3Bytes);
    private ByteArrayAccessibleOutputStream m4Bytes = new ByteArrayAccessibleOutputStream();
    private DataOutput m4BytesOutput = new DataOutputStream(m4Bytes);
    private ByteArrayAccessibleOutputStream countBytes = new ByteArrayAccessibleOutputStream();
    private DataOutput countBytesOutput = new DataOutputStream(countBytes);
    private IScalarEvaluator evalM1 = new AccessibleByteArrayEval(m1Bytes);
    private IScalarEvaluator evalM2 = new AccessibleByteArrayEval(m2Bytes);
    private IScalarEvaluator evalM3 = new AccessibleByteArrayEval(m3Bytes);
    private IScalarEvaluator evalM4 = new AccessibleByteArrayEval(m4Bytes);
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

    public AbstractSerializableSingleVariableStatisticsAggregateFunction(IScalarEvaluatorFactory[] args,
            IEvaluatorContext context, SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        eval = args[0].createScalarEvaluator(context);
    }

    @Override
    public void init(DataOutput state) throws HyracksDataException {
        try {
            state.writeDouble(0.0);
            state.writeDouble(0.0);
            state.writeDouble(0.0);
            state.writeDouble(0.0);
            state.writeLong(0L);
            state.writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
            moments.set(0, 0, 0, 0, 0, getM3Flag(), getM4Flag());
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

    protected abstract FunctionIdentifier getFunctionIdentifier();

    protected abstract boolean getM3Flag();

    protected abstract boolean getM4Flag();

    protected void processDataValues(IFrameTupleReference tuple, byte[] state, int start, int len)
            throws HyracksDataException {
        if (skipStep(state, start)) {
            return;
        }
        eval.evaluate(tuple, inputVal);
        byte[] bytes = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();

        double m1 = BufferSerDeUtil.getDouble(state, start + M1_OFFSET);
        double m2 = BufferSerDeUtil.getDouble(state, start + M2_OFFSET);
        double m3 = BufferSerDeUtil.getDouble(state, start + M3_OFFSET);
        double m4 = BufferSerDeUtil.getDouble(state, start + M4_OFFSET);
        long count = BufferSerDeUtil.getLong(state, start + COUNT_OFFSET);
        moments.set(m1, m2, m3, m4, count, getM3Flag(), getM4Flag());

        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
        ATypeTag aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);
        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
            processNull(state, start);
            return;
        } else if (aggType == ATypeTag.SYSTEM_NULL) {
            aggType = typeTag;
        } else if (typeTag != ATypeTag.SYSTEM_NULL && !ATypeHierarchy.isCompatible(typeTag, aggType)) {
            if (typeTag.ordinal() > aggType.ordinal()) {
                throw new IncompatibleTypeException(sourceLoc, getFunctionIdentifier(), bytes[offset],
                        aggType.serialize());
            } else {
                throw new IncompatibleTypeException(sourceLoc, getFunctionIdentifier(), aggType.serialize(),
                        bytes[offset]);
            }
        } else if (ATypeHierarchy.canPromote(aggType, typeTag)) {
            aggType = typeTag;
        }
        double val;
        switch (typeTag) {
            case TINYINT:
                val = AInt8SerializerDeserializer.getByte(bytes, offset + 1);
                moments.push(val);
                break;
            case SMALLINT:
                val = AInt16SerializerDeserializer.getShort(bytes, offset + 1);
                moments.push(val);
                break;
            case INTEGER:
                val = AInt32SerializerDeserializer.getInt(bytes, offset + 1);
                moments.push(val);
                break;
            case BIGINT:
                val = AInt64SerializerDeserializer.getLong(bytes, offset + 1);
                moments.push(val);
                break;
            case FLOAT:
                val = AFloatSerializerDeserializer.getFloat(bytes, offset + 1);
                moments.push(val);
                break;
            case DOUBLE:
                val = ADoubleSerializerDeserializer.getDouble(bytes, offset + 1);
                moments.push(val);
                break;
            default:
                throw new UnsupportedItemTypeException(sourceLoc, getFunctionIdentifier(), bytes[offset]);
        }
        BufferSerDeUtil.writeDouble(moments.getM1(), state, start + M1_OFFSET);
        BufferSerDeUtil.writeDouble(moments.getM2(), state, start + M2_OFFSET);
        BufferSerDeUtil.writeDouble(moments.getM3(), state, start + M3_OFFSET);
        BufferSerDeUtil.writeDouble(moments.getM4(), state, start + M4_OFFSET);
        BufferSerDeUtil.writeLong(moments.getCount(), state, start + COUNT_OFFSET);
        state[start + AGG_TYPE_OFFSET] = aggType.serialize();
    }

    protected void finishPartialResults(byte[] state, int start, int len, DataOutput result)
            throws HyracksDataException {
        double m1 = BufferSerDeUtil.getDouble(state, start + M1_OFFSET);
        double m2 = BufferSerDeUtil.getDouble(state, start + M2_OFFSET);
        double m3 = BufferSerDeUtil.getDouble(state, start + M3_OFFSET);
        double m4 = BufferSerDeUtil.getDouble(state, start + M4_OFFSET);
        long count = BufferSerDeUtil.getLong(state, start + COUNT_OFFSET);
        ATypeTag aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);
        if (recordEval == null) {
            ARecordType recType =
                    new ARecordType(null,
                            new String[] { "m1", "m2", "m3", "m4", "count" }, new IAType[] { BuiltinType.ADOUBLE,
                                    BuiltinType.ADOUBLE, BuiltinType.ADOUBLE, BuiltinType.ADOUBLE, BuiltinType.AINT64 },
                            false);
            recordEval = new ClosedRecordConstructorEval(recType,
                    new IScalarEvaluator[] { evalM1, evalM2, evalM3, evalM4, evalCount });
        }

        try {
            if (aggType == ATypeTag.SYSTEM_NULL) {
                result.writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
            } else if (aggType == ATypeTag.NULL) {
                result.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
            } else {
                m1Bytes.reset();
                aDouble.setValue(m1);
                doubleSerde.serialize(aDouble, m1BytesOutput);
                m2Bytes.reset();
                aDouble.setValue(m2);
                doubleSerde.serialize(aDouble, m2BytesOutput);
                m3Bytes.reset();
                aDouble.setValue(m3);
                doubleSerde.serialize(aDouble, m3BytesOutput);
                m4Bytes.reset();
                aDouble.setValue(m4);
                doubleSerde.serialize(aDouble, m4BytesOutput);
                countBytes.reset();
                aInt64.setValue(count);
                longSerde.serialize(aInt64, countBytesOutput);
                recordEval.evaluate(null, resultBytes);
                result.write(resultBytes.getByteArray(), resultBytes.getStartOffset(), resultBytes.getLength());
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
        double m1 = BufferSerDeUtil.getDouble(state, start + M1_OFFSET);
        double m2 = BufferSerDeUtil.getDouble(state, start + M2_OFFSET);
        double m3 = BufferSerDeUtil.getDouble(state, start + M3_OFFSET);
        double m4 = BufferSerDeUtil.getDouble(state, start + M4_OFFSET);
        long count = BufferSerDeUtil.getLong(state, start + COUNT_OFFSET);
        moments.set(m1, m2, m3, m4, count, getM3Flag(), getM4Flag());

        eval.evaluate(tuple, inputVal);
        byte[] serBytes = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();

        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serBytes[offset]);
        switch (typeTag) {
            case NULL:
                processNull(state, start);
                break;
            case SYSTEM_NULL:
                // Ignore and return.
                break;
            case OBJECT:
                // Expected.
                ATypeTag aggType = ATypeTag.DOUBLE;
                int nullBitmapSize = 0;
                int offset1 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, M1_FIELD_ID,
                        nullBitmapSize, false);
                int offset2 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, M2_FIELD_ID,
                        nullBitmapSize, false);
                int offset3 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, M3_FIELD_ID,
                        nullBitmapSize, false);
                int offset4 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, M4_FIELD_ID,
                        nullBitmapSize, false);
                int offset5 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, COUNT_FIELD_ID,
                        nullBitmapSize, false);
                double temp_m1 = ADoubleSerializerDeserializer.getDouble(serBytes, offset1);
                double temp_m2 = ADoubleSerializerDeserializer.getDouble(serBytes, offset2);
                double temp_m3 = ADoubleSerializerDeserializer.getDouble(serBytes, offset3);
                double temp_m4 = ADoubleSerializerDeserializer.getDouble(serBytes, offset4);
                long temp_count = AInt64SerializerDeserializer.getLong(serBytes, offset5);
                moments.combine(temp_m1, temp_m2, temp_m3, temp_m4, temp_count);

                BufferSerDeUtil.writeDouble(moments.getM1(), state, start + M1_OFFSET);
                BufferSerDeUtil.writeDouble(moments.getM2(), state, start + M2_OFFSET);
                BufferSerDeUtil.writeDouble(moments.getM3(), state, start + M3_OFFSET);
                BufferSerDeUtil.writeDouble(moments.getM4(), state, start + M4_OFFSET);
                BufferSerDeUtil.writeLong(moments.getCount(), state, start + COUNT_OFFSET);
                state[start + AGG_TYPE_OFFSET] = aggType.serialize();
                break;
            default:
                throw new UnsupportedItemTypeException(sourceLoc, getFunctionIdentifier(), serBytes[offset]);
        }
    }

    protected void finishStddevFinalResults(byte[] state, int start, int len, DataOutput result, int delta)
            throws HyracksDataException {
        double m2 = BufferSerDeUtil.getDouble(state, start + M2_OFFSET);
        long count = BufferSerDeUtil.getLong(state, start + COUNT_OFFSET);
        ATypeTag aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);
        try {
            if (count <= 1 || aggType == ATypeTag.NULL) {
                nullSerde.serialize(ANull.NULL, result);
            } else {
                aDouble.setValue(Math.sqrt(m2 / (count - delta)));
                doubleSerde.serialize(aDouble, result);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void finishVarFinalResults(byte[] state, int start, int len, DataOutput result, int delta)
            throws HyracksDataException {
        double m2 = BufferSerDeUtil.getDouble(state, start + M2_OFFSET);
        long count = BufferSerDeUtil.getLong(state, start + COUNT_OFFSET);
        ATypeTag aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);
        try {
            if (count <= 1 || aggType == ATypeTag.NULL) {
                nullSerde.serialize(ANull.NULL, result);
            } else {
                aDouble.setValue(m2 / (count - delta));
                doubleSerde.serialize(aDouble, result);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void finishSkewFinalResults(byte[] state, int start, int len, DataOutput result)
            throws HyracksDataException {
        double m2 = BufferSerDeUtil.getDouble(state, start + M2_OFFSET);
        double m3 = BufferSerDeUtil.getDouble(state, start + M3_OFFSET);
        long count = BufferSerDeUtil.getLong(state, start + COUNT_OFFSET);
        ATypeTag aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);
        try {
            if (count <= 1 || aggType == ATypeTag.NULL || (m2 < Double.MIN_VALUE && m2 > -Double.MIN_VALUE)) {
                nullSerde.serialize(ANull.NULL, result);
            } else {
                aDouble.setValue(Math.sqrt(count) * m3 / Math.pow(m2, 1.5));
                doubleSerde.serialize(aDouble, result);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void finishKurtFinalResults(byte[] state, int start, int len, DataOutput result)
            throws HyracksDataException {
        double m2 = BufferSerDeUtil.getDouble(state, start + M2_OFFSET);
        double m4 = BufferSerDeUtil.getDouble(state, start + M4_OFFSET);
        long count = BufferSerDeUtil.getLong(state, start + COUNT_OFFSET);
        ATypeTag aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);
        try {
            if (count <= 1 || aggType == ATypeTag.NULL || (m2 < Double.MIN_VALUE && m2 > -Double.MIN_VALUE)) {
                nullSerde.serialize(ANull.NULL, result);
            } else {
                aDouble.setValue(m4 * count / (m2 * m2 - 3));
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
