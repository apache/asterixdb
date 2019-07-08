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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractSingleVarStatisticsAggregateFunction extends AbstractAggregateFunction {

    /*
    M1 and M2 are the 1st and 2nd central moment of a data sample
     */
    private static final int M1_FIELD_ID = 0;
    private static final int M2_FIELD_ID = 1;
    private static final int M3_FIELD_ID = 2;
    private static final int M4_FIELD_ID = 3;
    private static final int COUNT_FIELD_ID = 4;

    private final ARecordType recType;

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private IPointable inputVal = new VoidPointable();
    private IScalarEvaluator eval;
    protected ATypeTag aggType;
    private SingleVarFunctionsUtil moments = new SingleVarFunctionsUtil();
    private AMutableDouble aDouble = new AMutableDouble(0);
    private AMutableInt64 aInt64 = new AMutableInt64(0);

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

    public AbstractSingleVarStatisticsAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        eval = args[0].createScalarEvaluator(context);
        recType =
                new ARecordType(null,
                        new String[] { "m1", "m2", "m3", "m4", "count" }, new IAType[] { BuiltinType.ADOUBLE,
                                BuiltinType.ADOUBLE, BuiltinType.ADOUBLE, BuiltinType.ADOUBLE, BuiltinType.AINT64 },
                        false);
        recordEval = new ClosedRecordConstructorEval(recType,
                new IScalarEvaluator[] { evalM1, evalM2, evalM3, evalM4, evalCount });
    }

    @Override
    public void init() throws HyracksDataException {
        aggType = ATypeTag.SYSTEM_NULL;
        moments.set(0, 0, 0, 0, 0, getM3Flag(), getM4Flag());
    }

    @Override
    public abstract void step(IFrameTupleReference tuple) throws HyracksDataException;

    @Override
    public abstract void finish(IPointable result) throws HyracksDataException;

    @Override
    public abstract void finishPartial(IPointable result) throws HyracksDataException;

    protected abstract FunctionIdentifier getFunctionIdentifier();

    protected abstract void processNull();

    protected abstract boolean getM3Flag();

    protected abstract boolean getM4Flag();

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
        } else if (aggType == ATypeTag.SYSTEM_NULL) {
            aggType = typeTag;
        } else if (typeTag != ATypeTag.SYSTEM_NULL && !ATypeHierarchy.isCompatible(typeTag, aggType)) {
            if (typeTag.ordinal() > aggType.ordinal()) {
                throw new IncompatibleTypeException(sourceLoc, getFunctionIdentifier(), data[offset],
                        aggType.serialize());
            } else {
                throw new IncompatibleTypeException(sourceLoc, getFunctionIdentifier(), aggType.serialize(),
                        data[offset]);
            }
        } else if (ATypeHierarchy.canPromote(aggType, typeTag)) {
            aggType = typeTag;
        }
        double val;
        switch (typeTag) {
            case TINYINT:
                val = AInt8SerializerDeserializer.getByte(data, offset + 1);
                moments.push(val);
                break;
            case SMALLINT:
                val = AInt16SerializerDeserializer.getShort(data, offset + 1);
                moments.push(val);
                break;
            case INTEGER:
                val = AInt32SerializerDeserializer.getInt(data, offset + 1);
                moments.push(val);
                break;
            case BIGINT:
                val = AInt64SerializerDeserializer.getLong(data, offset + 1);
                moments.push(val);
                break;
            case FLOAT:
                val = AFloatSerializerDeserializer.getFloat(data, offset + 1);
                moments.push(val);
                break;
            case DOUBLE:
                val = ADoubleSerializerDeserializer.getDouble(data, offset + 1);
                moments.push(val);
                break;
            default:
                throw new UnsupportedItemTypeException(sourceLoc, getFunctionIdentifier(), data[offset]);
        }
    }

    protected void finishPartialResults(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            // Double check that count 0 is accounted
            if (aggType == ATypeTag.SYSTEM_NULL) {
                resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
                result.set(resultStorage);
            } else if (aggType == ATypeTag.NULL) {
                resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                result.set(resultStorage);
            } else {
                m1Bytes.reset();
                aDouble.setValue(moments.getM1());
                doubleSerde.serialize(aDouble, m1BytesOutput);
                m2Bytes.reset();
                aDouble.setValue(moments.getM2());
                doubleSerde.serialize(aDouble, m2BytesOutput);
                m3Bytes.reset();
                aDouble.setValue(moments.getM3());
                doubleSerde.serialize(aDouble, m3BytesOutput);
                m4Bytes.reset();
                aDouble.setValue(moments.getM4());
                doubleSerde.serialize(aDouble, m4BytesOutput);
                countBytes.reset();
                aInt64.setValue(moments.getCount());
                longSerde.serialize(aInt64, countBytesOutput);
                recordEval.evaluate(null, resultBytes);
                result.set(resultBytes);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void processPartialResults(IFrameTupleReference tuple) throws HyracksDataException {
        if (skipStep()) {
            return;
        }
        eval.evaluate(tuple, inputVal);
        byte[] serBytes = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serBytes[offset]);
        switch (typeTag) {
            case NULL:
                processNull();
                break;
            case SYSTEM_NULL:
                // Ignore and return.
                break;
            case OBJECT:
                // Expected.
                aggType = ATypeTag.DOUBLE;
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
                break;
            default:
                throw new UnsupportedItemTypeException(sourceLoc, "intermediate/global-single-var-statistics",
                        serBytes[offset]);
        }
    }

    protected void finishStddevFinalResults(IPointable result, int delta) throws HyracksDataException {
        resultStorage.reset();
        try {
            long count = moments.getCount();
            if (count <= delta || aggType == ATypeTag.NULL) {
                nullSerde.serialize(ANull.NULL, resultStorage.getDataOutput());
            } else {
                aDouble.setValue(Math.sqrt(moments.getM2() / (count - delta)));
                doubleSerde.serialize(aDouble, resultStorage.getDataOutput());
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    protected void finishVarFinalResults(IPointable result, int delta) throws HyracksDataException {
        resultStorage.reset();
        try {
            long count = moments.getCount();
            if (count <= delta || aggType == ATypeTag.NULL) {
                nullSerde.serialize(ANull.NULL, resultStorage.getDataOutput());
            } else {
                aDouble.setValue(moments.getM2() / (count - delta));
                doubleSerde.serialize(aDouble, resultStorage.getDataOutput());
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    protected void finishSkewFinalResults(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            long count = moments.getCount();
            double m2 = moments.getM2();
            if (count <= 2 || aggType == ATypeTag.NULL || (m2 < Double.MIN_VALUE && m2 > -Double.MIN_VALUE)) {
                nullSerde.serialize(ANull.NULL, resultStorage.getDataOutput());
            } else {
                aDouble.setValue(Math.sqrt(count) * moments.getM3() / Math.pow(m2, 1.5));
                doubleSerde.serialize(aDouble, resultStorage.getDataOutput());
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    protected void finishKurtFinalResults(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            long count = moments.getCount();
            double m2 = moments.getM2();
            if (count <= 2 || aggType == ATypeTag.NULL || (m2 < Double.MIN_VALUE && m2 > -Double.MIN_VALUE)) {
                nullSerde.serialize(ANull.NULL, resultStorage.getDataOutput());
            } else {
                aDouble.setValue(moments.getM4() * count / (m2 * m2) - 3);
                doubleSerde.serialize(aDouble, resultStorage.getDataOutput());
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    protected boolean skipStep() {
        return false;
    }

}
