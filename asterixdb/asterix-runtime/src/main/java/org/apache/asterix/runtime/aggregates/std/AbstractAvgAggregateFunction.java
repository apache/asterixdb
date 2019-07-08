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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractAvgAggregateFunction extends AbstractAggregateFunction {
    private static final int SUM_FIELD_ID = 0;
    private static final int COUNT_FIELD_ID = 1;

    private final ARecordType recType;

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private IPointable inputVal = new VoidPointable();
    private IScalarEvaluator eval;
    protected ATypeTag aggType;
    private double sum;
    private long count;
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

    public AbstractAvgAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        eval = args[0].createScalarEvaluator(context);
        recType = new ARecordType(null, new String[] { "sum", "count" },
                new IAType[] { BuiltinType.ADOUBLE, BuiltinType.AINT64 }, false);
        recordEval = new ClosedRecordConstructorEval(recType, new IScalarEvaluator[] { evalSum, evalCount });
    }

    @Override
    public void init() throws HyracksDataException {
        aggType = ATypeTag.SYSTEM_NULL;
        sum = 0.0;
        count = 0;
    }

    @Override
    public abstract void step(IFrameTupleReference tuple) throws HyracksDataException;

    @Override
    public abstract void finish(IPointable result) throws HyracksDataException;

    @Override
    public abstract void finishPartial(IPointable result) throws HyracksDataException;

    protected abstract void processNull();

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
                throw new IncompatibleTypeException(sourceLoc, BuiltinFunctions.AVG, data[offset], aggType.serialize());
            } else {
                throw new IncompatibleTypeException(sourceLoc, BuiltinFunctions.AVG, aggType.serialize(), data[offset]);
            }
        } else if (ATypeHierarchy.canPromote(aggType, typeTag)) {
            aggType = typeTag;
        }
        ++count;
        switch (typeTag) {
            case TINYINT: {
                byte val = AInt8SerializerDeserializer.getByte(data, offset + 1);
                sum += val;
                break;
            }
            case SMALLINT: {
                short val = AInt16SerializerDeserializer.getShort(data, offset + 1);
                sum += val;
                break;
            }
            case INTEGER: {
                int val = AInt32SerializerDeserializer.getInt(data, offset + 1);
                sum += val;
                break;
            }
            case BIGINT: {
                long val = AInt64SerializerDeserializer.getLong(data, offset + 1);
                sum += val;
                break;
            }
            case FLOAT: {
                float val = AFloatSerializerDeserializer.getFloat(data, offset + 1);
                sum += val;
                break;
            }
            case DOUBLE: {
                double val = ADoubleSerializerDeserializer.getDouble(data, offset + 1);
                sum += val;
                break;
            }
            default: {
                throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.AVG, data[offset]);
            }
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
                sumBytes.reset();
                aDouble.setValue(sum);
                doubleSerde.serialize(aDouble, sumBytesOutput);
                countBytes.reset();
                aInt64.setValue(count);
                longSerde.serialize(aInt64, countBytesOutput);
                recordEval.evaluate(null, avgBytes);
                result.set(avgBytes);
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
            case NULL: {
                processNull();
                break;
            }
            case SYSTEM_NULL: {
                // Ignore and return.
                break;
            }
            case OBJECT: {
                // Expected.
                aggType = ATypeTag.DOUBLE;
                int nullBitmapSize = 0;
                int offset1 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, SUM_FIELD_ID,
                        nullBitmapSize, false);
                sum += ADoubleSerializerDeserializer.getDouble(serBytes, offset1);
                int offset2 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, COUNT_FIELD_ID,
                        nullBitmapSize, false);
                count += AInt64SerializerDeserializer.getLong(serBytes, offset2);
                break;
            }
            default: {
                throw new UnsupportedItemTypeException(sourceLoc, "intermediate/global-Avg", serBytes[offset]);
            }
        }
    }

    protected void finishFinalResults(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            if (count == 0 || aggType == ATypeTag.NULL) {
                nullSerde.serialize(ANull.NULL, resultStorage.getDataOutput());
            } else {
                aDouble.setValue(sum / count);
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
