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

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
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
import org.apache.asterix.runtime.evaluators.common.AccessibleByteArrayEval;
import org.apache.asterix.runtime.evaluators.common.ClosedRecordConstructorEvalFactory.ClosedRecordConstructorEval;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractAvgAggregateFunction implements ICopyAggregateFunction {
    private static final int SUM_FIELD_ID = 0;
    private static final int COUNT_FIELD_ID = 1;

    private final ARecordType recType;

    private DataOutput out;
    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
    private ICopyEvaluator eval;
    protected ATypeTag aggType;
    private double sum;
    private long count;
    private AMutableDouble aDouble = new AMutableDouble(0);
    private AMutableInt64 aInt64 = new AMutableInt64(0);

    private ArrayBackedValueStorage avgBytes = new ArrayBackedValueStorage();
    private ByteArrayAccessibleOutputStream sumBytes = new ByteArrayAccessibleOutputStream();
    private DataOutput sumBytesOutput = new DataOutputStream(sumBytes);
    private ByteArrayAccessibleOutputStream countBytes = new ByteArrayAccessibleOutputStream();
    private DataOutput countBytesOutput = new DataOutputStream(countBytes);
    private ICopyEvaluator evalSum = new AccessibleByteArrayEval(avgBytes.getDataOutput(), sumBytes);
    private ICopyEvaluator evalCount = new AccessibleByteArrayEval(avgBytes.getDataOutput(), countBytes);
    private ClosedRecordConstructorEval recordEval;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ADouble> doubleSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADOUBLE);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt64> longSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);

    public AbstractAvgAggregateFunction(ICopyEvaluatorFactory[] args, IDataOutputProvider output)
            throws AlgebricksException {
        eval = args[0].createEvaluator(inputVal);
        out = output.getDataOutput();

        recType = new ARecordType(null, new String[] { "sum", "count" },
                new IAType[] { BuiltinType.ADOUBLE, BuiltinType.AINT64 }, false);
        recordEval = new ClosedRecordConstructorEval(recType, new ICopyEvaluator[] { evalSum, evalCount }, avgBytes,
                out);
    }

    @Override
    public void init() {
        aggType = ATypeTag.SYSTEM_NULL;
        sum = 0.0;
        count = 0;
    }

    @Override
    public abstract void step(IFrameTupleReference tuple) throws AlgebricksException;

    @Override
    public abstract void finish() throws AlgebricksException;

    @Override
    public abstract void finishPartial() throws AlgebricksException;

    protected abstract void processNull();

    protected void processDataValues(IFrameTupleReference tuple) throws AlgebricksException {
        if (skipStep()) {
            return;
        }
        inputVal.reset();
        eval.evaluate(tuple);
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[0]);
        if (typeTag == ATypeTag.NULL) {
            processNull();
            return;
        } else if (aggType == ATypeTag.SYSTEM_NULL) {
            aggType = typeTag;
        } else if (typeTag != ATypeTag.SYSTEM_NULL && !ATypeHierarchy.isCompatible(typeTag, aggType)) {
            throw new AlgebricksException(
                    "Unexpected type " + typeTag + " in aggregation input stream. Expected type " + aggType + ".");
        } else if (ATypeHierarchy.canPromote(aggType, typeTag)) {
            aggType = typeTag;
        }
        ++count;
        switch (typeTag) {
            case INT8: {
                byte val = AInt8SerializerDeserializer.getByte(inputVal.getByteArray(), 1);
                sum += val;
                break;
            }
            case INT16: {
                short val = AInt16SerializerDeserializer.getShort(inputVal.getByteArray(), 1);
                sum += val;
                break;
            }
            case INT32: {
                int val = AInt32SerializerDeserializer.getInt(inputVal.getByteArray(), 1);
                sum += val;
                break;
            }
            case INT64: {
                long val = AInt64SerializerDeserializer.getLong(inputVal.getByteArray(), 1);
                sum += val;
                break;
            }
            case FLOAT: {
                float val = AFloatSerializerDeserializer.getFloat(inputVal.getByteArray(), 1);
                sum += val;
                break;
            }
            case DOUBLE: {
                double val = ADoubleSerializerDeserializer.getDouble(inputVal.getByteArray(), 1);
                sum += val;
                break;
            }
            default: {
                throw new NotImplementedException("Cannot compute AVG for values of type " + typeTag);
            }
        }
        inputVal.reset();
    }

    protected void finishPartialResults() throws AlgebricksException {
        try {
            // Double check that count 0 is accounted
            if (aggType == ATypeTag.SYSTEM_NULL) {
                if (GlobalConfig.DEBUG) {
                    GlobalConfig.ASTERIX_LOGGER.finest("AVG aggregate ran over empty input.");
                }
                out.writeByte(ATypeTag.SYSTEM_NULL.serialize());
            } else if (aggType == ATypeTag.NULL) {
                out.writeByte(ATypeTag.NULL.serialize());
            } else {
                sumBytes.reset();
                aDouble.setValue(sum);
                doubleSerde.serialize(aDouble, sumBytesOutput);
                countBytes.reset();
                aInt64.setValue(count);
                longSerde.serialize(aInt64, countBytesOutput);
                recordEval.evaluate(null);
            }
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    protected void processPartialResults(IFrameTupleReference tuple) throws AlgebricksException {
        if (skipStep()) {
            return;
        }
        inputVal.reset();
        eval.evaluate(tuple);
        byte[] serBytes = inputVal.getByteArray();
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serBytes[0]);
        switch (typeTag) {
            case NULL: {
                processNull();
                break;
            }
            case SYSTEM_NULL: {
                // Ignore and return.
                break;
            }
            case RECORD: {
                // Expected.
                aggType = ATypeTag.DOUBLE;
                int nullBitmapSize = 0;
                int offset1 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, SUM_FIELD_ID, nullBitmapSize,
                        false);
                sum += ADoubleSerializerDeserializer.getDouble(serBytes, offset1);
                int offset2 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, COUNT_FIELD_ID, nullBitmapSize,
                        false);
                count += AInt64SerializerDeserializer.getLong(serBytes, offset2);
                break;
            }
            default: {
                throw new AlgebricksException("Global-Avg is not defined for values of type "
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serBytes[0]));
            }
        }
    }

    protected void finishFinalResults() throws AlgebricksException {
        try {
            if (count == 0 || aggType == ATypeTag.NULL) {
                nullSerde.serialize(ANull.NULL, out);
            } else {
                aDouble.setValue(sum / count);
                doubleSerde.serialize(aDouble, out);
            }
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    protected boolean skipStep() {
        return false;
    }

}
