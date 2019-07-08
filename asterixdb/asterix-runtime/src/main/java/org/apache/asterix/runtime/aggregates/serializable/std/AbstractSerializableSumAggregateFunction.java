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
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.exceptions.OverflowException;
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
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractSerializableSumAggregateFunction extends AbstractSerializableAggregateFunction {

    // Handles evaluating and storing/passing serialized data
    protected static final int AGG_TYPE_OFFSET = 0;
    private static final int SUM_OFFSET = 1;
    private IPointable inputVal = new VoidPointable();
    private IScalarEvaluator eval;

    // Aggregate type
    protected ATypeTag aggType;

    // Result holders
    private AMutableInt64 aInt64 = new AMutableInt64(0);
    private AMutableDouble aDouble = new AMutableDouble(0);

    // Flags for output type (If all output flags are false, double output is used)
    private boolean isUseInt64ForResult = true;

    // Serializer/Deserializer
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer aInt64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer aDoubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    // Constructor
    public AbstractSerializableSumAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        eval = args[0].createScalarEvaluator(context);
    }

    // Abstract methods
    protected abstract boolean skipStep(byte[] state, int start); // Skip step

    protected abstract void processNull(byte[] state, int start); // Handle NULL step

    protected abstract void processSystemNull() throws HyracksDataException; // Handle SYSTEM_NULL step

    protected abstract void finishNull(DataOutput out) throws IOException; // Handle NULL finish

    protected abstract void finishSystemNull(DataOutput out) throws IOException; // Handle SYSTEM_NULL finish

    // Init the values
    @Override
    public void init(DataOutput state) throws HyracksDataException {
        try {
            state.writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
            state.writeLong(0);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    // Called for each incoming tuple
    @Override
    public void step(IFrameTupleReference tuple, byte[] state, int start, int len) throws HyracksDataException {
        // Skip current step
        if (skipStep(state, start)) {
            return;
        }

        // Evaluate/Get the data from the tuple
        eval.evaluate(tuple, inputVal);
        byte[] bytes = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();

        // Get the data type tag
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);

        // Handle MISSING and NULL values
        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
            processNull(state, start);
            return;
        }

        // Calculate based on the incoming data type + handles invalid data type
        switch (typeTag) {
            case TINYINT: {
                byte val = AInt8SerializerDeserializer.getByte(bytes, offset + 1);
                processInt64Value(state, start, val);
                break;
            }
            case SMALLINT: {
                short val = AInt16SerializerDeserializer.getShort(bytes, offset + 1);
                processInt64Value(state, start, val);
                break;
            }
            case INTEGER: {
                int val = AInt32SerializerDeserializer.getInt(bytes, offset + 1);
                processInt64Value(state, start, val);
                break;
            }
            case BIGINT: {
                long val = AInt64SerializerDeserializer.getLong(bytes, offset + 1);
                processInt64Value(state, start, val);
                break;
            }
            case FLOAT: {
                upgradeOutputType();
                float val = AFloatSerializerDeserializer.getFloat(bytes, offset + 1);
                processFloatValue(state, start, val);
                break;
            }
            case DOUBLE: {
                upgradeOutputType();
                double val = ADoubleSerializerDeserializer.getDouble(bytes, offset + 1);
                processFloatValue(state, start, val);
                break;
            }
            case SYSTEM_NULL:
                processSystemNull();
                break;
            default:
                throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.SUM, bytes[offset]);
        }
    }

    // Upgrade the output type
    private void upgradeOutputType() {
        isUseInt64ForResult = false;
    }

    // Process int64 value
    private void processInt64Value(byte[] state, int start, long value) throws HyracksDataException {
        // Check the output flag first
        if (!isUseInt64ForResult) {
            processFloatValue(state, start, value);
        }

        // Int64 output, watch out for overflow exception
        else {
            try {
                // Current total
                long sum = BufferSerDeUtil.getLong(state, start + SUM_OFFSET);
                sum = Math.addExact(sum, value);

                // Write the output
                state[start + AGG_TYPE_OFFSET] = ATypeTag.SERIALIZED_INT64_TYPE_TAG;
                BufferSerDeUtil.writeLong(sum, state, start + SUM_OFFSET);
            } catch (ArithmeticException ignored) {
                throw new OverflowException(sourceLoc, getIdentifier());
            }
        }
    }

    // Process float value
    private void processFloatValue(byte[] state, int start, double value) {
        double sum;
        aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);

        // This checks if the previous written value is bigint, SYSTEM_NULL or double and reads it accordingly
        // Important: SYSTEM_NULL reads int64 because the written value is in64 as well, check the init() method
        if (aggType == ATypeTag.BIGINT || aggType == ATypeTag.SYSTEM_NULL) {
            // Last write was a bigint or SYSTEM_NULL
            sum = BufferSerDeUtil.getLong(state, start + SUM_OFFSET);
        } else {
            // Last write was a double
            sum = BufferSerDeUtil.getDouble(state, start + SUM_OFFSET);
        }

        // Add the value
        sum += value;

        // Write the output
        state[start + AGG_TYPE_OFFSET] = ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG;
        BufferSerDeUtil.writeDouble(sum, state, start + SUM_OFFSET);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void finishPartial(byte[] state, int start, int len, DataOutput out) throws HyracksDataException {
        // finishPartial() has identical behavior to finish()
        finishFinal(state, start, len, out);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void finish(byte[] state, int start, int len, DataOutput out) throws HyracksDataException {
        // Finish
        finishFinal(state, start, len, out);
    }

    @SuppressWarnings({ "unchecked", "unused" })
    private void finishFinal(byte[] state, int start, int len, DataOutput out) throws HyracksDataException {
        aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);

        try {
            // aggType is SYSTEM_NULL (ran over zero values)
            if (aggType == ATypeTag.SYSTEM_NULL) {
                finishSystemNull(out);
            }
            // aggType is NULL
            else if (aggType == ATypeTag.NULL) {
                finishNull(out);
            }
            // Pass the result
            else {
                if (isUseInt64ForResult) {
                    long sum = BufferSerDeUtil.getLong(state, start + SUM_OFFSET);
                    aInt64.setValue(sum);
                    aInt64Serde.serialize(aInt64, out);
                } else {
                    double sum = BufferSerDeUtil.getDouble(state, start + SUM_OFFSET);
                    aDouble.setValue(sum);
                    aDoubleSerde.serialize(aDouble, out);
                }
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    // Function identifier
    private FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SUM;
    }
}
