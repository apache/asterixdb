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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractSumAggregateFunction extends AbstractAggregateFunction {

    // Handles evaluating and storing/passing serialized data
    protected ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private IPointable inputVal = new VoidPointable();
    private IScalarEvaluator eval;

    // Aggregate type
    protected ATypeTag aggType;

    // Result holders
    private long sumInt64;
    private double sumDouble;
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
    public AbstractSumAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        eval = args[0].createScalarEvaluator(context);
    }

    // Abstract methods
    protected abstract boolean skipStep(); // Skip step

    protected abstract void processNull(); // Handle NULL step

    protected abstract void processSystemNull() throws HyracksDataException; // Handle SYSTEM_NULL step

    protected abstract void finishNull(IPointable result) throws IOException; // Handle NULL finish

    protected abstract void finishSystemNull(IPointable result) throws IOException; // Handle SYSTEM_NULL finish

    // Init the values
    @Override
    public void init() throws HyracksDataException {
        aggType = ATypeTag.SYSTEM_NULL;
        sumInt64 = 0;
        sumDouble = 0.0;
    }

    // Called for each incoming tuple
    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
        // Skip current step
        if (skipStep()) {
            return;
        }

        // Evaluate/Get the data from the tuple
        eval.evaluate(tuple, inputVal);
        byte[] data = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();

        // Get the data type tag
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);

        // Handle MISSING and NULL values
        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
            processNull();
            return;
        }
        // Non-missing and Non-null
        else if (aggType == ATypeTag.SYSTEM_NULL) {
            aggType = typeTag;
        }

        // Calculate based on the incoming data type + handles invalid data type
        switch (typeTag) {
            case TINYINT: {
                byte val = AInt8SerializerDeserializer.getByte(data, offset + 1);
                processInt64Value(val);
                break;
            }
            case SMALLINT: {
                short val = AInt16SerializerDeserializer.getShort(data, offset + 1);
                processInt64Value(val);
                break;
            }
            case INTEGER: {
                int val = AInt32SerializerDeserializer.getInt(data, offset + 1);
                processInt64Value(val);
                break;
            }
            case BIGINT: {
                long val = AInt64SerializerDeserializer.getLong(data, offset + 1);
                processInt64Value(val);
                break;
            }
            case FLOAT: {
                upgradeOutputType();
                float val = AFloatSerializerDeserializer.getFloat(data, offset + 1);
                processFloatValue(val);
                break;
            }
            case DOUBLE: {
                upgradeOutputType();
                double val = ADoubleSerializerDeserializer.getDouble(data, offset + 1);
                processFloatValue(val);
                break;
            }
            case SYSTEM_NULL: {
                processSystemNull();
                break;
            }
            default: {
                throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.SUM, typeTag.serialize());
            }
        }
    }

    // Upgrade the output type
    private void upgradeOutputType() {
        isUseInt64ForResult = false;
    }

    // Process int64 value
    private void processInt64Value(long value) throws HyracksDataException {
        // Check the output flag first
        if (!isUseInt64ForResult) {
            processFloatValue(value);
        }
        // Int64 output, watch out for overflow exception
        else {
            try {
                sumInt64 = Math.addExact(sumInt64, value);
                sumDouble = sumInt64; // Keep the sumDouble variable up-to-date as well
            } catch (ArithmeticException ignored) {
                throw new OverflowException(sourceLoc, getIdentifier());
            }
        }
    }

    // Process float value
    private void processFloatValue(double value) {
        // If this method is called, it means the output is going to be a double, no need to check the output type flag
        // and the sumInt64 can be ignored
        sumDouble += value;
    }

    // Called for partial calculations
    @SuppressWarnings("unchecked")
    @Override
    public void finishPartial(IPointable result) throws HyracksDataException {
        // finishPartial() has identical behavior to finish()
        finishFinal(result);
    }

    // Called for final calculations
    @SuppressWarnings("unchecked")
    @Override
    public void finish(IPointable result) throws HyracksDataException {
        // Finish
        finishFinal(result);
    }

    // Called for final calculations
    @SuppressWarnings("unchecked")
    private void finishFinal(IPointable result) throws HyracksDataException {
        // Reset the result storage
        resultStorage.reset();

        try {
            // aggType is SYSTEM_NULL
            if (aggType == ATypeTag.SYSTEM_NULL) {
                finishSystemNull(result);
            }
            // aggType is NULL
            else if (aggType == ATypeTag.NULL) {
                finishNull(result);
            }
            // Pass the result
            else {
                // Output type based on the flag
                if (isUseInt64ForResult) {
                    aInt64.setValue(sumInt64);
                    aInt64Serde.serialize(aInt64, resultStorage.getDataOutput());
                    result.set(resultStorage);
                } else {
                    aDouble.setValue(sumDouble);
                    aDoubleSerde.serialize(aDouble, resultStorage.getDataOutput());
                    result.set(resultStorage);
                }
            }
        } catch (IOException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    // Function identifier
    private FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SUM;
    }
}
