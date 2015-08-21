/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.base.AMutableInt16;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.AMutableInt8;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.hierachy.ATypeHierarchy;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractSumAggregateFunction implements ICopyAggregateFunction {
    protected DataOutput out;
    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
    private ICopyEvaluator eval;
    private double sum;
    protected ATypeTag aggType;
    private AMutableDouble aDouble = new AMutableDouble(0);
    private AMutableFloat aFloat = new AMutableFloat(0);
    private AMutableInt64 aInt64 = new AMutableInt64(0);
    private AMutableInt32 aInt32 = new AMutableInt32(0);
    private AMutableInt16 aInt16 = new AMutableInt16((short) 0);
    private AMutableInt8 aInt8 = new AMutableInt8((byte) 0);
    @SuppressWarnings("rawtypes")
    protected ISerializerDeserializer serde;

    public AbstractSumAggregateFunction(ICopyEvaluatorFactory[] args, IDataOutputProvider provider)
            throws AlgebricksException {
        out = provider.getDataOutput();
        eval = args[0].createEvaluator(inputVal);
    }

    @Override
    public void init() {
        aggType = ATypeTag.SYSTEM_NULL;
        sum = 0.0;
    }

    @Override
    public void step(IFrameTupleReference tuple) throws AlgebricksException {
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
            throw new AlgebricksException("Unexpected type " + typeTag
                    + " in aggregation input stream. Expected type (or a promotable type to)" + aggType + ".");
        }

        if (ATypeHierarchy.canPromote(aggType, typeTag)) {
            aggType = typeTag;
        }

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
            case NULL: {
                break;
            }
            case SYSTEM_NULL: {
                processSystemNull();
                break;
            }
            default: {
                throw new NotImplementedException("Cannot compute SUM for values of type " + typeTag + ".");
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void finish() throws AlgebricksException {
        try {
            switch (aggType) {
                case INT8: {
                    serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);
                    aInt8.setValue((byte) sum);
                    serde.serialize(aInt8, out);
                    break;
                }
                case INT16: {
                    serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT16);
                    aInt16.setValue((short) sum);
                    serde.serialize(aInt16, out);
                    break;
                }
                case INT32: {
                    serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
                    aInt32.setValue((int) sum);
                    serde.serialize(aInt32, out);
                    break;
                }
                case INT64: {
                    serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
                    aInt64.setValue((long) sum);
                    serde.serialize(aInt64, out);
                    break;
                }
                case FLOAT: {
                    serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AFLOAT);
                    aFloat.setValue((float) sum);
                    serde.serialize(aFloat, out);
                    break;
                }
                case DOUBLE: {
                    serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
                    aDouble.setValue(sum);
                    serde.serialize(aDouble, out);
                    break;
                }
                case NULL: {
                    serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
                    serde.serialize(ANull.NULL, out);
                    break;
                }
                case SYSTEM_NULL: {
                    finishSystemNull();
                    break;
                }
                default:
                    throw new AlgebricksException("SumAggregationFunction: incompatible type for the result ("
                            + aggType + "). ");
            }
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void finishPartial() throws AlgebricksException {
        finish();
    }

    protected boolean skipStep() {
        return false;
    }
    protected abstract void processNull();
    protected abstract void processSystemNull() throws AlgebricksException;
    protected abstract void finishSystemNull() throws IOException;
}
