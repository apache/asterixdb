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
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.base.AMutableInt16;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableInt8;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.exceptions.IncompatibleTypeException;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.ISerializedAggregateEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractSerializableSumAggregateFunction implements ISerializedAggregateEvaluator {
    protected static final int AGG_TYPE_OFFSET = 0;
    private static final int SUM_OFFSET = 1;

    private IPointable inputVal = new VoidPointable();
    private IScalarEvaluator eval;
    private AMutableDouble aDouble = new AMutableDouble(0);
    private AMutableFloat aFloat = new AMutableFloat(0);
    private AMutableInt64 aInt64 = new AMutableInt64(0);
    private AMutableInt32 aInt32 = new AMutableInt32(0);
    private AMutableInt16 aInt16 = new AMutableInt16((short) 0);
    private AMutableInt8 aInt8 = new AMutableInt8((byte) 0);
    @SuppressWarnings("rawtypes")
    public ISerializerDeserializer serde;

    public AbstractSerializableSumAggregateFunction(IScalarEvaluatorFactory[] args, IHyracksTaskContext context)
            throws HyracksDataException {
        eval = args[0].createScalarEvaluator(context);
    }

    @Override
    public void init(DataOutput state) throws HyracksDataException {
        try {
            state.writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
            state.writeDouble(0.0);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void step(IFrameTupleReference tuple, byte[] state, int start, int len) throws HyracksDataException {
        if (skipStep(state, start)) {
            return;
        }
        ATypeTag aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);
        double sum = BufferSerDeUtil.getDouble(state, start + SUM_OFFSET);
        eval.evaluate(tuple, inputVal);
        byte[] bytes = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();

        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
            processNull(state, start);
            return;
        } else if (aggType == ATypeTag.SYSTEM_NULL) {
            aggType = typeTag;
        } else if (typeTag != ATypeTag.SYSTEM_NULL && !ATypeHierarchy.isCompatible(typeTag, aggType)) {
            throw new IncompatibleTypeException(AsterixBuiltinFunctions.SUM, bytes[offset], aggType.serialize());
        }

        if (ATypeHierarchy.canPromote(aggType, typeTag)) {
            aggType = typeTag;
        }

        switch (typeTag) {
            case INT8: {
                byte val = AInt8SerializerDeserializer.getByte(bytes, offset + 1);
                sum += val;
                break;
            }
            case INT16: {
                short val = AInt16SerializerDeserializer.getShort(bytes, offset + 1);
                sum += val;
                break;
            }
            case INT32: {
                int val = AInt32SerializerDeserializer.getInt(bytes, offset + 1);
                sum += val;
                break;
            }
            case INT64: {
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
            case NULL: {
                aggType = typeTag;
                break;
            }
            case SYSTEM_NULL: {
                processSystemNull();
                break;
            }
            default:
                throw new UnsupportedItemTypeException(AsterixBuiltinFunctions.SUM, bytes[offset]);
        }
        state[start + AGG_TYPE_OFFSET] = aggType.serialize();
        BufferSerDeUtil.writeDouble(sum, state, start + SUM_OFFSET);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void finish(byte[] state, int start, int len, DataOutput out) throws HyracksDataException {
        ATypeTag aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);
        double sum = BufferSerDeUtil.getDouble(state, start + SUM_OFFSET);
        try {
            switch (aggType) {
                case INT8: {
                    serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);
                    aInt8.setValue((byte) sum);
                    serde.serialize(aInt8, out);
                    break;
                }
                case INT16: {
                    serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT16);
                    aInt16.setValue((short) sum);
                    serde.serialize(aInt16, out);
                    break;
                }
                case INT32: {
                    serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
                    aInt32.setValue((int) sum);
                    serde.serialize(aInt32, out);
                    break;
                }
                case INT64: {
                    serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
                    aInt64.setValue((long) sum);
                    serde.serialize(aInt64, out);
                    break;
                }
                case FLOAT: {
                    serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AFLOAT);
                    aFloat.setValue((float) sum);
                    serde.serialize(aFloat, out);
                    break;
                }
                case DOUBLE: {
                    serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
                    aDouble.setValue(sum);
                    serde.serialize(aDouble, out);
                    break;
                }
                case NULL: {
                    serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
                    serde.serialize(ANull.NULL, out);
                    break;
                }
                case SYSTEM_NULL: {
                    finishSystemNull(out);
                    break;
                }
                default:
                    throw new UnsupportedItemTypeException(AsterixBuiltinFunctions.SUM, aggType.serialize());
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void finishPartial(byte[] state, int start, int len, DataOutput out) throws HyracksDataException {
        finish(state, start, len, out);
    }

    protected boolean skipStep(byte[] state, int start) {
        return false;
    }

    protected abstract void processNull(byte[] state, int start);

    protected abstract void processSystemNull() throws HyracksDataException;

    protected abstract void finishSystemNull(DataOutput out) throws IOException;

}
