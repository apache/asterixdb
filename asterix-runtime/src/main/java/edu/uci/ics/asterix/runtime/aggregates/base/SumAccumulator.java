package edu.uci.ics.asterix.runtime.aggregates.base;

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
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.BufferSerDeUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

public class SumAccumulator implements IAccumulator {
    private static final int SUM_OFF = 0;
    // TODO: Let's encode this in a single byte.
    private static final int MET_INT8_OFF = 8;
    private static final int MET_INT16_OFF = 9;
    private static final int MET_INT32_OFF = 10;
    private static final int MET_INT64_OFF = 11;
    private static final int MET_FLOAT_OFF = 12;
    private static final int MET_DOUBLE_OFF = 13;
    private static final int MET_NULL_OFF = 14;
        
    private AMutableInt8 aInt8 = new AMutableInt8((byte) 0);
    private AMutableInt16 aInt16 = new AMutableInt16((short) 0);
    private AMutableInt32 aInt32 = new AMutableInt32(0);
    private AMutableInt64 aInt64 = new AMutableInt64(0);
    private AMutableFloat aFloat = new AMutableFloat(0);
    private AMutableDouble aDouble = new AMutableDouble(0);
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer serde;
    
    private IValueReference defaultValue;
    
    @Override
    public void init(IMutableValueStorage state, IValueReference defaultValue) throws IOException {
        // Set initial value.
        state.getDataOutput().writeDouble(0);
        // Initialize met flags to false.
        state.getDataOutput().write((byte) 0);
        state.getDataOutput().write((byte) 0);
        state.getDataOutput().write((byte) 0);
        state.getDataOutput().write((byte) 0);
        state.getDataOutput().write((byte) 0);
        state.getDataOutput().write((byte) 0);
        state.getDataOutput().write((byte) 0);
        // Remember default value.
        this.defaultValue = defaultValue;
    }

    @Override
    public void step(IMutableValueStorage state, IValueReference value) {
        byte[] valueBytes = value.getByteArray();
        int stateStartOff = state.getStartOffset();
        ATypeTag valueTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(valueBytes[0]);
        double sum = BufferSerDeUtil.getDouble(state.getByteArray(), stateStartOff + SUM_OFF);
        switch (valueTypeTag) {
            case INT8: {
                state.getByteArray()[stateStartOff + MET_INT8_OFF] = 1;
                sum += AInt8SerializerDeserializer.getByte(valueBytes, 1);
                break;
            }
            case INT16: {
                state.getByteArray()[stateStartOff + MET_INT16_OFF] = 1;
                sum += AInt16SerializerDeserializer.getShort(valueBytes, 1);
                break;
            }
            case INT32: {
                state.getByteArray()[stateStartOff + MET_INT32_OFF] = 1;
                sum += AInt32SerializerDeserializer.getInt(valueBytes, 1);
                break;
            }
            case INT64: {
                state.getByteArray()[stateStartOff + MET_INT64_OFF] = 1;
                sum += AInt64SerializerDeserializer.getLong(valueBytes, 1);
                break;
            }
            case FLOAT: {
                state.getByteArray()[stateStartOff + MET_FLOAT_OFF] = 1;
                sum += AFloatSerializerDeserializer.getFloat(valueBytes, 1);
                break;
            }
            case DOUBLE: {
                state.getByteArray()[stateStartOff + MET_DOUBLE_OFF] = 1;
                sum += ADoubleSerializerDeserializer.getDouble(valueBytes, 1);
                break;
            }
            case NULL: {
                state.getByteArray()[stateStartOff + MET_NULL_OFF] = 1;
                break;
            }
            case SYSTEM_NULL: {
                // Ignore.
                break;
            }
            default: {
                throw new NotImplementedException("Cannot compute SUM for values of type "
                        + valueTypeTag);
            }
        }
        BufferSerDeUtil.writeDouble(sum, state.getByteArray(), stateStartOff + SUM_OFF);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void finish(IMutableValueStorage state, DataOutput out) throws IOException {
        byte[] stateBytes = state.getByteArray();
        int stateStartOff = state.getStartOffset();        
        double sum = BufferSerDeUtil.getDouble(stateBytes, stateStartOff + SUM_OFF);
        if (stateBytes[stateStartOff + MET_NULL_OFF] == 1) {
            serde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);
            serde.serialize(ANull.NULL, out);
        } else if (stateBytes[stateStartOff + MET_DOUBLE_OFF] == 1) {
            serde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ADOUBLE);
            aDouble.setValue(sum);
            serde.serialize(aDouble, out);
        } else if (stateBytes[stateStartOff + MET_FLOAT_OFF] == 1) {
            serde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.AFLOAT);
            aFloat.setValue((float) sum);
            serde.serialize(aFloat, out);
        } else if (stateBytes[stateStartOff + MET_INT64_OFF] == 1) {
            serde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.AINT64);
            aInt64.setValue((long) sum);
            serde.serialize(aInt64, out);
        } else if (stateBytes[stateStartOff + MET_INT32_OFF] == 1) {
            serde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.AINT32);
            aInt32.setValue((int) sum);
            serde.serialize(aInt32, out);
        } else if (stateBytes[stateStartOff + MET_INT16_OFF] == 1) {
            serde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.AINT16);
            aInt16.setValue((short) sum);
            serde.serialize(aInt16, out);
        } else if (stateBytes[stateStartOff + MET_INT8_OFF] == 1) {
            serde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.AINT8);
            aInt8.setValue((byte) sum);
            serde.serialize(aInt8, out);
        } else {
            out.write(defaultValue.getByteArray(), defaultValue.getStartOffset(), defaultValue.getLength());
        }
    }
}
