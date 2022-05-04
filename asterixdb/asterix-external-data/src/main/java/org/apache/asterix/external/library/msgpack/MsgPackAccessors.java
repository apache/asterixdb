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
package org.apache.asterix.external.library.msgpack;

import static org.apache.hyracks.util.string.UTF8StringUtil.getUTFLength;
import static org.msgpack.core.MessagePack.Code.ARRAY32;
import static org.msgpack.core.MessagePack.Code.FALSE;
import static org.msgpack.core.MessagePack.Code.FLOAT32;
import static org.msgpack.core.MessagePack.Code.FLOAT64;
import static org.msgpack.core.MessagePack.Code.INT16;
import static org.msgpack.core.MessagePack.Code.INT32;
import static org.msgpack.core.MessagePack.Code.INT64;
import static org.msgpack.core.MessagePack.Code.INT8;
import static org.msgpack.core.MessagePack.Code.MAP32;
import static org.msgpack.core.MessagePack.Code.NIL;
import static org.msgpack.core.MessagePack.Code.STR32;
import static org.msgpack.core.MessagePack.Code.TRUE;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.nontagged.printers.PrintTools;
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.external.library.PyTypeInfo;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

public class MsgPackAccessors {

    private MsgPackAccessors() {
    }

    public static IMsgPackAccessor<IPointable, DataOutput, Void> createFlatMsgPackAccessor(ATypeTag aTypeTag)
            throws HyracksDataException {
        switch (aTypeTag) {
            case BOOLEAN:
                return MsgPackBooleanAccessor::apply;
            case TINYINT:
                return MsgPackInt8Accessor::apply;
            case SMALLINT:
                return MsgPackInt16Accessor::apply;
            case INTEGER:
                return MsgPackInt32Accessor::apply;
            case BIGINT:
                return MsgPackInt64Accessor::apply;
            case FLOAT:
                return MsgPackFloatAccessor::apply;
            case DOUBLE:
                return MsgPackDoubleAccessor::apply;
            case STRING:
                return MsgPackStringAccessor::apply;
            case MISSING:
            case NULL:
                return MsgPackNullAccessor::apply;
            default:
                throw HyracksDataException
                        .create(AsterixException.create(ErrorCode.TYPE_UNSUPPORTED, "msgpack", aTypeTag.name()));
        }
    }

    public static class MsgPackInt8Accessor {
        public static Void apply(IPointable pointable, DataOutput out) throws IOException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            byte o = AInt8SerializerDeserializer.getByte(b, s + 1);
            out.writeByte(INT8);
            out.writeByte(o);
            return null;
        }
    }

    public static class MsgPackInt16Accessor {

        public static Void apply(IPointable pointable, DataOutput out) throws IOException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            short i = AInt16SerializerDeserializer.getShort(b, s + 1);
            out.writeByte(INT16);
            out.writeShort(i);
            return null;
        }
    }

    public static class MsgPackInt32Accessor {

        public static Void apply(IPointable pointable, DataOutput out) throws IOException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int i = AInt32SerializerDeserializer.getInt(b, s + 1);
            out.writeByte(INT32);
            out.writeInt(i);
            return null;
        }
    }

    public static class MsgPackNullAccessor {
        public static Void apply(IPointable pointable, DataOutput out) throws IOException {
            out.writeByte(NIL);
            return null;
        }
    }

    public static class MsgPackInt64Accessor {

        public static Void apply(IPointable pointable, DataOutput out) throws IOException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            long v = AInt64SerializerDeserializer.getLong(b, s + 1);
            out.writeByte(INT64);
            out.writeLong(v);
            return null;
        }
    }

    public static class MsgPackFloatAccessor {

        public static Void apply(IPointable pointable, DataOutput out) throws IOException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            float v = AFloatSerializerDeserializer.getFloat(b, s + 1);
            out.writeByte(FLOAT32);
            out.writeFloat(v);
            return null;
        }
    }

    public static class MsgPackDoubleAccessor {
        public static Void apply(IPointable pointable, DataOutput out) throws IOException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            double v = ADoubleSerializerDeserializer.getDouble(b, s + 1);
            out.writeByte(FLOAT64);
            out.writeDouble(v);
            return null;
        }
    }

    public static class MsgPackStringAccessor {
        public static Void apply(IPointable pointable, DataOutput out) throws IOException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            out.writeByte(STR32);
            final int calculatedLength = getUTFLength(b, s + 1);
            out.writeInt(calculatedLength);
            PrintTools.writeUTF8StringRaw(b, s + 1, calculatedLength, out);
            return null;
        }

    }

    public static class MsgPackBooleanAccessor {
        public static Void apply(IPointable pointable, DataOutput out) throws IOException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            boolean v = ABooleanSerializerDeserializer.getBoolean(b, s + 1);
            if (v) {
                out.writeByte(TRUE);
            } else {
                out.writeByte(FALSE);
            }
            return null;
        }
    }

    public static class MsgPackRecordAccessor {

        public static int getUTFLength(byte[] b, int s) {
            return VarLenIntEncoderDecoder.decode(b, s);
        }

        public static Void access(ARecordVisitablePointable pointable, PyTypeInfo arg,
                MsgPackPointableVisitor pointableVisitor) throws HyracksDataException {
            List<IVisitablePointable> fieldPointables = pointable.getFieldValues();
            List<IVisitablePointable> fieldTypeTags = pointable.getFieldTypeTags();
            List<IVisitablePointable> fieldNames = pointable.getFieldNames();
            boolean closedPart;
            int index = 0;
            DataOutput out = arg.getDataOutput();
            ARecordType recordType = ((ARecordType) arg.getType());
            try {
                out.writeByte(MAP32);
                out.writeInt(fieldNames.size());
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            try {
                for (IVisitablePointable fieldPointable : fieldPointables) {
                    closedPart = index < recordType.getFieldTypes().length;
                    IVisitablePointable tt = fieldTypeTags.get(index);
                    ATypeTag typeTag =
                            EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(tt.getByteArray()[tt.getStartOffset()]);
                    IAType fieldType;
                    fieldType =
                            closedPart ? recordType.getFieldTypes()[index] : TypeTagUtil.getBuiltinTypeByTag(typeTag);
                    IPointable fieldName = fieldNames.get(index);
                    MsgPackAccessors.createFlatMsgPackAccessor(BuiltinType.ASTRING.getTypeTag()).apply(fieldName,
                            arg.getDataOutput());
                    PyTypeInfo fieldTypeInfo = pointableVisitor.getTypeInfo(fieldType, out);
                    fieldPointable.accept(pointableVisitor, fieldTypeInfo);
                    index++;
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            return null;
        }
    }

    public static class MsgPackListAccessor {

        public static Void access(AListVisitablePointable pointable, PyTypeInfo arg,
                MsgPackPointableVisitor pointableVisitor) throws HyracksDataException {
            List<IVisitablePointable> items = pointable.getItems();
            List<IVisitablePointable> itemTags = pointable.getItemTags();
            DataOutput out = arg.getDataOutput();
            try {
                out.writeByte(ARRAY32);
                out.writeInt(items.size());
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            for (int iter1 = 0; iter1 < items.size(); iter1++) {
                IVisitablePointable itemPointable = items.get(iter1);
                // First, try to get defined type.
                IAType fieldType = ((AbstractCollectionType) arg.getType()).getItemType();
                if (fieldType.getTypeTag() == ATypeTag.ANY) {
                    // Second, if defined type is not available, try to infer it from data
                    IVisitablePointable itemTagPointable = itemTags.get(iter1);
                    ATypeTag itemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                            .deserialize(itemTagPointable.getByteArray()[itemTagPointable.getStartOffset()]);
                    fieldType = TypeTagUtil.getBuiltinTypeByTag(itemTypeTag);
                }
                PyTypeInfo fieldTypeInfo = pointableVisitor.getTypeInfo(fieldType, out);
                itemPointable.accept(pointableVisitor, fieldTypeInfo);
            }
            return null;
        }
    }
}
