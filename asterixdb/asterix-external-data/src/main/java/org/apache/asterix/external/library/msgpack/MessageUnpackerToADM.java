/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.library.msgpack;

import static org.msgpack.core.MessagePack.Code.ARRAY16;
import static org.msgpack.core.MessagePack.Code.ARRAY32;
import static org.msgpack.core.MessagePack.Code.FALSE;
import static org.msgpack.core.MessagePack.Code.FIXARRAY_PREFIX;
import static org.msgpack.core.MessagePack.Code.FIXMAP_PREFIX;
import static org.msgpack.core.MessagePack.Code.FIXSTR_PREFIX;
import static org.msgpack.core.MessagePack.Code.FLOAT32;
import static org.msgpack.core.MessagePack.Code.FLOAT64;
import static org.msgpack.core.MessagePack.Code.INT16;
import static org.msgpack.core.MessagePack.Code.INT32;
import static org.msgpack.core.MessagePack.Code.INT64;
import static org.msgpack.core.MessagePack.Code.INT8;
import static org.msgpack.core.MessagePack.Code.MAP16;
import static org.msgpack.core.MessagePack.Code.MAP32;
import static org.msgpack.core.MessagePack.Code.NIL;
import static org.msgpack.core.MessagePack.Code.STR16;
import static org.msgpack.core.MessagePack.Code.STR32;
import static org.msgpack.core.MessagePack.Code.STR8;
import static org.msgpack.core.MessagePack.Code.TRUE;
import static org.msgpack.core.MessagePack.Code.UINT16;
import static org.msgpack.core.MessagePack.Code.UINT32;
import static org.msgpack.core.MessagePack.Code.UINT64;
import static org.msgpack.core.MessagePack.Code.UINT8;
import static org.msgpack.core.MessagePack.Code.isFixInt;
import static org.msgpack.core.MessagePack.Code.isFixStr;
import static org.msgpack.core.MessagePack.Code.isFixedArray;
import static org.msgpack.core.MessagePack.Code.isFixedMap;
import static org.msgpack.core.MessagePack.Code.isNegFixInt;
import static org.msgpack.core.MessagePack.Code.isPosFixInt;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.input.stream.StandardUTF8ToModifiedUTF8OutputStream;
import org.apache.asterix.external.input.stream.builders.ListLikeNumericArrayFactory;
import org.apache.asterix.external.input.stream.builders.StandardToModifiedUTF8OutputStreamFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class MessageUnpackerToADM {

    private final IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool =
            new ListObjectPool<>(new AbvsBuilderFactory());
    private final IObjectPool<StandardUTF8ToModifiedUTF8OutputStream, ATypeTag> utfPool =
            new ListObjectPool<>(new StandardToModifiedUTF8OutputStreamFactory());
    private final IObjectPool<List<Long>, Long> listPool = new ListObjectPool<>(new ListLikeNumericArrayFactory<>());

    public MessageUnpackerToADM() {
    }

    public void unpack(ByteBuffer in, DataOutput out, boolean tagged) throws IOException {
        byte tag = NIL;
        if (in != null) {
            tag = in.get();
        }
        if (isFixStr(tag)) {
            unpackStr(in, out, (tag ^ FIXSTR_PREFIX), tagged);
        } else if (isFixInt(tag)) {
            if (tagged) {
                out.writeByte(ATypeTag.SERIALIZED_INT8_TYPE_TAG);
            }
            if (isPosFixInt(tag)) {
                out.writeByte(tag);
            } else if (isNegFixInt(tag)) {
                out.writeByte(tag);
            }
        } else if (isFixedArray(tag)) {
            unpackArray(in, out, (tag ^ FIXARRAY_PREFIX));
        } else if (isFixedMap(tag)) {
            unpackMap(in, out, (tag ^ FIXMAP_PREFIX));
        } else {
            switch (tag) {
                case TRUE:
                    out.writeByte(ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG);
                    out.writeByte((byte) 1);
                    break;
                case FALSE:
                    out.writeByte(ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG);
                    out.writeByte((byte) 0);
                    break;
                case NIL:
                    out.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                    break;
                case UINT8:
                    unpackUByte(in, out, tagged);
                    break;
                case UINT16:
                    unpackUShort(in, out, tagged);
                    break;
                case UINT32:
                    unpackUInt(in, out, tagged);
                    break;
                case UINT64:
                    unpackULong(in, out, tagged);
                    break;
                case INT8:
                    unpackByte(in, out, tagged);
                    break;
                case INT16:
                    unpackShort(in, out, tagged);
                    break;
                case INT32:
                    unpackInt(in, out, tagged);
                    break;
                case INT64:
                    unpackLong(in, out, tagged);
                    break;
                case FLOAT32:
                    unpackFloat(in, out, tagged);
                    break;
                case FLOAT64:
                    unpackDouble(in, out, tagged);
                    break;
                case STR8:
                    unpackStr(in, out, Byte.toUnsignedInt(in.get()), tagged);
                    break;
                case STR16:
                    unpackStr(in, out, Short.toUnsignedInt(in.getShort()), tagged);
                    break;
                case STR32:
                    unpackStr(in, out, Integer.toUnsignedLong(in.getInt()), tagged);
                    break;
                case ARRAY16:
                    unpackArray(in, out, Short.toUnsignedInt(in.getShort()));
                    break;
                case ARRAY32:
                    unpackArray(in, out, Integer.toUnsignedLong(in.getInt()));
                    break;
                case MAP16:
                    unpackMap(in, out, Short.toUnsignedInt(in.getShort()));
                    break;
                case MAP32:
                    unpackMap(in, out, (int) Integer.toUnsignedLong(in.getInt()));
                    break;
                default:
                    throw HyracksDataException.create(AsterixException.create(
                            ErrorCode.PARSER_ADM_DATA_PARSER_CAST_ERROR, "msgpack tag " + tag + " ", "to an ADM type"));
            }
        }
    }

    public static void unpackByte(ByteBuffer in, DataOutput out, boolean tagged) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_INT8_TYPE_TAG);
        }
        out.writeByte(in.get());
    }

    public static void unpackShort(ByteBuffer in, DataOutput out, boolean tagged) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_INT16_TYPE_TAG);
        }
        out.writeShort(in.getShort());
    }

    public static void unpackInt(ByteBuffer in, DataOutput out, boolean tagged) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
        }
        out.writeInt(in.getInt());
    }

    public static void unpackLong(ByteBuffer in, DataOutput out, boolean tagged) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_INT64_TYPE_TAG);
        }
        out.writeLong(in.getLong());
    }

    public static void unpackUByte(ByteBuffer in, DataOutput out, boolean tagged) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_INT16_TYPE_TAG);
        }
        out.writeShort((short) (in.get() & ((short) 0x00FF)));
    }

    public static void unpackUShort(ByteBuffer in, DataOutput out, boolean tagged) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
        }
        out.writeInt(in.getShort() & 0x0000FFFF);
    }

    public static void unpackUInt(ByteBuffer in, DataOutput out, boolean tagged) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_INT64_TYPE_TAG);
        }
        out.writeLong(in.getInt() & 0x00000000FFFFFFFFl);
    }

    public static void unpackULong(ByteBuffer in, DataOutput out, boolean tagged) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_INT64_TYPE_TAG);
        }
        long val = in.getLong();
        if (val < 0) {
            throw new IllegalArgumentException("Integer overflow");
        }
        out.writeLong(val);
    }

    public static void unpackFloat(ByteBuffer in, DataOutput out, boolean tagged) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_FLOAT_TYPE_TAG);
        }
        out.writeFloat(in.getFloat());

    }

    public static void unpackDouble(ByteBuffer in, DataOutput out, boolean tagged) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
        }
        out.writeDouble(in.getDouble());
    }

    public void unpackArray(ByteBuffer in, DataOutput out, long uLen) throws IOException {
        if (uLen > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException("Array is too long");
        }
        ArrayBackedValueStorage buildBuf = (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.ARRAY);
        buildBuf.reset();
        DataOutput bufOut = buildBuf.getDataOutput();
        int count = (int) uLen;
        bufOut.writeByte(ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG);
        bufOut.writeByte(ATypeTag.ANY.serialize());
        int asxLenPos = buildBuf.getLength();
        //reserve space
        bufOut.writeInt(-1);
        bufOut.writeInt(count);
        int slotStartOffs = buildBuf.getLength();
        for (int i = 0; i < count; i++) {
            bufOut.writeInt(0xDEADBEEF);
        }
        for (int i = 0; i < count; i++) {
            IntegerPointable.setInteger(buildBuf.getByteArray(), ((slotStartOffs) + (i * 4)), buildBuf.getLength());
            //tagged b/c any
            unpack(in, bufOut, true);
        }
        IntegerPointable.setInteger(buildBuf.getByteArray(), asxLenPos, buildBuf.getLength());
        out.write(buildBuf.getByteArray(), buildBuf.getStartOffset(), buildBuf.getLength());
    }

    public void unpackMap(ByteBuffer in, DataOutput out, int count) throws IOException {
        //TODO: need to handle typed records. this only produces a completely open record.
        ArrayBackedValueStorage buildBuf = (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.OBJECT);
        List<Long> offsets = listPool.allocate((long) count);
        DataOutput bufOut = buildBuf.getDataOutput();
        //hdr size = 6?
        bufOut.writeByte(ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
        int totalSizeOffs = buildBuf.getLength();
        bufOut.writeInt(-1);
        //isExpanded
        bufOut.writeByte((byte) 1);
        int openPartOffs = buildBuf.getLength();
        bufOut.writeInt(-1);
        //isExpanded, so num of open fields
        IntegerPointable.setInteger(buildBuf.getByteArray(), openPartOffs, buildBuf.getLength());
        bufOut.writeInt(count);
        int offsetAryPos = buildBuf.getLength();
        int offsetArySz = count * 2;
        //allocate space for open field offsets
        for (int i = 0; i < offsetArySz; i++) {
            bufOut.writeInt(0xDEADBEEF);
        }
        for (int i = 0; i < count; i++) {
            int offs = buildBuf.getLength();
            unpack(in, bufOut, false);
            long hash = UTF8StringUtil.hash(buildBuf.getByteArray(), offs);
            offsets.set(i, (hash << 32) + offs);
            unpack(in, bufOut, true);
        }
        Collections.sort(offsets);
        for (Long l : offsets) {
            IntegerPointable.setInteger(buildBuf.getByteArray(), offsetAryPos, (int) (l >> 32));
            offsetAryPos += 4;
            IntegerPointable.setInteger(buildBuf.getByteArray(), offsetAryPos, (int) ((l << 32) >> 32));
            offsetAryPos += 4;
        }
        IntegerPointable.setInteger(buildBuf.getByteArray(), totalSizeOffs, buildBuf.getLength());
        out.write(buildBuf.getByteArray(), buildBuf.getStartOffset(), buildBuf.getLength());
    }

    public void unpackStr(ByteBuffer in, DataOutput out, long uLen, boolean tag) throws IOException {
        if (tag) {
            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
        if (Long.compareUnsigned(uLen, Integer.MAX_VALUE) > 0) {
            throw new UnsupportedOperationException("String is too long");
        }
        int len = (int) uLen;
        StandardUTF8ToModifiedUTF8OutputStream conv = utfPool.allocate(ATypeTag.STRING);
        conv.setDataOutput(out);
        conv.write(in.array(), in.arrayOffset() + in.position(), len);
        in.position(in.position() + len);
    }

}
