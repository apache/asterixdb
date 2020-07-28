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

import static org.msgpack.core.MessagePack.Code.*;

import java.nio.ByteBuffer;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class MessageUnpackerToADM {

    public static void unpack(ByteBuffer in, ByteBuffer out, boolean tagged) {
        byte tag = NIL;
        if (in != null) {
            tag = in.get();
        }
        if (isFixStr(tag)) {
            unpackStr(in, out, (tag ^ FIXSTR_PREFIX), tagged);
        } else if (isFixInt(tag)) {
            if (tagged) {
                out.put(ATypeTag.SERIALIZED_INT8_TYPE_TAG);
            }
            if (isPosFixInt(tag)) {
                out.put(tag);
            } else if (isNegFixInt(tag)) {
                out.put(tag);
            }
        } else if (isFixedArray(tag)) {
            unpackArray(in, out, (tag ^ FIXARRAY_PREFIX));
        } else if (isFixedMap(tag)) {
            unpackMap(in, out, (tag ^ FIXMAP_PREFIX));
        } else {
            switch (tag) {
                case TRUE:
                    out.put(ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG);
                    out.put((byte) 1);
                    break;
                case FALSE:
                    out.put(ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG);
                    out.put((byte) 0);
                    break;
                case NIL:
                    out.put(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
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
                    throw new IllegalArgumentException("NYI");
            }
        }
    }

    public static long unpackNextInt(ByteBuffer in) {
        byte tag = in.get();
        if (isFixInt(tag)) {
            if (isPosFixInt(tag)) {
                return tag;
            } else if (isNegFixInt(tag)) {
                return (tag ^ NEGFIXINT_PREFIX);
            }
        } else {
            switch (tag) {
                case INT8:
                    return in.get();
                case UINT8:
                    return Byte.toUnsignedInt(in.get());
                case INT16:
                    return in.getShort();
                case UINT16:
                    return Short.toUnsignedInt(in.getShort());
                case INT32:
                    return in.getInt();
                case UINT32:
                    return Integer.toUnsignedLong(in.getInt());
                case INT64:
                    return in.getLong();
                default:
                    throw new IllegalArgumentException("NYI");
            }
        }
        return -1;
    }

    public static void unpackByte(ByteBuffer in, ByteBuffer out, boolean tagged) {
        if (tagged) {
            out.put(ATypeTag.SERIALIZED_INT8_TYPE_TAG);
        }
        out.put(in.get());
    }

    public static void unpackShort(ByteBuffer in, ByteBuffer out, boolean tagged) {
        if (tagged) {
            out.put(ATypeTag.SERIALIZED_INT16_TYPE_TAG);
        }
        out.putShort(in.getShort());
    }

    public static void unpackInt(ByteBuffer in, ByteBuffer out, boolean tagged) {
        if (tagged) {
            out.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
        }
        out.putInt(in.getInt());
    }

    public static void unpackLong(ByteBuffer in, ByteBuffer out, boolean tagged) {
        if (tagged) {
            out.put(ATypeTag.SERIALIZED_INT64_TYPE_TAG);
        }
        out.putLong(in.getLong());
    }

    public static void unpackUByte(ByteBuffer in, ByteBuffer out, boolean tagged) {
        if (tagged) {
            out.put(ATypeTag.SERIALIZED_INT16_TYPE_TAG);
        }
        out.putShort((short) (in.get() & ((short) 0x00FF)));
    }

    public static void unpackUShort(ByteBuffer in, ByteBuffer out, boolean tagged) {
        if (tagged) {
            out.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
        }
        out.putInt(in.getShort() & 0x0000FFFF);
    }

    public static void unpackUInt(ByteBuffer in, ByteBuffer out, boolean tagged) {
        if (tagged) {
            out.put(ATypeTag.SERIALIZED_INT64_TYPE_TAG);
        }
        out.putLong(in.getInt() & 0x00000000FFFFFFFFl);
    }

    public static void unpackFloat(ByteBuffer in, ByteBuffer out, boolean tagged) {
        if (tagged) {
            out.put(ATypeTag.SERIALIZED_FLOAT_TYPE_TAG);
        }
        out.putFloat(in.getFloat());

    }

    public static void unpackDouble(ByteBuffer in, ByteBuffer out, boolean tagged) {
        if (tagged) {
            out.put(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
        }
        out.putDouble(in.getDouble());
    }

    public static void unpackArray(ByteBuffer in, ByteBuffer out, long uLen) {
        if (uLen > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException("String is too long");
        }
        int count = (int) uLen;
        int offs = out.position();
        out.put(ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG);
        out.put(ATypeTag.ANY.serialize());
        int asxLenPos = out.position();
        //reserve space
        out.putInt(-1);
        out.putInt(count);
        int slotStartOffs = out.position() + out.arrayOffset();
        for (int i = 0; i < count; i++) {
            out.putInt(0xFFFF);
        }
        for (int i = 0; i < count; i++) {
            out.putInt(slotStartOffs + (i * 4), (out.position() - offs));
            unpack(in, out, true);
        }
        int totalLen = out.position() - offs;
        out.putInt(asxLenPos, totalLen);
    }

    public static void unpackMap(ByteBuffer in, ByteBuffer out, int count) {
        //TODO: need to handle typed records. this only produces a completely open record.
        //hdr size = 6?
        int startOffs = out.position();
        out.put(ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
        int totalSizeOffs = out.position();
        out.putInt(-1);
        //isExpanded
        out.put((byte) 1);
        int openPartOffs = out.position();
        out.putInt(-1);
        //isExpanded, so num of open fields
        out.putInt(openPartOffs, out.position() - startOffs);
        out.putInt(count);
        int offsetAryPos = out.position();
        int offsetArySz = count * 2;
        //allocate space for open field offsets
        for (int i = 0; i < offsetArySz; i++) {
            out.putInt(0xDEADBEEF);
        }
        for (int i = 0; i < count; i++) {
            int offs = out.position() + out.arrayOffset();
            int relOffs = offs - startOffs;
            unpack(in, out, false);
            int hash = UTF8StringUtil.hash(out.array(), offs);
            out.putInt(offsetAryPos, hash);
            offsetAryPos += 4;
            out.putInt(offsetAryPos, relOffs);
            offsetAryPos += 4;
            unpack(in, out, true);
        }
        out.putInt(totalSizeOffs, out.position() - startOffs);
    }

    public static void unpackStr(ByteBuffer in, ByteBuffer out, long uLen, boolean tag) {
        //TODO: this probably breaks for 3 and 4 byte UTF-8
        if (tag) {
            out.put(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
        if (Long.compareUnsigned(uLen, Integer.MAX_VALUE) > 0) {
            throw new UnsupportedOperationException("String is too long");
        }
        int len = (int) uLen;
        int strLen = UTF8StringUtil.getStringLength(in.array(), in.position() + in.arrayOffset(), len);
        int adv = VarLenIntEncoderDecoder.encode(strLen, out.array(), out.position() + out.arrayOffset());
        out.position(out.position() + adv);
        System.arraycopy(in.array(), in.arrayOffset() + in.position(), out.array(), out.arrayOffset() + out.position(),
                len);
        out.position(out.position() + len);
        in.position(in.position() + len);
    }

}
