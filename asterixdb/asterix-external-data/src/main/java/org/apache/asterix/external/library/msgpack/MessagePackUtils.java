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

import static org.msgpack.core.MessagePack.Code.FIXARRAY_PREFIX;
import static org.msgpack.core.MessagePack.Code.FIXSTR_PREFIX;
import static org.msgpack.core.MessagePack.Code.STR32;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class MessagePackUtils {

    public static ATypeTag peekUnknown(IAType type) {
        switch (type.getTypeTag()) {
            case MISSING:
            case NULL:
                return type.getTypeTag();
            default:
                return ATypeTag.TYPE;
        }
    }

    public static void packFixPos(ByteBuffer buf, byte in) throws HyracksDataException {
        byte mask = (byte) (1 << 7);
        if ((in & mask) != 0) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "fixint7 must be positive");
        }
        buf.put(in);
    }

    public static void packFixStr(ByteBuffer buf, String in) throws HyracksDataException {
        byte[] strBytes = in.getBytes(StandardCharsets.UTF_8);
        if (strBytes.length > 31) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "fixint7 must be positive");
        }
        buf.put((byte) (FIXSTR_PREFIX + strBytes.length));
        buf.put(strBytes);
    }

    public static void packStr(ByteBuffer out, String in) {
        out.put(STR32);
        byte[] strBytes = in.getBytes(StandardCharsets.UTF_8);
        out.putInt(strBytes.length);
        out.put(strBytes);
    }

    public static void packFixArrayHeader(ByteBuffer buf, byte numObj) {
        buf.put((byte) (FIXARRAY_PREFIX + (0x0F & numObj)));
    }
}
