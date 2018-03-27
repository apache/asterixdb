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

package org.apache.asterix.om.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AUUID implements IAObject {

    public static final int UUID_CHARS = 36;
    public static final int UUID_BYTES = 16;

    protected final byte[] uuidBytes;

    private static final char[] CHARS;

    static {
        CHARS = new char[16];
        for (int i = 0; i < 16; i++) {
            CHARS[i] = Character.forDigit(i, 16);
        }
    }

    protected AUUID() {
        this(new byte[UUID_BYTES]);
    }

    public AUUID(byte[] bytes) {
        this.uuidBytes = bytes;
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();
        json.put("AUUID", toString());
        return json;
    }

    @Override
    public IAType getType() {
        return BuiltinType.AUUID;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof AUUID)) {
            return false;
        }
        AUUID oUUID = (AUUID) obj;
        return Arrays.equals(oUUID.uuidBytes, this.uuidBytes);
    }

    @Override
    public int hash() {
        return Arrays.hashCode(uuidBytes);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(UUID_CHARS + 9);
        buf.append("uuid: { ");
        return appendLiteralOnly(buf).append(" }").toString();
    }

    public StringBuilder appendLiteralOnly(StringBuilder buf) {
        return appendLiteralOnly(uuidBytes, 0, buf);
    }

    private static StringBuilder digits(byte b[], int offset, int count, StringBuilder result) {
        for (int i = 0; i < count; i++) {
            result.append(CHARS[(b[offset + i] >> 4) & 0xf]);
            result.append(CHARS[b[offset + i] & 0xf]);
        }
        return result;
    }

    public static StringBuilder appendLiteralOnly(byte[] bytes, int offset, StringBuilder result) {
        digits(bytes, offset, 4, result).append('-');
        digits(bytes, offset + 4, 2, result).append('-');
        digits(bytes, offset + 6, 2, result).append('-');
        digits(bytes, offset + 8, 2, result).append('-');
        return digits(bytes, offset + 10, 6, result);
    }

    public void writeTo(DataOutput out) throws HyracksDataException {
        try {
            out.write(uuidBytes);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static AUUID readFrom(DataInput in) throws HyracksDataException {
        AUUID instance = new AUUID();
        try {
            in.readFully(instance.uuidBytes);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        return instance;
    }

}
