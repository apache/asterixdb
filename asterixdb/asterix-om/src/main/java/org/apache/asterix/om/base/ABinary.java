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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.visitors.IOMVisitor;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ABinary implements IAObject {

    private static final int HASH_PREFIX = 31;

    protected byte[] bytes;
    protected int start;
    protected int length;

    public ABinary(byte[] byteArray) {
        this.bytes = byteArray;
        this.start = 0;
        this.length = byteArray.length;
    }

    public ABinary(byte[] byteArray, int start, int length) {
        this.bytes = byteArray;
        this.start = start;
        this.length = length;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public int getStart() {
        return start;
    }

    public int getLength() {
        return length;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitABinary(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.ABINARY;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ABinary)) {
            return false;
        }
        byte[] x = ((ABinary) obj).getBytes();
        int xStart = ((ABinary) obj).getStart();
        int xLength = ((ABinary) obj).getLength();

        if (getLength() != xLength) {
            return false;
        }
        for (int k = 0; k < xLength; k++) {
            if (bytes[start + k] != x[xStart + k]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hash() {
        int m = Math.min(HASH_PREFIX, getLength());
        int h = 0;
        for (int i = 0; i < m; i++) {
            h += 31 * h + bytes[start + i];
        }
        return h;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int start = getStart();
        sb.append("ABinary: [ ");
        for (int i = 0; i < getLength(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(bytes[start + i]);
        }
        sb.append(" ]");
        return sb.toString();

    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        int start = getStart();
        JSONArray byteArray = new JSONArray();
        for (int i = 0; i < getLength(); i++) {
            byteArray.put(bytes[start + i]);
        }
        json.put("ABinary", byteArray);

        return json;
    }

}
