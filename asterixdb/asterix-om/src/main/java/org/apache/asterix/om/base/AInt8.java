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

import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AInt8 implements IAObject {

    protected byte value;

    public AInt8(byte value) {
        this.value = value;
    }

    @Override
    public IAType getType() {
        return BuiltinType.AINT8;
    }

    public byte getByteValue() {
        return value;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof AInt8)) {
            return false;
        }
        return value == ((AInt8) obj).getByteValue();
    }

    @Override
    public int hash() {
        return value;
    }

    @Override
    public String toString() {
        return Byte.toString(value);
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        json.put("AInt8", value);

        return json;
    }
}
