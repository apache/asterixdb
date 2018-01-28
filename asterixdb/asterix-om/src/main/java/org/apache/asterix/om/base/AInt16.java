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

public class AInt16 implements IAObject {

    protected short value;

    public AInt16(short value) {
        this.value = value;
    }

    @Override
    public IAType getType() {
        return BuiltinType.AINT16;
    }

    public short getShortValue() {
        return value;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof AInt16)) {
            return false;
        } else {
            return value == ((AInt16) obj).getShortValue();
        }
    }

    @Override
    public int hash() {
        return value;
    }

    @Override
    public String toString() {
        return Short.toString(value);
    }

    @Override
    public ObjectNode toJSON() {

        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        json.put("AInt16", value);

        return json;
    }
}
