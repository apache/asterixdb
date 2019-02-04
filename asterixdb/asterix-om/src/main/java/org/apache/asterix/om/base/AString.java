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

import java.io.Serializable;

import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AString implements IAObject, Serializable {
    private static final long serialVersionUID = -5482850888016554079L;

    protected String value;

    public AString(String value) {
        this.value = value;
    }

    public String getStringValue() {
        return value;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ASTRING;
    }

    @Override
    public String toString() {
        return "\"" + value + "\"";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AString)) {
            return false;
        }
        return value.equals(((AString) obj).getStringValue());
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public int hash() {
        return hashCode();
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        json.put("AString", value);

        return json;
    }
}
