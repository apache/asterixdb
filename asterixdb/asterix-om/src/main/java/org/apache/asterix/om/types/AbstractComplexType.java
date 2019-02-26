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
package org.apache.asterix.om.types;

import org.apache.asterix.om.base.IAObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class AbstractComplexType implements IAType {

    private static final long serialVersionUID = 1L;
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected static final String TYPE_NAME_FIELD = "typeName";
    protected String typeName;

    public AbstractComplexType(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public abstract void generateNestedDerivedTypeNames();

    @Override
    public boolean equals(Object object) {
        return object instanceof IAObject && deepEqual((IAObject) object);
    }

    protected void addToJson(final ObjectNode json) {
        json.put(TYPE_NAME_FIELD, typeName);
    }

    public abstract boolean containsType(IAType type);
}
