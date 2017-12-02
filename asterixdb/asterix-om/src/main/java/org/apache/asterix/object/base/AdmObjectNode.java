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
package org.apache.asterix.object.base;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.asterix.om.types.ATypeTag;

/**
 * An adm object instance
 */
public class AdmObjectNode implements IAdmNode {
    private final Map<String, IAdmNode> children;

    public AdmObjectNode() {
        children = new HashMap<>();
    }

    @Override
    public ATypeTag getType() {
        return ATypeTag.OBJECT;
    }

    public int size() {
        return children.size();
    }

    public IAdmNode get(String fieldName) {
        return children.get(fieldName);
    }

    public Set<String> getFieldNames() {
        return children.keySet();
    }

    public Set<Entry<String, IAdmNode>> getFields() {
        return children.entrySet();
    }

    public AdmObjectNode set(String fieldName, IAdmNode value) {
        if (value == null) {
            value = AdmNullNode.INSTANCE; // NOSONAR
        }
        children.put(fieldName, value);
        return this;
    }

    public IAdmNode remove(String fieldName) {
        return children.remove(fieldName);
    }

    @Override
    public void reset() {
        children.clear();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        int count = 0;
        for (Map.Entry<String, IAdmNode> en : children.entrySet()) {
            if (count > 0) {
                sb.append(",");
            }
            ++count;
            sb.append('"');
            sb.append(en.getKey());
            sb.append('"');
            sb.append(':');
            sb.append(en.getValue().toString());
        }
        sb.append("}");
        return sb.toString();
    }
}
