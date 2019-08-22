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

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * An adm object instance
 */
public class AdmObjectNode implements IAdmNode {

    private static final long serialVersionUID = 1L;
    public static final AdmObjectNode EMPTY = new AdmObjectNode(Collections.emptyMap());
    private final Map<String, IAdmNode> children;

    public AdmObjectNode() {
        children = new HashMap<>();
    }

    public AdmObjectNode(Map<String, IAdmNode> children) {
        this.children = children;
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
        if (children.containsKey(fieldName)) {
            // TODO(ali): find a way to issue a warning
            return this;
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

    public static AdmObjectNode from(Map<String, IAdmNode> fields) {
        return fields.isEmpty() ? EMPTY : new AdmObjectNode(fields);
    }

    public boolean isEmpty() {
        return children.isEmpty();
    }

    @Override
    public void serializeValue(DataOutput dataOutput) throws IOException {
        ISerializerDeserializer<AString> stringSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
        RecordBuilder confRecordBuilder = new RecordBuilder();
        confRecordBuilder.reset(RecordUtil.FULLY_OPEN_RECORD_TYPE);
        confRecordBuilder.init();
        ArrayBackedValueStorage fieldNameBytes = new ArrayBackedValueStorage();
        ArrayBackedValueStorage fieldValueBytes = new ArrayBackedValueStorage();
        for (Entry<String, IAdmNode> field : getFields()) {
            String fieldName = field.getKey();
            fieldValueBytes.reset();
            fieldNameBytes.reset();
            stringSerde.serialize(new AString(fieldName), fieldNameBytes.getDataOutput());
            IAdmNode value = field.getValue();
            value.serialize(fieldValueBytes.getDataOutput());
            confRecordBuilder.addField(fieldNameBytes, fieldValueBytes);
        }
        confRecordBuilder.write(dataOutput, false);
    }

    public boolean contains(String fieldName) {
        return children.containsKey(fieldName);
    }

    public String getString(String field) throws HyracksDataException {
        return getString(this, field);
    }

    public String getOptionalString(String field) {
        final IAdmNode node = get(field);
        if (node == null) {
            return null;
        }
        return ((AdmStringNode) node).get();
    }

    public static String getString(AdmObjectNode openFields, String field) throws HyracksDataException {
        IAdmNode node = openFields.get(field);
        if (node == null) {
            throw HyracksDataException.create(ErrorCode.FIELD_NOT_FOUND, field);
        } else if (node.getType() != ATypeTag.STRING) {
            throw HyracksDataException.create(ErrorCode.FIELD_NOT_OF_TYPE, field, ATypeTag.STRING, node.getType());
        }
        return ((AdmStringNode) node).get();
    }
}
