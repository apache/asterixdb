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

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.RecordUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ARecord implements IAObject {
    public static final ARecord EMPTY_OPEN_RECORD = new ARecord(RecordUtil.FULLY_OPEN_RECORD_TYPE, new IAObject[] {});

    protected ARecordType type;
    protected IAObject[] fields;

    public ARecord(ARecordType type, IAObject[] fields) {
        this.type = type;
        this.fields = fields;
    }

    @Override
    public ARecordType getType() {
        return type;
    }

    public boolean isOpen() {
        return type.isOpen();
    }

    // efficient way of retrieving the value of a field; pos starts from 0
    public IAObject getValueByPos(int pos) {
        return fields[pos];
    }

    public int numberOfFields() {
        return fields.length;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ARecord)) {
            return false;
        }
        ARecord r = (ARecord) obj;
        if (!type.deepEqual(r.type)) {
            return false;
        }
        return InMemUtils.deepEqualArrays(fields, r.fields);
    }

    @Override
    public int hash() {
        int h = 0;
        for (int i = 0; i < fields.length; i++) {
            h += 31 * h + fields[i].hash();
        }
        return h;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        if (fields != null) {
            for (int i = 0; i < fields.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(type.getFieldNames()[i]);
                sb.append(": ");
                sb.append(fields[i]);
            }
        }
        sb.append(" }");
        return sb.toString();
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        ArrayNode record = om.createArrayNode();
        for (int i = 0; i < fields.length; i++) {
            ObjectNode item = om.createObjectNode();
            item.set(type.getFieldNames()[i], fields[i].toJSON());
            record.add(item);
        }
        json.set("ARecord", record);

        return json;
    }
}
