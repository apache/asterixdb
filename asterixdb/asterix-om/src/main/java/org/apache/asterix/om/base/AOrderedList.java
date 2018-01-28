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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AOrderedList implements IACollection {

    protected List<IAObject> values;
    protected AOrderedListType type;

    public AOrderedList(AOrderedListType type) {
        values = new ArrayList<IAObject>();
        this.type = type;
    }

    public AOrderedList(AOrderedListType type, List<IAObject> sequence) {
        values = sequence;
        this.type = type;
    }

    public AOrderedList(List<String> sequence) {
        values = new ArrayList<IAObject>();
        for (int i = 0; i < sequence.size(); i++) {
            values.add(new AString(sequence.get(i)));
        }
        this.type = new AOrderedListType(BuiltinType.ASTRING, null);
    }

    public void add(IAObject obj) {
        values.add(obj);
    }

    @Override
    public IACursor getCursor() {
        ACollectionCursor cursor = new ACollectionCursor();
        cursor.reset(this);
        return cursor;
    }

    @Override
    public IAType getType() {
        return type;
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AOrderedList)) {
            return false;
        } else {
            AOrderedList y = (AOrderedList) o;
            return InMemUtils.cursorEquals(this.getCursor(), y.getCursor());
        }
    }

    @Override
    public int hashCode() {
        return InMemUtils.hashCursor(getCursor());
    }

    public IAObject getItem(int index) {
        return values.get(index);
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
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("array: [ ");
        boolean first = true;
        for (IAObject v : values) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(v.toString());
        }
        sb.append(" ]");
        return sb.toString();
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        ArrayNode list = om.createArrayNode();
        for (IAObject v : values) {
            list.add(v.toJSON());
        }
        json.set("AOrderedList", list);

        return json;
    }
}
