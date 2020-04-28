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
package org.apache.asterix.external.library.java.base;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.api.IJType;
import org.apache.asterix.om.base.AMutableOrderedList;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public final class JOrderedList extends JList<List<? extends Object>> {

    private AOrderedListType listType;

    public JOrderedList() {
        this(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
    }

    public JOrderedList(IJType listItemType) {
        super();
        jObjects = new ArrayList<>();
        this.listType = new AOrderedListType(listItemType.getIAType(), null);
    }

    public JOrderedList(IAType listItemType) {
        super();
        jObjects = new ArrayList<>();
        this.listType = new AOrderedListType(listItemType, null);
    }

    public List<? extends Object> getValueGeneric() {
        return (ArrayList) jObjects;
    }

    public List<IJObject> getValue() {
        return (List) jObjects;
    }

    @Override
    public IAType getIAType() {
        return listType;
    }

    @Override
    public IAObject getIAObject() {
        AMutableOrderedList v = new AMutableOrderedList(listType);
        for (IJObject jObj : jObjects) {
            v.add(jObj.getIAObject());
        }
        return v;
    }

    @Override
    public void setValueGeneric(List<? extends Object> vals) throws HyracksDataException {
        reset();
        if (vals.size() > 0) {
            Object first = vals.get(0);
            IAType asxClass = JObject.convertType(first.getClass());
            IJObject obj = pool.allocate(asxClass);
            obj.setValueGeneric(first);
            IAType listType = obj.getIAType();
            this.listType = new AOrderedListType(listType, "");
        }
        for (Object v : vals) {
            IAType asxClass = JObject.convertType(v.getClass());
            IJObject obj = pool.allocate(asxClass);
            obj.setValueGeneric(v);
            add(obj);
        }

    }

    @Override
    public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
        IAsterixListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(listType);
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        for (IJObject jObject : jObjects) {
            fieldValue.reset();
            jObject.serialize(fieldValue.getDataOutput(), writeTypeTag);
            listBuilder.addItem(fieldValue);
        }
        listBuilder.write(dataOutput, writeTypeTag);

    }

    @Override
    public void reset() {
        jObjects.clear();
    }

}
