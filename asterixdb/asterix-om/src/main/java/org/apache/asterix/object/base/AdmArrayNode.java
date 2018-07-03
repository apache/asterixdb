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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class AdmArrayNode implements IAdmNode, Iterable<IAdmNode> {
    private static final long serialVersionUID = 1L;
    private final List<IAdmNode> children;

    public AdmArrayNode() {
        children = new ArrayList<>();
    }

    public AdmArrayNode(int initialCapacity) {
        children = new ArrayList<>(initialCapacity);
    }

    public IAdmNode set(int index, boolean value) {
        return set(index, AdmBooleanNode.get(value));
    }

    public void add(boolean value) {
        add(AdmBooleanNode.get(value));
    }

    public int size() {
        return children.size();
    }

    public void add(IAdmNode value) {
        if (value == null) {
            value = AdmNullNode.INSTANCE; // NOSONAR
        }
        children.add(value);
    }

    public IAdmNode set(int index, IAdmNode value) {
        if (value == null) {
            value = AdmNullNode.INSTANCE; // NOSONAR
        }
        return children.set(index, value);
    }

    public IAdmNode get(int index) {
        return children.get(index);
    }

    @Override
    public ATypeTag getType() {
        return ATypeTag.ARRAY;
    }

    @Override
    public void reset() {
        children.clear();
    }

    @Override
    public String toString() {
        return children.toString();
    }

    @Override
    public void serializeValue(DataOutput dataOutput) throws IOException {
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        for (int i = 0; i < children.size(); i++) {
            itemValue.reset();
            IAdmNode next = children.get(i);
            next.serialize(itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        listBuilder.write(dataOutput, false);
    }

    @Override
    public Iterator<IAdmNode> iterator() {
        return children.iterator();
    }
}
