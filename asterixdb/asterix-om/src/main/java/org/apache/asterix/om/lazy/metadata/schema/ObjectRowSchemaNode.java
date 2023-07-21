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
package org.apache.asterix.om.lazy.metadata.schema;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.RowMetadata;
import org.apache.asterix.om.lazy.metadata.PathRowInfoSerializer;
import org.apache.asterix.om.lazy.metadata.schema.primitive.MissingRowFieldSchemaNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.RunRowLengthIntArray;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.annotations.CriticalPath;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntImmutableList;
import it.unimi.dsi.fastutil.ints.IntList;

public final class ObjectRowSchemaNode extends AbstractRowSchemaNestedNode {
    private final Int2IntMap fieldNameIndexToChildIndexMap;
    private final List<AbstractRowSchemaNode> children;

    private ArrayBackedValueStorage fieldName;
    public ObjectRowSchemaNode(ArrayBackedValueStorage fieldName) {
        fieldNameIndexToChildIndexMap = new Int2IntOpenHashMap();
        children = new ArrayList<>();
        this.fieldName = fieldName;
    }

    public ObjectRowSchemaNode() {
        fieldNameIndexToChildIndexMap = new Int2IntOpenHashMap();
        children = new ArrayList<>();
    }

    ObjectRowSchemaNode(DataInput input, Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels)
            throws IOException {
        if (definitionLevels != null) {
            definitionLevels.put(this, new RunRowLengthIntArray());
        }
        int numberOfChildren = input.readInt();

        fieldNameIndexToChildIndexMap = new Int2IntOpenHashMap();
        deserializeFieldNameIndexToChildIndex(input, fieldNameIndexToChildIndexMap, numberOfChildren);

        children = new ArrayList<>();
        deserializeChildren(input, children, numberOfChildren, definitionLevels);
    }

    public AbstractRowSchemaNode getOrCreateChild(IValueReference fieldName, ATypeTag childTypeTag,
            RowMetadata columnMetadata) throws HyracksDataException {
        int numberOfChildren = children.size();
        int fieldNameIndex = columnMetadata.getFieldNamesDictionary().getOrCreateFieldNameIndex(fieldName);
        int childIndex = fieldNameIndexToChildIndexMap.getOrDefault(fieldNameIndex, numberOfChildren);
        AbstractRowSchemaNode currentChild = childIndex == numberOfChildren ? null : children.get(childIndex);
        ArrayBackedValueStorage fieldNameProp = new ArrayBackedValueStorage(fieldName.getLength());
        fieldNameProp.append(fieldName);
        AbstractRowSchemaNode newChild = columnMetadata.getOrCreateChild(currentChild, childTypeTag,fieldNameProp);
        if (currentChild == null) {
            children.add(childIndex, newChild);
            fieldNameIndexToChildIndexMap.put(fieldNameIndex, childIndex);
        } else if (currentChild != newChild) {
            children.set(childIndex, newChild);
        }

        return newChild;
    }

    public void addChild(int fieldNameIndex, AbstractRowSchemaNode child) {
        int childIndex = children.size();
        fieldNameIndexToChildIndexMap.put(fieldNameIndex, childIndex);
        children.add(child);
    }

    public AbstractRowSchemaNode getChild(int fieldNameIndex) {
        if (fieldNameIndexToChildIndexMap.containsKey(fieldNameIndex)) {
            return children.get(fieldNameIndexToChildIndexMap.get(fieldNameIndex));
        }
        return MissingRowFieldSchemaNode.INSTANCE;
    }

    public void removeChild(int fieldNameIndex) {
        int childIndex = fieldNameIndexToChildIndexMap.remove(fieldNameIndex);
        children.remove(childIndex);
    }

    public List<AbstractRowSchemaNode> getChildren() {
        return children;
    }

    /**
     * Should not be used in a {@link CriticalPath}
     */
    public IntList getChildrenFieldNameIndexes() {
        return IntImmutableList.toList(fieldNameIndexToChildIndexMap.int2IntEntrySet().stream()
                .sorted(Comparator.comparingInt(Entry::getIntValue)).mapToInt(Entry::getIntKey));
    }

    public boolean containsField(int fieldNameIndex) {
        return fieldNameIndexToChildIndexMap.containsKey(fieldNameIndex);
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.OBJECT;
    }

    @Override
    public boolean isObjectOrCollection() {
        return true;
    }

    @Override
    public boolean isCollection() {
        return false;
    }

    @Override
    public <R, T> R accept(IRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    public void serialize(DataOutput output, PathRowInfoSerializer pathInfoSerializer) throws IOException {
        output.write(ATypeTag.OBJECT.serialize());
        output.writeInt(children.size());
        for (Entry fieldNameIndexChildIndex : fieldNameIndexToChildIndexMap.int2IntEntrySet()) {
            output.writeInt(fieldNameIndexChildIndex.getIntKey());
            output.writeInt(fieldNameIndexChildIndex.getIntValue());
        }
        pathInfoSerializer.enter(this);
        for (AbstractRowSchemaNode child : children) {
            child.serialize(output, pathInfoSerializer);
        }
        pathInfoSerializer.exit(this);
    }

    public void abort(DataInputStream input, Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels)
            throws IOException {
        definitionLevels.put(this, new RunRowLengthIntArray());

        int numberOfChildren = input.readInt();

        fieldNameIndexToChildIndexMap.clear();
        deserializeFieldNameIndexToChildIndex(input, fieldNameIndexToChildIndexMap, numberOfChildren);

        children.clear();
        deserializeChildren(input, children, numberOfChildren, definitionLevels);
    }

    private static void deserializeFieldNameIndexToChildIndex(DataInput input, Int2IntMap fieldNameIndexToChildIndexMap,
            int numberOfChildren) throws IOException {
        for (int i = 0; i < numberOfChildren; i++) {
            int fieldNameIndex = input.readInt();
            int childIndex = input.readInt();
            fieldNameIndexToChildIndexMap.put(fieldNameIndex, childIndex);
        }
    }

    private static void deserializeChildren(DataInput input, List<AbstractRowSchemaNode> children, int numberOfChildren,
            Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels) throws IOException {
        for (int i = 0; i < numberOfChildren; i++) {
            children.add(AbstractRowSchemaNode.deserialize(input, definitionLevels));
        }
    }
}
