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
package org.apache.asterix.runtime.schemainferrence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Map;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.RunRowLengthIntArray;
import org.apache.asterix.runtime.schemainferrence.Serialization.fieldNameSerialization;
import org.apache.asterix.runtime.schemainferrence.Serialization.mapSerialization;
import org.apache.asterix.runtime.schemainferrence.lazy.IObjectRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.lazy.metadata.PathRowInfoSerializer;
import org.apache.asterix.runtime.schemainferrence.primitive.MissingRowFieldSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonPropertyOrder({ "fieldName", "typeTag", "numberOfChildren", "children" })
public final class UnionRowSchemaNode extends AbstractRowSchemaNestedNode {
    private final AbstractRowSchemaNode originalType;
    private IValueReference fieldName;

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.UNION;
    }

    @Override
    public int getNumberOfChildren() {
        return children.size();
    }

    private final Map<ATypeTag, AbstractRowSchemaNode> children;

    public UnionRowSchemaNode(AbstractRowSchemaNode child1, AbstractRowSchemaNode child2) {
        children = new EnumMap<>(ATypeTag.class);
        originalType = child1;
        fieldName = originalType.getFieldName();
        child1.setFieldName(null);
        child2.setFieldName(null);
        putChild(child1);
        putChild(child2);
    }

    UnionRowSchemaNode(DataInput input, Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels)
            throws IOException {

        if (definitionLevels != null) {
            definitionLevels.put(this, new RunRowLengthIntArray());
        }
        ATypeTag originalTypeTag = ATypeTag.VALUE_TYPE_MAPPING[input.readByte()];

        ArrayBackedValueStorage fieldNameSize = new ArrayBackedValueStorage(1);
        input.readFully(fieldNameSize.getByteArray(), 0, 1);

        ArrayBackedValueStorage fieldNameBuffer = new ArrayBackedValueStorage(fieldNameSize.getByteArray()[0]);
        ArrayBackedValueStorage fieldName = new ArrayBackedValueStorage(fieldNameSize.getByteArray()[0] + 1);

        input.readFully(fieldNameBuffer.getByteArray(), 0, fieldNameSize.getByteArray()[0]);
        fieldName.append(fieldNameSize.getByteArray(), 0, 1);
        fieldName.append(fieldNameBuffer.getByteArray(), 0, fieldNameSize.getByteArray()[0]);
        if (fieldName.getByteArray()[0] == 0) {
            this.fieldName = null;
        } else {
            this.fieldName = fieldName;
        }
        int numberOfChildren = input.readInt();
        children = new EnumMap<>(ATypeTag.class);
        for (int i = 0; i < numberOfChildren; i++) {
            AbstractRowSchemaNode child = AbstractRowSchemaNode.deserialize(input, definitionLevels);
            children.put(child.getTypeTag(), child);
        }
        originalType = children.get(originalTypeTag);
    }

    public void putChild(AbstractRowSchemaNode child) {
        children.put(child.getTypeTag(), child);
    }

    @JsonIgnore
    public AbstractRowSchemaNode getOriginalType() {
        return originalType;
    }

    public AbstractRowSchemaNode getOrCreateChild(ATypeTag childTypeTag, RowMetadata columnMetadata,
            IValueReference fieldName) throws HyracksDataException {
        ATypeTag normalizedTypeTag = RowMetadata.getNormalizedTypeTag(childTypeTag);
        AbstractRowSchemaNode currentChild = children.get(normalizedTypeTag);
        //The parent of a union child should be the actual parent
        AbstractRowSchemaNode newChild = columnMetadata.getOrCreateChild(currentChild, normalizedTypeTag, fieldName);
        if (currentChild != newChild) {
            putChild(newChild);
        }
        return newChild;
    }

    public AbstractRowSchemaNode getChild(ATypeTag typeTag) {
        return children.getOrDefault(typeTag, MissingRowFieldSchemaNode.INSTANCE);
    }

    @JsonSerialize(using = mapSerialization.class)
    public Map<ATypeTag, AbstractRowSchemaNode> getChildren() {
        return children;
    }

    @JsonIgnore
    @Override
    public boolean isObjectOrCollection() {
        return false;
    }

    @JsonIgnore
    @Override
    public boolean isCollection() {
        return false;
    }

    @JsonSerialize(using = fieldNameSerialization.class)
    @Override
    public IValueReference getFieldName() {
        if (originalType != null) {
            return fieldName;
        }
        return new ArrayBackedValueStorage(1);
    }

    @Override
    public void setFieldName(IValueReference newFieldName) {
        fieldName = newFieldName;
    }

    @Override
    public <R, T> R accept(IRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    public void serialize(DataOutput output, PathRowInfoSerializer pathInfoSerializer) throws IOException {
        output.write(ATypeTag.UNION.serialize());
        output.writeByte(originalType.getTypeTag().serialize());
        if (fieldName == null) {
            output.writeByte(0);
        } else {
            output.write(fieldName.getByteArray());
        }
        output.writeInt(children.size());
        pathInfoSerializer.enter(this);
        for (AbstractRowSchemaNode child : children.values()) {
            child.serialize(output, pathInfoSerializer);
        }
        pathInfoSerializer.exit(this);
    }

    @Override
    public AbstractRowSchemaNode getChild(int i) {
        return null;
    }

    @JsonIgnore
    public ArrayList<AbstractRowSchemaNode> getChildrenList() {
        return new ArrayList<AbstractRowSchemaNode>(children.values());
    }

    /**
     * This would return any numeric node
     *
     * @return first numeric node or missing node\
     */
    @JsonIgnore
    public AbstractRowSchemaNode getNumericChildOrMissing() {
        for (AbstractRowSchemaNode node : children.values()) {
            if (ATypeHierarchy.getTypeDomain(node.getTypeTag()) == ATypeHierarchy.Domain.NUMERIC) {
                return node;
            }
        }
        return MissingRowFieldSchemaNode.INSTANCE;
    }

    @JsonIgnore
    public int getNumberOfNumericChildren() {
        int counter = 0;
        for (AbstractRowSchemaNode node : children.values()) {
            if (ATypeHierarchy.getTypeDomain(node.getTypeTag()) == ATypeHierarchy.Domain.NUMERIC) {
                counter++;
            }
        }

        return counter;
    }

    public final <R, T> R accept(IObjectRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }
}
