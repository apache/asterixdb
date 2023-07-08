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
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

import org.apache.asterix.om.RowMetadata;
import org.apache.asterix.om.lazy.metadata.PathRowInfoSerializer;
import org.apache.asterix.om.lazy.metadata.schema.primitive.MissingRowFieldSchemaNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.RunRowLengthIntArray;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class UnionRowSchemaNode extends AbstractRowSchemaNestedNode {
    private final AbstractRowSchemaNode originalType;
    private final Map<ATypeTag, AbstractRowSchemaNode> children;

    public UnionRowSchemaNode(AbstractRowSchemaNode child1, AbstractRowSchemaNode child2) {
        children = new EnumMap<>(ATypeTag.class);
        originalType = child1;
        putChild(child1);
        putChild(child2);
    }

    UnionRowSchemaNode(DataInput input, Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels)
            throws IOException {
        if (definitionLevels != null) {
            definitionLevels.put(this, new RunRowLengthIntArray());
        }
        ATypeTag originalTypeTag = ATypeTag.VALUE_TYPE_MAPPING[input.readByte()];
        int numberOfChildren = input.readInt();
        children = new EnumMap<>(ATypeTag.class);
        for (int i = 0; i < numberOfChildren; i++) {
            AbstractRowSchemaNode child = AbstractRowSchemaNode.deserialize(input, definitionLevels);
            children.put(child.getTypeTag(), child);
        }
        originalType = children.get(originalTypeTag);
    }

    private void putChild(AbstractRowSchemaNode child) {
        children.put(child.getTypeTag(), child);
    }

    public AbstractRowSchemaNode getOriginalType() {
        return originalType;
    }

    public AbstractRowSchemaNode getOrCreateChild(ATypeTag childTypeTag, RowMetadata columnMetadata)
            throws HyracksDataException {
        ATypeTag normalizedTypeTag = RowMetadata.getNormalizedTypeTag(childTypeTag);
        AbstractRowSchemaNode currentChild = children.get(normalizedTypeTag);
        //The parent of a union child should be the actual parent
        AbstractRowSchemaNode newChild = columnMetadata.getOrCreateChild(currentChild, normalizedTypeTag);
        if (currentChild != newChild) {
            putChild(newChild);
        }
        return newChild;
    }

    public AbstractRowSchemaNode getChild(ATypeTag typeTag) {
        return children.getOrDefault(typeTag, MissingRowFieldSchemaNode.INSTANCE);
    }

    public Map<ATypeTag, AbstractRowSchemaNode> getChildren() {
        return children;
    }

    @Override
    public boolean isObjectOrCollection() {
        return false;
    }

    @Override
    public boolean isCollection() {
        return false;
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.UNION;
    }

    @Override
    public <R, T> R accept(IRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    public void serialize(DataOutput output, PathRowInfoSerializer pathInfoSerializer) throws IOException {
        output.write(ATypeTag.UNION.serialize());
        output.writeByte(originalType.getTypeTag().serialize());
        output.writeInt(children.size());
        pathInfoSerializer.enter(this);
        for (AbstractRowSchemaNode child : children.values()) {
            child.serialize(output, pathInfoSerializer);
        }
        pathInfoSerializer.exit(this);
    }

    /**
     * This would return any numeric node
     *
     * @return first numeric node or missing node
     * @see SchemaClipperVisitor
     */
    public AbstractRowSchemaNode getNumericChildOrMissing() {
        for (AbstractRowSchemaNode node : children.values()) {
            if (ATypeHierarchy.getTypeDomain(node.getTypeTag()) == ATypeHierarchy.Domain.NUMERIC) {
                return node;
            }
        }
        return MissingRowFieldSchemaNode.INSTANCE;
    }

    public int getNumberOfNumericChildren() {
        int counter = 0;
        for (AbstractRowSchemaNode node : children.values()) {
            if (ATypeHierarchy.getTypeDomain(node.getTypeTag()) == ATypeHierarchy.Domain.NUMERIC) {
                counter++;
            }
        }

        return counter;
    }
}
