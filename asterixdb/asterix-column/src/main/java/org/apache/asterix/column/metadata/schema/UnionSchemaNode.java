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
package org.apache.asterix.column.metadata.schema;

import static org.apache.asterix.column.util.ColumnValuesUtil.getNormalizedTypeTag;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

import org.apache.asterix.column.metadata.PathInfoSerializer;
import org.apache.asterix.column.metadata.schema.primitive.MissingFieldSchemaNode;
import org.apache.asterix.column.metadata.schema.visitor.SchemaClipperVisitor;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class UnionSchemaNode extends AbstractSchemaNestedNode {
    private final AbstractSchemaNode originalType;
    private final Map<ATypeTag, AbstractSchemaNode> children;

    public UnionSchemaNode(AbstractSchemaNode child1, AbstractSchemaNode child2) {
        children = new EnumMap<>(ATypeTag.class);
        originalType = child1;
        // this is a new node, update the number of columns
        numberOfColumns = child1.getNumberOfColumns();
        previousNumberOfColumns = numberOfColumns; // this is an older node
        child2.getDeltaColumnsChanged();
        numberOfColumns += child2.getNumberOfColumns();
        putChild(child1);
        putChild(child2);
    }

    UnionSchemaNode(DataInput input, Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels)
            throws IOException {
        if (definitionLevels != null) {
            definitionLevels.put(this, new RunLengthIntArray());
        }
        ATypeTag originalTypeTag = ATypeTag.VALUE_TYPE_MAPPING[input.readByte()];
        int numberOfChildren = input.readInt();
        children = new EnumMap<>(ATypeTag.class);
        int columnsCount = 0;
        for (int i = 0; i < numberOfChildren; i++) {
            AbstractSchemaNode child = AbstractSchemaNode.deserialize(input, definitionLevels);
            columnsCount += child.getNumberOfColumns();
            children.put(child.getTypeTag(), child);
        }
        numberOfColumns = columnsCount;
        previousNumberOfColumns = numberOfColumns;
        originalType = children.get(originalTypeTag);
    }

    private void putChild(AbstractSchemaNode child) {
        children.put(child.getTypeTag(), child);
    }

    private void putChild(AbstractSchemaNode newChild, AbstractSchemaNode currentChild) {
        if (currentChild != null && newChild.getTypeTag() == currentChild.getTypeTag()) {
            numberOfColumns -= currentChild.getNumberOfColumns();
        }
        newChild.getDeltaColumnsChanged();
        numberOfColumns += newChild.getNumberOfColumns();
        children.put(newChild.getTypeTag(), newChild);
    }

    public AbstractSchemaNode getOriginalType() {
        return originalType;
    }

    public AbstractSchemaNode getOrCreateChild(ATypeTag childTypeTag, FlushColumnMetadata columnMetadata)
            throws HyracksDataException {
        ATypeTag normalizedTypeTag = getNormalizedTypeTag(childTypeTag);
        AbstractSchemaNode currentChild = children.get(normalizedTypeTag);
        //The parent of a union child should be the actual parent
        AbstractSchemaNode newChild = columnMetadata.getOrCreateChild(currentChild, normalizedTypeTag);
        if (currentChild != newChild) {
            putChild(newChild, currentChild);
        } else {
            numberOfColumns += newChild.getDeltaColumnsChanged();
        }
        return newChild;
    }

    public AbstractSchemaNode getChild(ATypeTag typeTag) {
        return children.getOrDefault(typeTag, MissingFieldSchemaNode.INSTANCE);
    }

    public Map<ATypeTag, AbstractSchemaNode> getChildren() {
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
    public <R, T> R accept(ISchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    public void serialize(DataOutput output, PathInfoSerializer pathInfoSerializer) throws IOException {
        output.write(ATypeTag.UNION.serialize());
        output.writeByte(originalType.getTypeTag().serialize());
        output.writeInt(children.size());
        pathInfoSerializer.enter(this);
        for (AbstractSchemaNode child : children.values()) {
            child.serialize(output, pathInfoSerializer);
        }
        pathInfoSerializer.exit(this);
    }

    /**
     * This would return any numeric node that has a different typeTag than the 'excludeTypeTag'
     *
     * @param excludeTypeTag exclude child with the provided {@link ATypeTag}
     * @return first numeric node or missing node
     * @see SchemaClipperVisitor
     */
    public AbstractSchemaNode getNumericChildOrMissing(ATypeTag excludeTypeTag) {
        for (AbstractSchemaNode child : children.values()) {
            ATypeTag childTypeTag = child.getTypeTag();
            boolean numeric = ATypeHierarchy.getTypeDomain(childTypeTag) == ATypeHierarchy.Domain.NUMERIC;
            if (numeric && childTypeTag != excludeTypeTag) {
                return child;
            }
        }
        return MissingFieldSchemaNode.INSTANCE;
    }

    public int getNumberOfNumericChildren() {
        int counter = 0;
        for (AbstractSchemaNode node : children.values()) {
            if (ATypeHierarchy.getTypeDomain(node.getTypeTag()) == ATypeHierarchy.Domain.NUMERIC) {
                counter++;
            }
        }

        return counter;
    }
}
