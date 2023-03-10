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
package org.apache.asterix.column.metadata.schema.collection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.asterix.column.metadata.PathInfoSerializer;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ISchemaNodeVisitor;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractCollectionSchemaNode extends AbstractSchemaNestedNode {
    private AbstractSchemaNode item;

    AbstractCollectionSchemaNode() {
        item = null;
    }

    AbstractCollectionSchemaNode(DataInput input, Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels)
            throws IOException {
        if (definitionLevels != null) {
            definitionLevels.put(this, new RunLengthIntArray());
        }
        item = AbstractSchemaNode.deserialize(input, definitionLevels);
    }

    public final AbstractSchemaNode getOrCreateItem(ATypeTag childTypeTag, FlushColumnMetadata columnMetadata)
            throws HyracksDataException {
        AbstractSchemaNode newItem = columnMetadata.getOrCreateChild(item, childTypeTag);
        if (newItem != item) {
            item = newItem;
        }
        return item;
    }

    public final AbstractSchemaNode getItemNode() {
        return item;
    }

    public final void setItemNode(AbstractSchemaNode item) {
        this.item = item;
    }

    @Override
    public final <R, T> R accept(ISchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    public final boolean isObjectOrCollection() {
        return true;
    }

    @Override
    public final boolean isCollection() {
        return true;
    }

    @Override
    public final void serialize(DataOutput output, PathInfoSerializer pathInfoSerializer) throws IOException {
        output.write(getTypeTag().serialize());
        pathInfoSerializer.enter(this);
        item.serialize(output, pathInfoSerializer);
        pathInfoSerializer.exit(this);
    }

    public static AbstractCollectionSchemaNode create(ATypeTag typeTag) {
        if (typeTag == ATypeTag.ARRAY) {
            return new ArraySchemaNode();
        }

        return new MultisetSchemaNode();
    }
}
