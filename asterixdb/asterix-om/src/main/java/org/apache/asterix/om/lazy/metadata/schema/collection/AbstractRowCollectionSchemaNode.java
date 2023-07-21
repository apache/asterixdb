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
package org.apache.asterix.om.lazy.metadata.schema.collection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.asterix.om.RowMetadata;
import org.apache.asterix.om.lazy.metadata.PathRowInfoSerializer;
import org.apache.asterix.om.lazy.metadata.schema.AbstractRowSchemaNestedNode;
import org.apache.asterix.om.lazy.metadata.schema.AbstractRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.IRowSchemaNodeVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.RunRowLengthIntArray;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractRowCollectionSchemaNode extends AbstractRowSchemaNestedNode {
    private AbstractRowSchemaNode item;

    AbstractRowCollectionSchemaNode() {
        item = null;
    }

    AbstractRowCollectionSchemaNode(DataInput input,
            Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels) throws IOException {
        if (definitionLevels != null) {
            definitionLevels.put(this, new RunRowLengthIntArray());
        }
        item = AbstractRowSchemaNestedNode.deserialize(input, definitionLevels);
    }

    public final AbstractRowSchemaNode getOrCreateItem(ATypeTag childTypeTag, RowMetadata columnMetadata)
            throws HyracksDataException {
        AbstractRowSchemaNode newItem = columnMetadata.getOrCreateChild(item, childTypeTag);
        if (newItem != item) {
            item = newItem;
        }
        return item;
    }

    public final AbstractRowSchemaNode getItemNode() {
        return item;
    }

    public final void setItemNode(AbstractRowSchemaNode item) {
        this.item = item;
    }

    @Override
    public final <R, T> R accept(IRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
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
    public final void serialize(DataOutput output, PathRowInfoSerializer pathInfoSerializer) throws IOException {
        output.write(getTypeTag().serialize());
        pathInfoSerializer.enter(this);
        item.serialize(output, pathInfoSerializer);
        pathInfoSerializer.exit(this);
    }

    public static AbstractRowCollectionSchemaNode create(ATypeTag typeTag) {
        ArrayBackedValueStorage initFieldName = new ArrayBackedValueStorage();
        if (typeTag == ATypeTag.ARRAY) {
            return new ArrayRowSchemaNode(initFieldName);
        }
        return new MultisetRowSchemaNode(initFieldName);
    }
}
