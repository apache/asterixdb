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
package org.apache.asterix.runtime.schemainferrence.collection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.RunRowLengthIntArray;
import org.apache.asterix.runtime.schemainferrence.AbstractRowSchemaNestedNode;
import org.apache.asterix.runtime.schemainferrence.AbstractRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.IRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.RowMetadata;
import org.apache.asterix.runtime.schemainferrence.Serialization.itemNodeSerialization;
import org.apache.asterix.runtime.schemainferrence.lazy.IObjectRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.lazy.metadata.PathRowInfoSerializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/*
Node that defines collection nodes methods. Collection schema nodes include Array and Multiset.
*/

public abstract class AbstractRowCollectionSchemaNode extends AbstractRowSchemaNestedNode {
    private AbstractRowSchemaNode item;

    @Override
    public IValueReference getFieldName() {
        return fieldName;
    }

    @Override
    public void setFieldName(IValueReference newFieldName) {
        fieldName = newFieldName;
    }

    private IValueReference fieldName;

    AbstractRowCollectionSchemaNode(IValueReference fieldName) {
        this.fieldName = fieldName;
        item = null;
    }

    AbstractRowCollectionSchemaNode(DataInput input,
            Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels) throws IOException {
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
        //        this.fieldName = fieldName;
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

    public final AbstractRowSchemaNode getOrCreateItem(ATypeTag childTypeTag, RowMetadata columnMetadata,
            IValueReference fieldName) throws HyracksDataException {
        AbstractRowSchemaNode newItem = columnMetadata.getOrCreateChild(item, childTypeTag, fieldName);
        if (newItem != item) {
            item = newItem;
        }
        return item;
    }

    @JsonSerialize(using = itemNodeSerialization.class)
    @JsonProperty("children")
    public final AbstractRowSchemaNode getItemNode() {
        return item;
    }

    @Override
    public final <R, T> R accept(IRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @JsonIgnore
    @Override
    public final boolean isObjectOrCollection() {
        return true;
    }

    @JsonIgnore
    @Override
    public final boolean isCollection() {
        return true;
    }

    @Override
    public final void serialize(DataOutput output, PathRowInfoSerializer pathInfoSerializer) throws IOException {

        output.write(getTypeTag().serialize());
        if (fieldName == null) {
            output.writeByte(0);
        } else {
            output.write(fieldName.getByteArray());
        }
        pathInfoSerializer.enter(this);
        item.serialize(output, pathInfoSerializer);
        pathInfoSerializer.exit(this);
    }

    public static AbstractRowCollectionSchemaNode create(ATypeTag typeTag) {
        IValueReference initFieldName = new ArrayBackedValueStorage();
        if (typeTag == ATypeTag.ARRAY) {
            return new ArrayRowSchemaNode(initFieldName);
        }
        return new MultisetRowSchemaNode(initFieldName);
    }

    public final <R, T> R accept(IObjectRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }
}
