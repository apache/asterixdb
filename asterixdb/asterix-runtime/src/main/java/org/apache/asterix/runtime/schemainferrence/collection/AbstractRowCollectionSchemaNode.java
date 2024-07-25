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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.schemainferrence.AbstractRowSchemaNestedNode;
import org.apache.asterix.runtime.schemainferrence.AbstractRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.IRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.RowMetadata;
import org.apache.asterix.runtime.schemainferrence.lazy.IObjectRowSchemaNodeVisitor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

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

    AbstractRowCollectionSchemaNode(DataInput input) throws IOException {
        item = AbstractRowSchemaNestedNode.deserialize(input);
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

    public final AbstractRowSchemaNode getItemNode() {
        return item;
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
    public final void serialize(DataOutput output) throws IOException {

        output.write(getTypeTag().serialize());
        item.serialize(output);
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
