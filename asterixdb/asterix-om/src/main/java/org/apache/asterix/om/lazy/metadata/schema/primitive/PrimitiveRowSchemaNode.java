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
package org.apache.asterix.om.lazy.metadata.schema.primitive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.lazy.metadata.PathRowInfoSerializer;
import org.apache.asterix.om.lazy.metadata.schema.AbstractRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.IRowSchemaNodeVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class PrimitiveRowSchemaNode extends AbstractRowSchemaNode {
    private final int columnIndex;
    private final ATypeTag typeTag;
    private final boolean primaryKey;
    private ArrayBackedValueStorage fieldName;

    public PrimitiveRowSchemaNode(int columnIndex, ATypeTag typeTag, boolean primaryKey, ArrayBackedValueStorage fieldName) {
        this.columnIndex = columnIndex;
        this.typeTag = typeTag;
        this.primaryKey = primaryKey;
        this.fieldName = fieldName;
    }

    public PrimitiveRowSchemaNode(int columnIndex, ATypeTag typeTag, boolean primaryKey) {
        this.columnIndex = columnIndex;
        this.typeTag = typeTag;
        this.primaryKey = primaryKey;
        this.fieldName = fieldName;
    }

    public PrimitiveRowSchemaNode(ATypeTag typeTag, DataInput input) throws IOException {
        this.typeTag = typeTag;
        columnIndex = input.readInt();
        primaryKey = input.readBoolean();
    }

    public final int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public final ATypeTag getTypeTag() {
        return typeTag;
    }

    @Override
    public final boolean isNested() {
        return false;
    }

    @Override
    public final boolean isObjectOrCollection() {
        return false;
    }

    @Override
    public final boolean isCollection() {
        return false;
    }

    public final boolean isPrimaryKey() {
        return primaryKey;
    }

    public ArrayBackedValueStorage getFieldName() {
        return fieldName;
    }


    @Override
    public final <R, T> R accept(IRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    public void serialize(DataOutput output, PathRowInfoSerializer pathInfoSerializer) throws IOException {
        output.write(typeTag.serialize());
        output.writeInt(columnIndex);
        output.writeBoolean(primaryKey);
        pathInfoSerializer.writePathInfo(typeTag, columnIndex, primaryKey);
    }
}
