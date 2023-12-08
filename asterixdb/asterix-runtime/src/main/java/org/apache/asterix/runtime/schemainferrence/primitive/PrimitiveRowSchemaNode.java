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
package org.apache.asterix.runtime.schemainferrence.primitive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.schemainferrence.AbstractRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.IRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.Serialization.fieldNameSerialization;
import org.apache.asterix.runtime.schemainferrence.lazy.IObjectRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.lazy.metadata.PathRowInfoSerializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/*
A schema node for representing Asterix data types such as : String, Integer
 */
@JsonPropertyOrder({ "fieldName", "typeTag" })
public class PrimitiveRowSchemaNode extends AbstractRowSchemaNode {
    @JsonIgnore
    private final int columnIndex;
    private final ATypeTag typeTag;
    private final boolean primaryKey;
    //    private ArrayBackedValueStorage fieldName;
    private IValueReference fieldName;

    public PrimitiveRowSchemaNode(int columnIndex, ATypeTag typeTag, boolean primaryKey, IValueReference fieldName) {
        this.columnIndex = columnIndex;
        this.typeTag = typeTag;
        this.primaryKey = primaryKey;
        this.fieldName = fieldName;
    }

    public PrimitiveRowSchemaNode(int columnIndex, ATypeTag typeTag, boolean primaryKey) {
        this.columnIndex = columnIndex;
        this.typeTag = typeTag;
        this.primaryKey = primaryKey;
        //        this.fieldName = fieldName;
    }

    public PrimitiveRowSchemaNode(ATypeTag typeTag, DataInput input) throws IOException {
        this.typeTag = typeTag;
        columnIndex = input.readInt();
        primaryKey = input.readBoolean();

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

    }

    public final int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public final ATypeTag getTypeTag() {
        return typeTag;
    }

    @JsonIgnore
    @Override
    public final boolean isNested() {
        return false;
    }

    @JsonIgnore
    @Override
    public final boolean isObjectOrCollection() {
        return false;
    }

    @JsonIgnore
    @Override
    public final boolean isCollection() {
        return false;
    }

    @JsonIgnore
    public final boolean isPrimaryKey() {
        return primaryKey;
    }

    @JsonSerialize(using = fieldNameSerialization.class)
    public IValueReference getFieldName() {
        return fieldName;
    }

    @Override
    public void setFieldName(IValueReference newFieldName) {
        fieldName = newFieldName;
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
        if (fieldName == null) {
            output.writeByte(0);
        } else {
            output.write(fieldName.getByteArray());
        }
        pathInfoSerializer.writePathInfo(typeTag, columnIndex, primaryKey);
    }

    @Override
    public AbstractRowSchemaNode getChild(int i) {
        return null;
    }

    @JsonIgnore
    @Override
    public int getNumberOfChildren() {
        return 0;
    }

    public final <R, T> R accept(IObjectRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }
}
