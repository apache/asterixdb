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
import org.apache.asterix.runtime.schemainferrence.lazy.IObjectRowSchemaNodeVisitor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

/*
A schema node for representing Asterix data types such as : String, Integer
 */
public class PrimitiveRowSchemaNode extends AbstractRowSchemaNode {
    private final ATypeTag typeTag;
    private final boolean primaryKey;
    private IValueReference fieldName;

    public PrimitiveRowSchemaNode(ATypeTag typeTag, boolean primaryKey) {

        this.typeTag = typeTag;
        this.primaryKey = primaryKey;

    }

    public PrimitiveRowSchemaNode(ATypeTag typeTag, DataInput input) throws IOException {
        this.typeTag = typeTag;

        primaryKey = input.readBoolean();

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
    public void serialize(DataOutput output) throws IOException {
        output.write(typeTag.serialize());

        output.writeBoolean(primaryKey);
    }

    @Override
    public AbstractRowSchemaNode getChild(int i) {
        return null;
    }

    @Override
    public int getNumberOfChildren() {
        return 0;
    }

    public final <R, T> R accept(IObjectRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }
}
