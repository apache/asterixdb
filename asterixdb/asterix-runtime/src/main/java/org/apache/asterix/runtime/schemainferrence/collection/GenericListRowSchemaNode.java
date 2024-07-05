
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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.schemainferrence.AbstractRowSchemaNestedNode;
import org.apache.asterix.runtime.schemainferrence.AbstractRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.IRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.ObjectRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.lazy.IObjectRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.primitive.PrimitiveRowSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

/*
A general node for collection types.
Holds the collection node and applies collection specific based
schema operation based on collection types.
 */
public class GenericListRowSchemaNode extends AbstractRowSchemaNestedNode {

    ObjectRowSchemaNode objectRowSchemaNode;
    MultisetRowSchemaNode multisetRowSchemaNode;
    ArrayRowSchemaNode arrayRowSchemaNode;
    PrimitiveRowSchemaNode flat;
    ATypeTag current;

    public GenericListRowSchemaNode(ATypeTag type, AbstractRowSchemaNode objectRowSchemaNode) {
        if (type == ATypeTag.OBJECT) {
            this.objectRowSchemaNode = (ObjectRowSchemaNode) objectRowSchemaNode;
        } else if (type == ATypeTag.MULTISET) {
            this.multisetRowSchemaNode = (MultisetRowSchemaNode) objectRowSchemaNode;
        } else if (type == ATypeTag.ARRAY) {
            this.arrayRowSchemaNode = (ArrayRowSchemaNode) objectRowSchemaNode;
        } else {
            this.flat = (PrimitiveRowSchemaNode) objectRowSchemaNode;
        }
        current = type;
    }

    @Override
    public ATypeTag getTypeTag() {
        return current;
    }

    @Override
    public IValueReference getFieldName() {
        return null;
    }

    @Override
    public void setFieldName(IValueReference newFieldName) {
        return;
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
    public <R, T> R accept(IRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return null;
    }

    public <R, T> R accept(IObjectRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        switch (current) {
            case OBJECT:
                return visitor.visit(objectRowSchemaNode, arg);
            case ARRAY:
                return visitor.visit(arrayRowSchemaNode, arg);
            case MULTISET:
                return visitor.visit(multisetRowSchemaNode, arg);
            default:
                return visitor.visit(flat, arg);
        }
    }

    @Override
    public void serialize(DataOutput output) throws IOException {

    }

    @Override
    public AbstractRowSchemaNode getChild(int i) {
        return null;
    }

    @Override
    public int getNumberOfChildren() {
        return 0;
    }

}
