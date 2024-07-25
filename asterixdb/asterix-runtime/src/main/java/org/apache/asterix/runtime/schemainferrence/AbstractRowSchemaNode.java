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
package org.apache.asterix.runtime.schemainferrence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.schemainferrence.collection.ArrayRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.collection.MultisetRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.lazy.IObjectRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.primitive.MissingRowFieldSchemaNode;
import org.apache.asterix.runtime.schemainferrence.primitive.PrimitiveRowSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public abstract class AbstractRowSchemaNode {
    private int counter;

    public abstract ATypeTag getTypeTag();

    public abstract IValueReference getFieldName();

    public abstract void setFieldName(IValueReference newFieldName);

    public abstract boolean isNested();

    public abstract boolean isObjectOrCollection();

    public abstract boolean isCollection();

    public final void incrementCounter() {
        counter++;
    }

    public final void setCounter(int counter) {
        this.counter = counter;
    }

    public final int getCounter() {
        return counter;
    }

    public abstract <R, T> R accept(IRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException;

    public abstract <R, T> R accept(IObjectRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException;

    public abstract void serialize(DataOutput output) throws IOException;

    public static AbstractRowSchemaNode deserialize(DataInput input) throws IOException {
        ATypeTag typeTag = ATypeTag.VALUE_TYPE_MAPPING[input.readByte()];
        switch (typeTag) {
            case SYSTEM_NULL:
                return MissingRowFieldSchemaNode.INSTANCE;
            case OBJECT:
                return new ObjectRowSchemaNode(input);
            case ARRAY:
                return new ArrayRowSchemaNode(input);
            case MULTISET:
                return new MultisetRowSchemaNode(input);
            case UNION:
                return new UnionRowSchemaNode(input);
            case NULL:
            case MISSING:
            case BOOLEAN:
            case BIGINT:
            case DOUBLE:
            case STRING:
            case UUID:
                return new PrimitiveRowSchemaNode(typeTag, input);
            default:
                throw new UnsupportedEncodingException(typeTag + " is not supported");
        }
    }

    public abstract AbstractRowSchemaNode getChild(int i);

    public abstract int getNumberOfChildren();
}
