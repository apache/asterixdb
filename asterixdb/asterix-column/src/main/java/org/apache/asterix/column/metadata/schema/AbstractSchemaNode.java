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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.asterix.column.metadata.PathInfoSerializer;
import org.apache.asterix.column.metadata.schema.collection.ArraySchemaNode;
import org.apache.asterix.column.metadata.schema.collection.MultisetSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.MissingFieldSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractSchemaNode {
    private int counter;

    public abstract ATypeTag getTypeTag();

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

    public abstract <R, T> R accept(ISchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException;

    public abstract void serialize(DataOutput output, PathInfoSerializer pathInfoSerializer) throws IOException;

    public static AbstractSchemaNode deserialize(DataInput input,
            Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels) throws IOException {
        ATypeTag typeTag = ATypeTag.VALUE_TYPE_MAPPING[input.readByte()];
        switch (typeTag) {
            case SYSTEM_NULL:
                return MissingFieldSchemaNode.INSTANCE;
            case OBJECT:
                return new ObjectSchemaNode(input, definitionLevels);
            case ARRAY:
                return new ArraySchemaNode(input, definitionLevels);
            case MULTISET:
                return new MultisetSchemaNode(input, definitionLevels);
            case UNION:
                return new UnionSchemaNode(input, definitionLevels);
            case NULL:
            case MISSING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case UUID:
                return new PrimitiveSchemaNode(typeTag, input);
            default:
                throw new UnsupportedEncodingException(typeTag + " is not supported");
        }
    }
}
