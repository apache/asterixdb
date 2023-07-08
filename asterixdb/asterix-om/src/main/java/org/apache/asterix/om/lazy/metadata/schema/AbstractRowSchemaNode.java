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
package org.apache.asterix.om.lazy.metadata.schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.asterix.om.lazy.metadata.PathRowInfoSerializer;
import org.apache.asterix.om.lazy.metadata.schema.collection.ArrayRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.collection.MultisetRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.primitive.MissingRowFieldSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.primitive.PrimitiveRowSchemaNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.RunRowLengthIntArray;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractRowSchemaNode {
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

    public abstract <R, T> R accept(IRowSchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException;

    public abstract void serialize(DataOutput output, PathRowInfoSerializer pathInfoSerializer) throws IOException;

    public static AbstractRowSchemaNode deserialize(DataInput input,
            Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels) throws IOException {
        ATypeTag typeTag = ATypeTag.VALUE_TYPE_MAPPING[input.readByte()];
        switch (typeTag) {
            case SYSTEM_NULL:
                return MissingRowFieldSchemaNode.INSTANCE;
            case OBJECT:
                return new ObjectRowSchemaNode(input, definitionLevels);
            case ARRAY:
                return new ArrayRowSchemaNode(input, definitionLevels);
            case MULTISET:
                return new MultisetRowSchemaNode(input, definitionLevels);
            case UNION:
                return new UnionRowSchemaNode(input, definitionLevels);
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
}
