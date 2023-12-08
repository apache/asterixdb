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
import java.io.IOException;
import java.util.Map;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.RunRowLengthIntArray;
import org.apache.asterix.runtime.schemainferrence.AbstractRowSchemaNestedNode;
import org.apache.asterix.runtime.schemainferrence.AbstractRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.Serialization.fieldNameSerialization;
import org.apache.hyracks.data.std.api.IValueReference;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/*
Schema node for Multiset types.
*/

public final class MultisetRowSchemaNode extends AbstractRowCollectionSchemaNode {
    private IValueReference fieldName;

    public MultisetRowSchemaNode(IValueReference fieldName) {
        super(fieldName);
        this.fieldName = fieldName;
    }

    public MultisetRowSchemaNode(DataInput input,
            Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels) throws IOException {
        super(input, definitionLevels);
        this.fieldName = super.getFieldName();
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.MULTISET;
    }

    @JsonSerialize(using = fieldNameSerialization.class)
    @Override
    public IValueReference getFieldName() {
        return fieldName;
    }

    @Override
    public void setFieldName(IValueReference newFieldName) {
        fieldName = newFieldName;
    }

    @Override
    public AbstractRowSchemaNode getChild(int i) {
        return getItemNode().getChild(i);
    }

    @JsonIgnore
    @Override
    public int getNumberOfChildren() {
        return getItemNode().getNumberOfChildren();
    }

    @JsonIgnore
    public ATypeTag getItemTypeTag() {
        return getItemNode().getTypeTag();
    }
}
