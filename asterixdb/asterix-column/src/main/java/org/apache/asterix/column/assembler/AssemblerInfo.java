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
package org.apache.asterix.column.assembler;

import static org.apache.asterix.om.typecomputer.impl.TypeComputeUtils.getActualType;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;

public class AssemblerInfo {
    private final AbstractNestedValueAssembler parent;
    private final IAType declaredType;
    private final boolean delegate;
    private final IValueReference fieldName;
    private final int fieldIndex;

    public AssemblerInfo() {
        this(BuiltinType.ANY, null, false);
    }

    public AssemblerInfo(IAType declaredType, EmptyAssembler parent) {
        this(declaredType, parent, false);
    }

    public AssemblerInfo(IAType declaredType, AbstractNestedValueAssembler parent, boolean delegate) {
        this(declaredType, parent, delegate, null, -1);
    }

    public AssemblerInfo(IAType declaredType, AbstractNestedValueAssembler parent, boolean delegate,
            IValueReference fieldName) {
        this(declaredType, parent, delegate, fieldName, -1);
    }

    public AssemblerInfo(IAType declaredType, AbstractNestedValueAssembler parent, boolean delegate, int fieldIndex) {
        this(declaredType, parent, delegate, null, fieldIndex);
    }

    public AssemblerInfo(IAType declaredType, AbstractNestedValueAssembler parent, boolean delegate,
            IValueReference fieldName, int fieldIndex) {
        this(declaredType, parent, delegate, fieldName, fieldIndex, false);
    }

    public AssemblerInfo(IAType declaredType, AbstractNestedValueAssembler parent, boolean delegate,
            IValueReference fieldName, int fieldIndex, boolean fieldNameTagged) {
        this.parent = parent;
        this.declaredType = getActualType(declaredType);
        this.delegate = delegate;
        this.fieldName = fieldNameTagged ? fieldName : createTaggedFieldName(fieldName);
        this.fieldIndex = fieldIndex;
    }

    private IValueReference createTaggedFieldName(IValueReference fieldName) {
        if (fieldName == null) {
            return null;
        }
        byte[] storage = new byte[1 + fieldName.getLength()];
        storage[0] = ATypeTag.STRING.serialize();
        System.arraycopy(fieldName.getByteArray(), fieldName.getStartOffset(), storage, 1, fieldName.getLength());
        VoidPointable taggedFieldName = new VoidPointable();
        taggedFieldName.set(storage, 0, storage.length);
        return taggedFieldName;
    }

    public AbstractNestedValueAssembler getParent() {
        return parent;
    }

    public IAType getDeclaredType() {
        return declaredType;
    }

    public boolean isDelegate() {
        return delegate;
    }

    public IValueReference getFieldName() {
        return fieldName;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }
}
