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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;

public abstract class AbstractValueAssembler {
    protected static final VoidPointable NULL;
    protected static final VoidPointable MISSING;
    private final AbstractNestedValueAssembler parent;
    private final IValueReference fieldName;
    private final int fieldIndex;
    private final boolean delegate;
    protected final int level;
    protected boolean started;

    static {
        NULL = new VoidPointable();
        NULL.set(new byte[] { ATypeTag.NULL.serialize() }, 0, 1);

        MISSING = new VoidPointable();
        MISSING.set(new byte[] { ATypeTag.MISSING.serialize() }, 0, 1);
    }

    protected AbstractValueAssembler(int level, AssemblerInfo info) {
        this.parent = info.getParent();
        this.fieldName = info.getFieldName();
        this.fieldIndex = info.getFieldIndex();
        this.delegate = info.isDelegate();
        this.level = level;
    }

    /**
     * Add {@link ATypeTag#NULL} value to the ancestor at {@code nullLevel}
     *
     * @param nullLevel at what level the null occurred
     */
    abstract void addNullToAncestor(int nullLevel) throws HyracksDataException;

    /**
     * Add {@link ATypeTag#MISSING} value to the ancestor at {@code missingLevel}
     *
     * @param missingLevel at what level the missing occurred
     */
    abstract void addMissingToAncestor(int missingLevel) throws HyracksDataException;

    /**
     * Add the value of this assembler to its parent
     */
    abstract void addValueToParent() throws HyracksDataException;

    /**
     * @return the assembled value
     */
    public abstract IValueReference getValue() throws HyracksDataException;

    /**
     * Reset assembler
     */
    void reset() {
        //NoOp
    }

    /**
     * @return whether this assembler is the delegate (or representative) of its siblings
     */
    final boolean isDelegate() {
        return delegate;
    }

    /**
     * @return parent of the assembler
     */
    final AbstractNestedValueAssembler getParent() {
        return parent;
    }

    /**
     * Return the field name of the value of this assembler
     */
    final IValueReference getFieldName() {
        return fieldName;
    }

    /**
     * Return the field index of the value of this assembler (for closed types)
     */
    final int getFieldIndex() {
        return fieldIndex;
    }
}
