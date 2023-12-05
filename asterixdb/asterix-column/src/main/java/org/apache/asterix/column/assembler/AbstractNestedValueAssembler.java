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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractNestedValueAssembler extends AbstractValueAssembler {
    protected final ArrayBackedValueStorage storage;

    AbstractNestedValueAssembler(int level, AssemblerInfo info) {
        super(level, info);
        storage = new ArrayBackedValueStorage();
    }

    /**
     * @return whether the nested assembler was started or not
     */
    final boolean isStarted() {
        return started;
    }

    /**
     * Add a nested value
     *
     * @param value contains the value and its information
     */
    abstract void addValue(AbstractValueAssembler value) throws HyracksDataException;

    /**
     * Add a nested {@link ATypeTag#NULL}
     *
     * @param value contains the value's information
     */
    abstract void addNull(AbstractValueAssembler value) throws HyracksDataException;

    /**
     * Add a nested {@link ATypeTag#MISSING}
     */
    void addMissing() throws HyracksDataException {
        //By default, we ignore missing
    }

    @Override
    final void addNullToAncestor(int nullLevel) throws HyracksDataException {
        AbstractNestedValueAssembler parent = getParent();
        if (nullLevel + 1 == level) {
            parent.start();
            parent.addNull(this);
            return;
        }
        parent.addNullToAncestor(nullLevel);
    }

    @Override
    final void addMissingToAncestor(int missingLevel) throws HyracksDataException {
        AbstractNestedValueAssembler parent = getParent();
        if (missingLevel + 1 == level) {
            parent.start();
            parent.addMissing();
            return;
        }
        parent.addMissingToAncestor(missingLevel);
    }

    /**
     * Recursively start the path of this assembler by staring all un-started parents
     */
    public final void start() {
        if (started) {
            return;
        }
        started = true;
        reset();
        AbstractNestedValueAssembler parent = getParent();
        if (parent != null && !parent.isStarted()) {
            parent.start();
        }
    }

    /**
     * End the assembler and add this nested value to its parent
     */
    public final void end() throws HyracksDataException {
        if (started) {
            addValueToParent();
            started = false;
        }

        if (isDelegate()) {
            getParent().end();
        }
    }
}
