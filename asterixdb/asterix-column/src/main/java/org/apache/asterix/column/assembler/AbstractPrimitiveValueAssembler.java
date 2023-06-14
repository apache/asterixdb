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

import org.apache.asterix.column.assembler.value.IValueGetter;
import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public abstract class AbstractPrimitiveValueAssembler extends AbstractValueAssembler {
    /**
     * An indicator to go to the next value
     */
    public static final int NEXT_ASSEMBLER = -1;
    protected final IValueGetter primitiveValueGetter;
    protected final IColumnValuesReader reader;

    AbstractPrimitiveValueAssembler(int level, AssemblerInfo info, IColumnValuesReader reader,
            IValueGetter primitiveValueGetter) {
        super(level, info);
        this.primitiveValueGetter = primitiveValueGetter;
        this.reader = reader;
    }

    @Override
    public final IValueReference getValue() throws HyracksDataException {
        return primitiveValueGetter.getValue(reader);
    }

    @Override
    void addNullToAncestor(int nullLevel) throws HyracksDataException {
        AbstractNestedValueAssembler parent = getParent();
        if (nullLevel + 1 == level) {
            parent.start();
            parent.addNull(this);
            return;
        }
        parent.addNullToAncestor(nullLevel);
    }

    @Override
    void addMissingToAncestor(int missingLevel) throws HyracksDataException {
        AbstractNestedValueAssembler parent = getParent();
        if (missingLevel + 1 == level) {
            parent.start();
            parent.addMissing();
            return;
        }
        parent.addMissingToAncestor(missingLevel);
    }

    @Override
    final void addValueToParent() throws HyracksDataException {
        AbstractNestedValueAssembler parent = getParent();
        parent.start();
        getParent().addValue(this);
    }

    public final int getColumnIndex() {
        return reader.getColumnIndex();
    }

    public void skip(int count) throws HyracksDataException {
        reader.skip(count);
    }

    /**
     * Reset the assembler
     *
     * @param in             stream for value reader
     * @param numberOfTuples in the current mega leaf node
     */
    public abstract void reset(AbstractBytesInputStream in, int numberOfTuples) throws HyracksDataException;

    /**
     * Move to the next primitive value assembler
     *
     * @return the index of the next value
     */
    public abstract int next(AssemblerState state) throws HyracksDataException;
}
