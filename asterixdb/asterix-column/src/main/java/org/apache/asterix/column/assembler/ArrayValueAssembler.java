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

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class ArrayValueAssembler extends AbstractNestedValueAssembler {
    private final IAsterixListBuilder listBuilder;
    private final AbstractCollectionType collectionType;
    private final int firstValueIndex;
    private boolean missing;

    ArrayValueAssembler(int level, AssemblerInfo info, int firstValueIndex) {
        super(level, info);
        this.firstValueIndex = firstValueIndex;
        collectionType = (AbstractCollectionType) info.getDeclaredType();
        listBuilder = new ListBuilderFactory().create(collectionType.getTypeTag());
        missing = false;
    }

    final int getFirstValueIndex() {
        return firstValueIndex;
    }

    @Override
    void reset() {
        missing = false;
        listBuilder.reset(collectionType);
        storage.reset();
    }

    @Override
    void addValue(AbstractValueAssembler value) throws HyracksDataException {
        writePreviousMissing();
        listBuilder.addItem(value.getValue());
    }

    @Override
    void addNull(AbstractValueAssembler value) throws HyracksDataException {
        writePreviousMissing();
        listBuilder.addItem(NULL);
    }

    @Override
    void addMissing() throws HyracksDataException {
        writePreviousMissing();
        missing = true;
    }

    @Override
    void addValueToParent() throws HyracksDataException {
        storage.reset();
        listBuilder.write(storage.getDataOutput(), true);
        getParent().addValue(this);
    }

    @Override
    public IValueReference getValue() {
        return storage;
    }

    private void writePreviousMissing() throws HyracksDataException {
        if (missing) {
            listBuilder.addItem(MISSING);
            missing = false;
        }
    }
}
