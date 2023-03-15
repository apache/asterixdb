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

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class ObjectValueAssembler extends AbstractNestedValueAssembler {
    private final RecordBuilder recordBuilder;
    private final ARecordType recordType;

    ObjectValueAssembler(int level, AssemblerInfo info) {
        super(level, info);
        recordBuilder = new RecordBuilder();
        recordType = (ARecordType) info.getDeclaredType();
    }

    @Override
    void reset() {
        recordBuilder.reset(recordType);
        storage.reset();
    }

    @Override
    void addValue(AbstractValueAssembler value) throws HyracksDataException {
        int valueIndex = value.getFieldIndex();
        if (valueIndex >= 0) {
            recordBuilder.addField(valueIndex, value.getValue());
        } else {
            recordBuilder.addField(value.getFieldName(), value.getValue());
        }
    }

    @Override
    void addNull(AbstractValueAssembler value) throws HyracksDataException {
        int valueIndex = value.getFieldIndex();
        if (valueIndex >= 0) {
            recordBuilder.addField(valueIndex, NULL);
        } else {
            recordBuilder.addField(value.getFieldName(), NULL);
        }
    }

    @Override
    void addValueToParent() throws HyracksDataException {
        storage.reset();
        recordBuilder.write(storage.getDataOutput(), true);
        getParent().addValue(this);
    }

    @Override
    public IValueReference getValue() {
        return storage;
    }
}
