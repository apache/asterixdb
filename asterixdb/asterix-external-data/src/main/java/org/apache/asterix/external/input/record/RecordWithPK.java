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
package org.apache.asterix.external.input.record;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class RecordWithPK<T> implements IRawRecord<T> {

    protected final ArrayBackedValueStorage[] pkFieldValueBuffers;
    protected final int[] pkIndexes;
    protected final IAType[] keyTypes;
    protected IRawRecord<T> record;

    public RecordWithPK(final IRawRecord<T> record, final IAType[] pkTypes, final int[] pkIndexes) {
        this.record = record;
        this.keyTypes = pkTypes;
        this.pkIndexes = pkIndexes;
        if (keyTypes != null) {
            this.pkFieldValueBuffers = new ArrayBackedValueStorage[pkTypes.length];
        } else {
            this.pkFieldValueBuffers = null;
        }
    }

    public RecordWithPK(final IRawRecord<T> rawRecord, final ArrayBackedValueStorage[] pkFieldValueBuffers) {
        this.record = rawRecord;
        this.keyTypes = null;
        this.pkIndexes = null;
        this.pkFieldValueBuffers = pkFieldValueBuffers;
    }

    public ArrayBackedValueStorage[] getPKs() {
        return pkFieldValueBuffers;
    }

    @Override
    public byte[] getBytes() {
        return record.getBytes();
    }

    @Override
    public T get() {
        return record.get();
    }

    public IRawRecord<? extends T> getRecord() {
        return record;
    }

    @Override
    public void reset() {
        record.reset();
        for (final ArrayBackedValueStorage pkStorage : pkFieldValueBuffers) {
            pkStorage.reset();
        }
    }

    @Override
    public int size() {
        return record.size();
    }

    @Override
    public void set(final T t) {
        record.set(t);
    }

    public void appendPrimaryKeyToTuple(final ArrayTupleBuilder tb) throws HyracksDataException {
        for (final ArrayBackedValueStorage pkStorage : pkFieldValueBuffers) {
            tb.addField(pkStorage);
        }
    }

    public void set(final IRawRecord<? extends T> record) {
        this.record.set(record.get());
    }
}
