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

package org.apache.hyracks.storage.am.common.datagen;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class TupleGenerator {
    protected final ISerializerDeserializer[] fieldSerdes;
    protected final IFieldValueGenerator[] fieldGens;
    protected final ArrayTupleBuilder tb;
    protected final ArrayTupleReference tuple;
    protected final byte[] payload;
    protected final DataOutput tbDos;

    public TupleGenerator(IFieldValueGenerator[] fieldGens, ISerializerDeserializer[] fieldSerdes, int payloadSize) {
        this.fieldSerdes = fieldSerdes;
        this.fieldGens = fieldGens;
        tuple = new ArrayTupleReference();
        if (payloadSize > 0) {
            tb = new ArrayTupleBuilder(fieldSerdes.length + 1);
            payload = new byte[payloadSize];
        } else {
            tb = new ArrayTupleBuilder(fieldSerdes.length);
            payload = null;
        }
        tbDos = tb.getDataOutput();
    }

    public ITupleReference next() throws IOException {
        tb.reset();
        for (int i = 0; i < fieldSerdes.length; i++) {
            fieldSerdes[i].serialize(fieldGens[i].next(), tbDos);
            tb.addFieldEndOffset();
        }
        if (payload != null) {
            tbDos.write(payload);
            tb.addFieldEndOffset();
        }
        tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        return tuple;
    }

    public ITupleReference get() {
        return tuple;
    }

    public void reset() {
        for (IFieldValueGenerator fieldGen : fieldGens) {
            fieldGen.reset();
        }
    }

    public ISerializerDeserializer[] getFieldSerdes() {
        return fieldSerdes;
    }

    public IFieldValueGenerator[] getFieldGens() {
        return fieldGens;
    }
}
