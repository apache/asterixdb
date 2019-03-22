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
package org.apache.asterix.app.data.gen;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.app.data.gen.RecordTupleGenerator.GenerationFunction;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.test.common.TestTupleReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class ARecordValueGenerator implements IAsterixFieldValueGenerator<ITupleReference> {
    private final IAsterixFieldValueGenerator<?>[] generators;
    private final boolean tagged;
    private final ARecordType recordType;
    private final RecordBuilder recBuilder;
    private final ArrayBackedValueStorage fieldValueBuffer;
    private final TestTupleReference tuple;

    public ARecordValueGenerator(GenerationFunction[] generationFunctions, ARecordType recordType, boolean[] uniques,
            boolean tagged) {
        this.tagged = tagged;
        this.recordType = recordType;
        tuple = new TestTupleReference(1);
        fieldValueBuffer = new ArrayBackedValueStorage();
        recBuilder = new RecordBuilder();
        recBuilder.reset(recordType);
        recBuilder.init();
        generators = new IAsterixFieldValueGenerator<?>[recordType.getFieldTypes().length];
        for (int i = 0; i < recordType.getFieldTypes().length; i++) {
            ATypeTag tag = recordType.getFieldTypes()[i].getTypeTag();
            switch (tag) {
                case BOOLEAN:
                    generators[i] = new ABooleanFieldValueGenerator(generationFunctions[i], true);
                    break;
                case DOUBLE:
                    generators[i] = new ADoubleFieldValueGenerator(generationFunctions[i], uniques[i], true);
                    break;
                case INTEGER:
                    generators[i] = new AInt32FieldValueGenerator(generationFunctions[i], uniques[i], true);
                    break;
                case BIGINT:
                    generators[i] = new AInt64FieldValueGenerator(generationFunctions[i], uniques[i], true);
                    break;
                case STRING:
                    generators[i] = new AStringFieldValueGenerator(generationFunctions[i], uniques[i], true);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type " + tag);
            }
        }
    }

    @Override
    public void next(DataOutput out) throws IOException {
        recBuilder.reset(recordType);
        recBuilder.init();
        for (int i = 0; i < generators.length; i++) {
            fieldValueBuffer.reset();
            generators[i].next(fieldValueBuffer.getDataOutput());
            recBuilder.addField(i, fieldValueBuffer);
        }
        recBuilder.write(out, tagged);
    }

    @Override
    public ITupleReference next() throws IOException {
        tuple.reset();
        next(tuple.getFields()[0].getDataOutput());
        return tuple;
    }

    @Override
    public void get(DataOutput out) throws IOException {
        recBuilder.reset(recordType);
        recBuilder.init();
        for (int i = 0; i < generators.length; i++) {
            fieldValueBuffer.reset();
            generators[i].get(fieldValueBuffer.getDataOutput());
            recBuilder.addField(i, fieldValueBuffer);
        }
        recBuilder.write(out, tagged);
    }

    @Override
    public ITupleReference get() throws IOException {
        tuple.reset();
        get(tuple.getFields()[0].getDataOutput());
        return tuple;
    }

    public void get(int i, DataOutput out) throws IOException {
        generators[i].get(out);
    }

    public Object get(int i) throws IOException {
        return generators[i].get();
    }
}
