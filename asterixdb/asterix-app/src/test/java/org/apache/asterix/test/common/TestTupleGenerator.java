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
package org.apache.asterix.test.common;

import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.BooleanSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

public class TestTupleGenerator {
    private final int stringFieldSizes;
    private final FieldType[] types;
    private final boolean reuseObject;
    private final Random random = new Random();
    private ITupleReference tuple;
    private UTF8StringSerializerDeserializer stringSerde =
            new UTF8StringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());
    private GrowableArray[] fields;

    public enum FieldType {
        Integer64,
        Boolean,
        Double,
        String
    }

    public TestTupleGenerator(FieldType[] types, int stringFieldSizes, boolean resueObject) {
        this.types = types;
        this.stringFieldSizes = stringFieldSizes;
        this.reuseObject = resueObject;
        this.fields = new GrowableArray[types.length];
        for (int i = 0; i < types.length; i++) {
            fields[i] = new GrowableArray();
        }
        tuple = new TestTupleReference(fields);
    }

    public ITupleReference next() throws HyracksDataException {
        if (reuseObject) {
            for (int i = 0; i < types.length; i++) {
                fields[i].reset();
            }
        } else {
            this.fields = new GrowableArray[types.length];
            for (int i = 0; i < types.length; i++) {
                fields[i] = new GrowableArray();
            }
            tuple = new TestTupleReference(fields);
        }
        for (int i = 0; i < types.length; i++) {
            FieldType type = types[i];
            switch (type) {
                case Boolean:
                    Boolean aBoolean = random.nextBoolean();
                    BooleanSerializerDeserializer.INSTANCE.serialize(aBoolean, fields[i].getDataOutput());
                    break;
                case Double:
                    double aDouble = random.nextDouble();
                    DoubleSerializerDeserializer.INSTANCE.serialize(aDouble, fields[i].getDataOutput());
                    break;
                case Integer64:
                    long aLong = random.nextLong();
                    Integer64SerializerDeserializer.INSTANCE.serialize(aLong, fields[i].getDataOutput());
                    break;
                case String:
                    String aString = RandomStringUtils.randomAlphanumeric(stringFieldSizes);
                    stringSerde.serialize(aString, fields[i].getDataOutput());
                    break;
                default:
                    break;
            }
        }
        return tuple;
    }
}
