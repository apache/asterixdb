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
package org.apache.asterix.external.input.filter;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.util.string.UTF8StringWriter;

public class ExternalFilterValueEvaluator implements IExternalFilterValueEvaluator {
    private final ATypeTag typeTag;
    private final ArrayBackedValueStorage value;
    private final AStringSerializerDeserializer stringSerDer;

    ExternalFilterValueEvaluator(ATypeTag typeTag) {
        this.typeTag = typeTag;
        value = new ArrayBackedValueStorage();
        stringSerDer = new AStringSerializerDeserializer(new UTF8StringWriter(), null);
    }

    @Override
    public void setValue(String stringValue) throws HyracksDataException {
        value.reset();
        try {
            writeValue(typeTag, stringValue, value, stringSerDer);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        result.set(value);
    }

    public static void writeValue(ATypeTag typeTag, String stringValue, ArrayBackedValueStorage value,
            AStringSerializerDeserializer stringSerDer) throws HyracksDataException {
        DataOutput output = value.getDataOutput();
        SerializerDeserializerUtil.serializeTag(typeTag, output);

        switch (typeTag) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                Integer64SerializerDeserializer.write(Long.parseLong(stringValue), output);
                break;
            case DOUBLE:
                DoubleSerializerDeserializer.write(Double.parseDouble(stringValue), output);
                break;
            case STRING:
                stringSerDer.serialize(stringValue, output);
                break;
        }
    }
}
