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
package org.apache.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.asterix.om.base.AFloat;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;

public class AFloatSerializerDeserializer implements ISerializerDeserializer<AFloat> {

    private static final long serialVersionUID = 1L;

    public static final AFloatSerializerDeserializer INSTANCE = new AFloatSerializerDeserializer();

    private AFloatSerializerDeserializer() {
    }

    @Override
    public AFloat deserialize(DataInput in) throws HyracksDataException {
        return new AFloat(FloatSerializerDeserializer.read(in));
    }

    @Override
    public void serialize(AFloat instance, DataOutput out) throws HyracksDataException {
        FloatSerializerDeserializer.write(instance.getFloatValue(), out);
    }

    public static float getFloat(byte[] bytes, int offset) {
        return FloatPointable.getFloat(bytes, offset);
    }
}
