/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.common.data.marshalling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class DoubleSerializerDeserializer implements ISerializerDeserializer<Double> {
    private static final long serialVersionUID = 1L;

    public static final DoubleSerializerDeserializer INSTANCE = new DoubleSerializerDeserializer();

    private DoubleSerializerDeserializer() {
    }

    @Override
    public Double deserialize(DataInput in) throws HyracksDataException {
        try {
            return in.readDouble();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(Double instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeDouble(instance.doubleValue());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static double getDouble(byte[] bytes, int offset) {
        return Double.longBitsToDouble(getLongBits(bytes, offset));
    }

    public static int getIntBits(byte[] bytes, int offset) {
        return IntegerSerializerDeserializer.getInt(bytes, offset);
    }

    public static long getLongBits(byte[] bytes, int offset) {
        return Integer64SerializerDeserializer.getLong(bytes, offset);
    }
    
    
}
