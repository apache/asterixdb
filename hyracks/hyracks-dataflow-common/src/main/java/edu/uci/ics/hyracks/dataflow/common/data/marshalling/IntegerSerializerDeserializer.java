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

public class IntegerSerializerDeserializer implements ISerializerDeserializer<Integer> {
    private static final long serialVersionUID = 1L;

    public static final IntegerSerializerDeserializer INSTANCE = new IntegerSerializerDeserializer();

    private IntegerSerializerDeserializer() {
    }

    @Override
    public Integer deserialize(DataInput in) throws HyracksDataException {
        try {
            return in.readInt();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(Integer instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeInt(instance.intValue());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static int getInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }
    
    public static void putInt(int val, byte[] bytes, int offset) {
    	bytes[offset] = (byte)((val >>> 24) & 0xFF);    	
    	bytes[offset + 1] = (byte)((val >>> 16) & 0xFF);
    	bytes[offset + 2] = (byte)((val >>>  8) & 0xFF);
    	bytes[offset + 3] = (byte)((val >>>  0) & 0xFF);
    }
}
