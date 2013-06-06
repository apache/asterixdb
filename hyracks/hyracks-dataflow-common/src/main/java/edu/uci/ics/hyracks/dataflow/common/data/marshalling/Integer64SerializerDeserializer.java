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

public class Integer64SerializerDeserializer implements ISerializerDeserializer<Long> {

    private static final long serialVersionUID = 1L;

    public static final Integer64SerializerDeserializer INSTANCE = new Integer64SerializerDeserializer();

    private Integer64SerializerDeserializer() {
    }

    @Override
    public Long deserialize(DataInput in) throws HyracksDataException {
        try {
            return in.readLong();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(Long instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeLong(instance.longValue());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    public static long getLong(byte[] bytes, int offset) {
        return (((long) (bytes[offset] & 0xff)) << 56) + (((long) (bytes[offset + 1] & 0xff)) << 48)
                + (((long) (bytes[offset + 2] & 0xff)) << 40) + (((long) (bytes[offset + 3] & 0xff)) << 32)
                + (((long) (bytes[offset + 4] & 0xff)) << 24) + (((long) (bytes[offset + 5] & 0xff)) << 16)
                + (((long) (bytes[offset + 6] & 0xff)) << 8) + (((long) (bytes[offset + 7] & 0xff)) << 0);
    }

}
