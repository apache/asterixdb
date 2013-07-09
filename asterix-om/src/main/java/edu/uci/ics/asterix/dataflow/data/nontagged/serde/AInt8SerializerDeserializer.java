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
package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.AInt8;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class AInt8SerializerDeserializer implements ISerializerDeserializer<AInt8> {

    private static final long serialVersionUID = 1L;

    public static final AInt8SerializerDeserializer INSTANCE = new AInt8SerializerDeserializer();

    @Override
    public AInt8 deserialize(DataInput in) throws HyracksDataException {
        try {
            return new AInt8(in.readByte());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    @Override
    public void serialize(AInt8 instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeByte(instance.getByteValue());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    public static byte getByte(byte[] bytes, int offset) {
        return bytes[offset];
    }

}
