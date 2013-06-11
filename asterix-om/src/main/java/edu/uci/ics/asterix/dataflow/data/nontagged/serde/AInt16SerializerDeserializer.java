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

import edu.uci.ics.asterix.om.base.AInt16;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class AInt16SerializerDeserializer implements ISerializerDeserializer<AInt16> {

    private static final long serialVersionUID = 1L;

    public static final AInt16SerializerDeserializer INSTANCE = new AInt16SerializerDeserializer();

    @Override
    public AInt16 deserialize(DataInput in) throws HyracksDataException {
        try {
            return new AInt16(in.readShort());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    @Override
    public void serialize(AInt16 instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeShort(instance.getShortValue());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    public static short getShort(byte[] bytes, int offset) {
        return (short) (((bytes[offset] & 0xff) << 8) + ((bytes[offset + 1] & 0xff) << 0));
    }

    public static int getUnsignedShort(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 8) + ((bytes[offset + 1] & 0xff) << 0);
    }

}
