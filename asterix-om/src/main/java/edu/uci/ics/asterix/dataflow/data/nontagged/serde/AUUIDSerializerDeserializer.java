/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.AUUID;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;

public class AUUIDSerializerDeserializer implements ISerializerDeserializer<AUUID> {

    private static final long serialVersionUID = 1L;

    public static final AUUIDSerializerDeserializer INSTANCE = new AUUIDSerializerDeserializer();

    @Override
    public AUUID deserialize(DataInput in) throws HyracksDataException {
        long msb = Integer64SerializerDeserializer.INSTANCE.deserialize(in);
        long lsb = Integer64SerializerDeserializer.INSTANCE.deserialize(in);
        // for each deserialization, a new object is created. maybe we should change this.
        return new AUUID(msb, lsb);
    }

    @Override
    public void serialize(AUUID instance, DataOutput out) throws HyracksDataException {
        try {
            Integer64SerializerDeserializer.INSTANCE.serialize(instance.getMostSignificantBits(), out);
            Integer64SerializerDeserializer.INSTANCE.serialize(instance.getLeastSignificantBits(), out);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

}
