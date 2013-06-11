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

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableTime;
import edu.uci.ics.asterix.om.base.ATime;
import edu.uci.ics.asterix.om.base.temporal.ATimeParserFactory;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ATimeSerializerDeserializer implements ISerializerDeserializer<ATime> {

    private static final long serialVersionUID = 1L;

    public static final ATimeSerializerDeserializer INSTANCE = new ATimeSerializerDeserializer();

    @SuppressWarnings("unchecked")
    private static final ISerializerDeserializer<ATime> timeSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ATIME);
    private static final AMutableTime aTime = new AMutableTime(0);
    
    private ATimeSerializerDeserializer() {
    }

    @Override
    public ATime deserialize(DataInput in) throws HyracksDataException {
        try {
            return new ATime(in.readInt());

        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ATime instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeInt(instance.getChrononTime());

        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String time, DataOutput out) throws HyracksDataException {
        int chrononTimeInMs;

        try {
            chrononTimeInMs = ATimeParserFactory.parseTimePart(time, 0, time.length());
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        aTime.setValue(chrononTimeInMs);

        timeSerde.serialize(aTime, out);
    }

    public static int getChronon(byte[] byteArray, int offset) {
        return AInt32SerializerDeserializer.getInt(byteArray, offset);
    }

}