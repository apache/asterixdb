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
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.temporal.ADateParserFactory;
import edu.uci.ics.asterix.om.base.temporal.ATimeParserFactory;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ADateTimeSerializerDeserializer implements ISerializerDeserializer<ADateTime> {

    private static final long serialVersionUID = 1L;

    public static final ADateTimeSerializerDeserializer INSTANCE = new ADateTimeSerializerDeserializer();
    @SuppressWarnings("unchecked")
    private static final ISerializerDeserializer<ADateTime> datetimeSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADATETIME);
    private static final AMutableDateTime aDateTime = new AMutableDateTime(0L);
    private static final String errorMessage = "This can not be an instance of datetime";

    private ADateTimeSerializerDeserializer() {
    }

    @Override
    public ADateTime deserialize(DataInput in) throws HyracksDataException {
        try {
            return new ADateTime(in.readLong());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ADateTime instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeLong(instance.getChrononTime());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String datetime, DataOutput out) throws HyracksDataException {

        long chrononTimeInMs = 0;
        try {

            // +1 if it is negative (-)
            short timeOffset = (short) ((datetime.charAt(0) == '-') ? 1 : 0);

            timeOffset += 8;
            
            if(datetime.charAt(timeOffset) != 'T'){
                timeOffset += 2;
                if(datetime.charAt(timeOffset) != 'T'){
                    throw new AlgebricksException(errorMessage + ": missing T");
                }
            }

            chrononTimeInMs = ADateParserFactory.parseDatePart(datetime, 0, timeOffset);

            chrononTimeInMs += ATimeParserFactory.parseTimePart(datetime, timeOffset + 1, datetime.length() - timeOffset - 1);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        aDateTime.setValue(chrononTimeInMs);

        datetimeSerde.serialize(aDateTime, out);
    }

    public static long getChronon(byte[] data, int offset) {
        return AInt64SerializerDeserializer.getLong(data, offset);
    }
}
