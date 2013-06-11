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
import edu.uci.ics.asterix.om.base.ADuration;
import edu.uci.ics.asterix.om.base.AMutableDuration;
import edu.uci.ics.asterix.om.base.temporal.ADurationParserFactory;
import edu.uci.ics.asterix.om.base.temporal.ADurationParserFactory.ADurationParseOption;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ADurationSerializerDeserializer implements ISerializerDeserializer<ADuration> {

    private static final long serialVersionUID = 1L;

    public static final ADurationSerializerDeserializer INSTANCE = new ADurationSerializerDeserializer();
    @SuppressWarnings("unchecked")
    private static final ISerializerDeserializer<ADuration> durationSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADURATION);
    private static final AMutableDuration aDuration = new AMutableDuration(0, 0);

    private ADurationSerializerDeserializer() {
    }

    @Override
    public ADuration deserialize(DataInput in) throws HyracksDataException {
        try {
            return new ADuration(in.readInt(), in.readLong());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ADuration instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeInt(instance.getMonths());
            out.writeLong(instance.getMilliseconds());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String duration, DataOutput out) throws HyracksDataException {
        try {
            ADurationParserFactory.parseDuration(duration, 0, duration.length(), aDuration, ADurationParseOption.All);
            durationSerde.serialize(aDuration, out);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    /**
     * Get the year-month field of the duration as an integer number of days.
     * 
     * @param data
     * @param offset
     * @return
     */
    public static int getYearMonth(byte[] data, int offset) {
        return AInt32SerializerDeserializer.getInt(data, offset + getYearMonthOffset());
    }

    /**
     * Get the day-time field of the duration as an long integer number of milliseconds.
     * 
     * @param data
     * @param offset
     * @return
     */
    public static long getDayTime(byte[] data, int offset) {
        return AInt64SerializerDeserializer.getLong(data, offset + getDayTimeOffset());
    }

    public static int getYearMonthOffset() {
        return 0;
    }

    public static int getDayTimeOffset() {
        return 4;
    }
}
