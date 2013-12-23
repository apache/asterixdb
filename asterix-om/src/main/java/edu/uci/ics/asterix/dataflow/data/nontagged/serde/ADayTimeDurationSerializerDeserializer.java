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
import edu.uci.ics.asterix.om.base.ADayTimeDuration;
import edu.uci.ics.asterix.om.base.AMutableDayTimeDuration;
import edu.uci.ics.asterix.om.base.temporal.ADurationParserFactory;
import edu.uci.ics.asterix.om.base.temporal.ADurationParserFactory.ADurationParseOption;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ADayTimeDurationSerializerDeserializer implements ISerializerDeserializer<ADayTimeDuration> {

    private static final long serialVersionUID = 1L;

    public static final ADayTimeDurationSerializerDeserializer INSTANCE = new ADayTimeDurationSerializerDeserializer();

    @SuppressWarnings("unchecked")
    private static final ISerializerDeserializer<ADayTimeDuration> dayTimeDurationSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADAYTIMEDURATION);
    private static final AMutableDayTimeDuration aDayTimeDuration = new AMutableDayTimeDuration(0);

    @Override
    public ADayTimeDuration deserialize(DataInput in) throws HyracksDataException {
        try {
            return new ADayTimeDuration(in.readLong());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ADayTimeDuration instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeLong(instance.getMilliseconds());
        } catch (IOException e) {
            throw new HyracksDataException();
        }
    }

    public void parse(String durationString, DataOutput out) throws HyracksDataException {
        try {
            ADurationParserFactory.parseDuration(durationString, 0, durationString.length(), aDayTimeDuration,
                    ADurationParseOption.All);
            dayTimeDurationSerde.serialize(aDayTimeDuration, out);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public static long getDayTime(byte[] data, int offset) {
        return AInt64SerializerDeserializer.getLong(data, offset);
    }
}
