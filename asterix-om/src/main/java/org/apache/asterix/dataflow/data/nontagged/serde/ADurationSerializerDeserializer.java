/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ADuration;
import org.apache.asterix.om.base.AMutableDuration;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
import org.apache.asterix.om.base.temporal.ADurationParserFactory.ADurationParseOption;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

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
