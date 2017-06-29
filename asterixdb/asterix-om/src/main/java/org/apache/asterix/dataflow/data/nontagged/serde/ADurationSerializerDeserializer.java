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

import org.apache.asterix.om.base.ADuration;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class ADurationSerializerDeserializer implements ISerializerDeserializer<ADuration> {

    private static final long serialVersionUID = 1L;

    public static final ADurationSerializerDeserializer INSTANCE = new ADurationSerializerDeserializer();

    private ADurationSerializerDeserializer() {
    }

    @Override
    public ADuration deserialize(DataInput in) throws HyracksDataException {
        final int months = IntegerSerializerDeserializer.read(in);
        final long seconds = Integer64SerializerDeserializer.read(in);
        return new ADuration(months, seconds);
    }

    @Override
    public void serialize(ADuration instance, DataOutput out) throws HyracksDataException {
        IntegerSerializerDeserializer.write(instance.getMonths(), out);
        Integer64SerializerDeserializer.write(instance.getMilliseconds(), out);
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
