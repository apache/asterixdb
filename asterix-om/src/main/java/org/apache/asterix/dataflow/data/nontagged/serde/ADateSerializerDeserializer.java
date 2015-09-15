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
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.AMutableDate;
import org.apache.asterix.om.base.temporal.ADateParserFactory;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ADateSerializerDeserializer implements ISerializerDeserializer<ADate> {

    private static final long serialVersionUID = 1L;

    public static final ADateSerializerDeserializer INSTANCE = new ADateSerializerDeserializer();
    @SuppressWarnings("unchecked")
    private static final ISerializerDeserializer<ADate> dateSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADATE);

    private ADateSerializerDeserializer() {
    }

    @Override
    public ADate deserialize(DataInput in) throws HyracksDataException {
        try {
            return new ADate(in.readInt());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ADate instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeInt(instance.getChrononTimeInDays());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String date, DataOutput out) throws HyracksDataException {
        AMutableDate aDate = new AMutableDate(0);

        long chrononTimeInMs = 0;
        try {
            chrononTimeInMs = ADateParserFactory.parseDatePart(date, 0, date.length());
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        short temp = 0;
        if (chrononTimeInMs < 0 && chrononTimeInMs % GregorianCalendarSystem.CHRONON_OF_DAY != 0) {
            temp = 1;
        }
        aDate.setValue((int) (chrononTimeInMs / GregorianCalendarSystem.CHRONON_OF_DAY) - temp);

        dateSerde.serialize(aDate, out);
    }

    public static int getChronon(byte[] byteArray, int offset) {
        return AInt32SerializerDeserializer.getInt(byteArray, offset);
    }
}
