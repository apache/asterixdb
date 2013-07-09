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
import edu.uci.ics.asterix.om.base.ADate;
import edu.uci.ics.asterix.om.base.AMutableDate;
import edu.uci.ics.asterix.om.base.temporal.ADateParserFactory;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

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
