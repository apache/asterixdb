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
package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.builders.UnorderedListBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableOrderedList;
import edu.uci.ics.asterix.om.base.AMutablePoint;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AMutableUnorderedList;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IACursor;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class FeedClient implements IFeedClient {

    protected static final Logger LOGGER = Logger.getLogger(FeedClient.class.getName());

    protected ARecordSerializerDeserializer recordSerDe;
    protected AMutableRecord mutableRecord;
    protected boolean messageReceived;
    protected boolean continueIngestion = true;
    protected IARecordBuilder recordBuilder = new RecordBuilder();

    protected AMutableString aString = new AMutableString("");
    protected AMutableInt32 aInt32 = new AMutableInt32(0);
    protected AMutablePoint aPoint = new AMutablePoint(0, 0);
    protected AMutableDateTime aDateTime = new AMutableDateTime(0);

    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);

    public abstract InflowState retrieveNextRecord() throws Exception;

    @Override
    public InflowState nextTuple(DataOutput dataOutput, int timeout) throws AsterixException {
        try {
            InflowState state = null;
            int waitCount = 0;
            boolean continueWait = true;
            while ((state == null || state.equals(InflowState.DATA_NOT_AVAILABLE)) && continueWait) {
                state = retrieveNextRecord();
                switch (state) {
                    case DATA_AVAILABLE:
                        recordBuilder.reset(mutableRecord.getType());
                        recordBuilder.init();
                        writeRecord(mutableRecord, dataOutput, recordBuilder);
                        break;
                    case DATA_NOT_AVAILABLE:
                        if (waitCount > timeout) {
                            continueWait = false;
                        } else {
                            if (LOGGER.isLoggable(Level.WARNING)) {
                                LOGGER.warning("Waiting to obtain data from pull based adaptor");
                            }
                            Thread.sleep(1000);
                            waitCount++;
                        }
                        break;
                    case NO_MORE_DATA:
                        break;
                }
            }
            return state;
        } catch (Exception e) {
            throw new AsterixException(e);
        }

    }

    private void writeRecord(AMutableRecord record, DataOutput dataOutput, IARecordBuilder recordBuilder)
            throws IOException, AsterixException {
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        int numFields = record.getType().getFieldNames().length;
        for (int pos = 0; pos < numFields; pos++) {
            fieldValue.reset();
            IAObject obj = record.getValueByPos(pos);
            writeObject(obj, fieldValue.getDataOutput());
            recordBuilder.addField(pos, fieldValue);
        }
        recordBuilder.write(dataOutput, true);
    }

    private void writeObject(IAObject obj, DataOutput dataOutput) throws IOException, AsterixException {
        switch (obj.getType().getTypeTag()) {
            case RECORD: {
                IARecordBuilder recordBuilder = new RecordBuilder();
                recordBuilder.reset((ARecordType) obj.getType());
                recordBuilder.init();
                writeRecord((AMutableRecord) obj, dataOutput, recordBuilder);
                break;
            }

            case ORDEREDLIST: {
                OrderedListBuilder listBuilder = new OrderedListBuilder();
                listBuilder.reset((AOrderedListType) ((AMutableOrderedList) obj).getType());
                IACursor cursor = ((AMutableOrderedList) obj).getCursor();
                ArrayBackedValueStorage listItemValue = new ArrayBackedValueStorage();
                while (cursor.next()) {
                    listItemValue.reset();
                    IAObject item = cursor.get();
                    writeObject(item, listItemValue.getDataOutput());
                    listBuilder.addItem(listItemValue);
                }
                listBuilder.write(dataOutput, true);
                break;
            }

            case UNORDEREDLIST: {
                UnorderedListBuilder listBuilder = new UnorderedListBuilder();
                listBuilder.reset((AUnorderedListType) ((AMutableUnorderedList) obj).getType());
                IACursor cursor = ((AMutableUnorderedList) obj).getCursor();
                ArrayBackedValueStorage listItemValue = new ArrayBackedValueStorage();
                while (cursor.next()) {
                    listItemValue.reset();
                    IAObject item = cursor.get();
                    writeObject(item, listItemValue.getDataOutput());
                    listBuilder.addItem(listItemValue);
                }
                listBuilder.write(dataOutput, true);
                break;
            }

            default:
                AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(obj.getType()).serialize(obj,
                        dataOutput);
                break;
        }
    }
}
