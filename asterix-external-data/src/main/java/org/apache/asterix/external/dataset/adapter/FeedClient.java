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
package org.apache.asterix.external.dataset.adapter;

import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableOrderedList;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutableRecord;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AMutableUnorderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

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
