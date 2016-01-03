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
package org.apache.asterix.external.api;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableOrderedList;
import org.apache.asterix.om.base.AMutableRecord;
import org.apache.asterix.om.base.AMutableUnorderedList;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public interface IDataParser {

    /**
     * @return The supported data sources
     */
    public DataSourceType getDataSourceType();

    /**
     * @param configuration
     *            a set of configurations that comes from two sources.
     *            1. The create adapter statement.
     *            2. The query compiler.
     * @param recordType
     *            The expected record type
     * @throws HyracksDataException
     * @throws IOException
     */
    public void configure(Map<String, String> configuration, ARecordType recordType)
            throws HyracksDataException, IOException;

    /*
     * The following two static methods are expensive. right now, they are used by RSSFeeds and Twitter feed
     * TODO: Get rid of them
     */
    public static void writeRecord(AMutableRecord record, DataOutput dataOutput, IARecordBuilder recordBuilder)
            throws IOException, AsterixException {
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        int numFields = record.getType().getFieldNames().length;
        for (int pos = 0; pos < numFields; pos++) {
            fieldValue.reset();
            IAObject obj = record.getValueByPos(pos);
            IDataParser.writeObject(obj, fieldValue.getDataOutput());
            recordBuilder.addField(pos, fieldValue);
        }
        recordBuilder.write(dataOutput, true);
    }

    @SuppressWarnings("unchecked")
    public static void writeObject(IAObject obj, DataOutput dataOutput) throws IOException, AsterixException {
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
