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

package org.apache.asterix.metadata.entitytupletranslators;

import java.io.DataOutput;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.metadata.bootstrap.FeedEntity;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Feed metadata entity to an ITupleReference and vice versa.
 */
public class FeedTupleTranslator extends AbstractTupleTranslator<Feed> {

    private final FeedEntity feedEntity;

    protected FeedTupleTranslator(boolean getTuple, FeedEntity feedEntity) {
        super(getTuple, feedEntity.getIndex(), feedEntity.payloadPosition());
        this.feedEntity = feedEntity;
    }

    @Override
    protected Feed createMetadataEntityFromARecord(ARecord feedRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) feedRecord.getValueByPos(feedEntity.dataverseNameIndex())).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        int databaseNameIndex = feedEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) feedRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        String feedName = ((AString) feedRecord.getValueByPos(feedEntity.feedNameIndex())).getStringValue();

        AUnorderedList feedConfig = (AUnorderedList) feedRecord.getValueByPos(feedEntity.adapterConfigIndex());
        IACursor cursor = feedConfig.getCursor();
        // restore configurations
        Map<String, String> adaptorConfiguration = new HashMap<>();
        while (cursor.next()) {
            ARecord field = (ARecord) cursor.get();
            String key =
                    ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX)).getStringValue();
            String value =
                    ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX)).getStringValue();
            adaptorConfiguration.put(key, value);
        }

        return new Feed(databaseName, dataverseName, feedName, adaptorConfiguration);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Feed feed) throws HyracksDataException {
        String dataverseCanonicalName = feed.getDataverseName().getCanonicalForm();

        // write the key in the first two fields of the tuple
        tupleBuilder.reset();

        if (feedEntity.databaseNameIndex() >= 0) {
            aString.setValue(feed.getDatabaseName());
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(feed.getFeedName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(feedEntity.getRecordType());

        if (feedEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(feed.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(feedEntity.databaseNameIndex(), fieldValue);
        }
        // write dataverse name
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(feedEntity.dataverseNameIndex(), fieldValue);

        // write feed name
        fieldValue.reset();
        aString.setValue(feed.getFeedName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(feedEntity.feedNameIndex(), fieldValue);

        // write adaptor configuration
        fieldValue.reset();
        writeFeedAdaptorField(recordBuilder, feed, fieldValue);

        // write timestamp
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(feedEntity.timestampIndex(), fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private void writeFeedAdaptorField(IARecordBuilder recordBuilder, Feed feed,
            ArrayBackedValueStorage fieldValueBuffer) throws HyracksDataException {
        UnorderedListBuilder listBuilder = new UnorderedListBuilder();
        ArrayBackedValueStorage listEleBuffer = new ArrayBackedValueStorage();

        listBuilder.reset(
                (AUnorderedListType) feedEntity.getRecordType().getFieldTypes()[feedEntity.adapterConfigIndex()]);
        for (Map.Entry<String, String> property : feed.getConfiguration().entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            listEleBuffer.reset();
            writePropertyTypeRecord(name, value, listEleBuffer.getDataOutput());
            listBuilder.addItem(listEleBuffer);
        }
        listBuilder.write(fieldValueBuffer.getDataOutput(), true);
        recordBuilder.addField(feedEntity.adapterConfigIndex(), fieldValueBuffer);
    }

    private void writePropertyTypeRecord(String name, String value, DataOutput out) throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE);
        AMutableString aString = new AMutableString("");

        // write field 0
        fieldValue.reset();
        aString.setValue(name);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(0, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(1, fieldValue);

        propertyRecordBuilder.write(out, true);
    }
}
