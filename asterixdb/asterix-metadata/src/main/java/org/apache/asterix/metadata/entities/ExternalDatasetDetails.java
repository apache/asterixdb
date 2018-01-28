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

package org.apache.asterix.metadata.entities;

import java.io.DataOutput;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.TransactionState;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ExternalDatasetDetails implements IDatasetDetails {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    private final String adapter;
    private final Map<String, String> properties;
    private Date lastRefreshTime;
    private TransactionState state;

    public ExternalDatasetDetails(String adapter, Map<String, String> properties, Date lastRefreshTime,
            TransactionState state) {
        this.properties = properties;
        this.adapter = adapter;
        this.lastRefreshTime = lastRefreshTime;
        this.state = state;
    }

    public String getAdapter() {
        return adapter;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public DatasetType getDatasetType() {
        return DatasetType.EXTERNAL;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeDatasetDetailsRecordType(DataOutput out) throws HyracksDataException {
        IARecordBuilder externalRecordBuilder = new RecordBuilder();
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        externalRecordBuilder.reset(MetadataRecordTypes.EXTERNAL_DETAILS_RECORDTYPE);
        AMutableString aString = new AMutableString("");

        ISerializerDeserializer<AString> stringSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
        ISerializerDeserializer<ADateTime> dateTimeSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);
        ISerializerDeserializer<AInt32> intSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);

        // write field 0
        fieldValue.reset();
        aString.setValue(this.getAdapter());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        externalRecordBuilder.addField(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX,
                fieldValue);

        // write field 1
        listBuilder.reset((AOrderedListType) MetadataRecordTypes.EXTERNAL_DETAILS_RECORDTYPE.getFieldTypes()[1]);
        for (Map.Entry<String, String> property : this.properties.entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            itemValue.reset();
            DatasetUtil.writePropertyTypeRecord(name, value, itemValue.getDataOutput(),
                    MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE);
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        externalRecordBuilder.addField(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        dateTimeSerde.serialize(new ADateTime(lastRefreshTime.getTime()), fieldValue.getDataOutput());
        externalRecordBuilder.addField(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_LAST_REFRESH_TIME_FIELD_INDEX,
                fieldValue);

        // write field 3
        fieldValue.reset();
        intSerde.serialize(new AInt32(state.ordinal()), fieldValue.getDataOutput());
        externalRecordBuilder.addField(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_TRANSACTION_STATE_FIELD_INDEX,
                fieldValue);
        externalRecordBuilder.write(out, true);
    }

    public Date getTimestamp() {
        return lastRefreshTime;
    }

    public void setRefreshTimestamp(Date timestamp) {
        this.lastRefreshTime = timestamp;
    }

    public TransactionState getState() {
        return state;
    }

    public void setState(TransactionState state) {
        this.state = state;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(toMap());
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.WARN, "Unable to convert map to json String", e);
            return getClass().getSimpleName();
        }
    }

    private Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("adapter", adapter);
        map.put("properties", properties);
        map.put("lastRefreshTime", lastRefreshTime.toString());
        map.put("state", state.name());
        return map;
    }
}
