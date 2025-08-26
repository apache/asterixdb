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
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.config.CatalogConfig.CatalogType;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.ICatalogDetails;
import org.apache.asterix.metadata.bootstrap.CatalogEntity;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.utils.DatasetUtil;
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

public class IcebergCatalogDetails implements ICatalogDetails, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    private final String adapter;
    private final Map<String, String> properties;

    public IcebergCatalogDetails(String adapter, Map<String, String> properties) {
        this.properties = properties;
        this.adapter = adapter;
    }

    public String getAdapter() {
        return adapter;
    }

    public Map<String, String> getProperties() {
        properties.computeIfAbsent(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE, k -> getAdapter());
        return properties;
    }

    @Override
    public CatalogType getCatalogType() {
        return CatalogType.ICEBERG;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeCatalogDetailsRecordType(DataOutput out, CatalogEntity catalogEntity) throws HyracksDataException {
        IARecordBuilder recordBuilder = new RecordBuilder();
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        recordBuilder.reset(MetadataRecordTypes.CATALOG_DETAILS_RECORDTYPE);
        AMutableString aString = new AMutableString("");

        ISerializerDeserializer<AString> stringSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);

        // write field 0
        fieldValue.reset();
        aString.setValue(getAdapter());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.CATALOG_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX, fieldValue);

        // write field 1
        listBuilder.reset((AOrderedListType) MetadataRecordTypes.CATALOG_DETAILS_RECORDTYPE.getFieldTypes()[1]);
        ArrayBackedValueStorage propertyValue = new ArrayBackedValueStorage();
        for (Map.Entry<String, String> property : this.properties.entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            propertyValue.reset();
            DatasetUtil.writePropertyTypeRecord(name, value, propertyValue.getDataOutput(),
                    MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE);
            listBuilder.addItem(propertyValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.CATALOG_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX, fieldValue);
        recordBuilder.write(out, true);
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
        return map;
    }
}
