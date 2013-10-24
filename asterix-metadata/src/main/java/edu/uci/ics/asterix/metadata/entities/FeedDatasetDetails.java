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

package edu.uci.ics.asterix.metadata.entities;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * Provides functionality for writing parameters for a FEED dataset into the
 * Metadata. Since FEED dataset is a special kind of INTERNAL dataset, this
 * class extends InternalDatasetDetails.
 */
public class FeedDatasetDetails extends InternalDatasetDetails {

    private static final long serialVersionUID = 1L;
    private final String adapterFactory;
    private final Map<String, String> properties;
    private final FunctionSignature signature;
    private FeedState feedState;

    public enum FeedState {
        INACTIVE,
        // INACTIVE state signifies that the feed dataset is not
        // connected with the external world through the feed
        // adapter.
        ACTIVE
        // ACTIVE state signifies that the feed dataset is connected to the
        // external world using an adapter that may put data into the dataset.
    }

    public FeedDatasetDetails(FileStructure fileStructure, PartitioningStrategy partitioningStrategy,
            List<String> partitioningKey, List<String> primaryKey, String groupName, String adapterFactory,
            Map<String, String> properties, FunctionSignature signature, String feedState, String compactionPolicy,
            Map<String, String> compactionPolicyProperties) {
        super(fileStructure, partitioningStrategy, partitioningKey, primaryKey, groupName, compactionPolicy,
                compactionPolicyProperties);
        this.properties = properties;
        this.adapterFactory = adapterFactory;
        this.signature = signature;
        this.feedState = feedState.equals(FeedState.ACTIVE.toString()) ? FeedState.ACTIVE : FeedState.INACTIVE;
    }

    @Override
    public DatasetType getDatasetType() {
        return DatasetType.FEED;
    }

    @Override
    public void writeDatasetDetailsRecordType(DataOutput out) throws HyracksDataException {
        IARecordBuilder feedRecordBuilder = new RecordBuilder();
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        feedRecordBuilder.reset(MetadataRecordTypes.FEED_DETAILS_RECORDTYPE);
        AMutableString aString = new AMutableString("");
        ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ASTRING);

        // write field 0
        fieldValue.reset();
        aString.setValue(this.getFileStructure().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        feedRecordBuilder.addField(MetadataRecordTypes.FEED_DETAILS_ARECORD_FILESTRUCTURE_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(this.getPartitioningStrategy().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        feedRecordBuilder.addField(MetadataRecordTypes.FEED_DETAILS_ARECORD_PARTITIONSTRATEGY_FIELD_INDEX, fieldValue);

        // write field 2
        listBuilder.reset((AOrderedListType) MetadataRecordTypes.FEED_DETAILS_RECORDTYPE.getFieldTypes()[2]);
        for (String field : partitioningKeys) {
            itemValue.reset();
            aString.setValue(field);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        feedRecordBuilder.addField(MetadataRecordTypes.FEED_DETAILS_ARECORD_PARTITIONKEY_FIELD_INDEX, fieldValue);

        // write field 3
        listBuilder.reset((AOrderedListType) MetadataRecordTypes.FEED_DETAILS_RECORDTYPE.getFieldTypes()[3]);
        for (String field : primaryKeys) {
            itemValue.reset();
            aString.setValue(field);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        feedRecordBuilder.addField(MetadataRecordTypes.FEED_DETAILS_ARECORD_PRIMARYKEY_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(getNodeGroupName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        feedRecordBuilder.addField(MetadataRecordTypes.FEED_DETAILS_ARECORD_GROUPNAME_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        aString.setValue(getAdapterFactory());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        feedRecordBuilder.addField(MetadataRecordTypes.FEED_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX, fieldValue);

        // write field 6
        listBuilder.reset((AOrderedListType) MetadataRecordTypes.FEED_DETAILS_RECORDTYPE.getFieldTypes()[6]);
        for (Map.Entry<String, String> property : properties.entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            itemValue.reset();
            writePropertyTypeRecord(name, value, itemValue.getDataOutput(),
                    MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE);
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        feedRecordBuilder.addField(MetadataRecordTypes.FEED_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX, fieldValue);

        // write field 7
        fieldValue.reset();
        if (signature != null) {
            aString.setValue(signature.toString());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            feedRecordBuilder.addField(MetadataRecordTypes.FEED_DETAILS_ARECORD_FUNCTION_FIELD_INDEX, fieldValue);
        }

        // write field 8
        fieldValue.reset();
        aString.setValue(getFeedState().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        feedRecordBuilder.addField(MetadataRecordTypes.FEED_DETAILS_ARECORD_STATE_FIELD_INDEX, fieldValue);

        // write field 9
        fieldValue.reset();
        aString.setValue(getCompactionPolicy().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        feedRecordBuilder.addField(MetadataRecordTypes.FEED_DETAILS_ARECORD_COMPACTION_POLICY_FIELD_INDEX, fieldValue);

        // write field 10
        listBuilder.reset((AOrderedListType) MetadataRecordTypes.FEED_DETAILS_RECORDTYPE.getFieldTypes()[10]);
        for (Map.Entry<String, String> property : compactionPolicyProperties.entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            itemValue.reset();
            writePropertyTypeRecord(name, value, itemValue.getDataOutput(),
                    MetadataRecordTypes.COMPACTION_POLICY_PROPERTIES_RECORDTYPE);
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        feedRecordBuilder.addField(MetadataRecordTypes.FEED_DETAILS_ARECORD_COMPACTION_POLICY_PROPERTIES_FIELD_INDEX,
                fieldValue);

        try {
            feedRecordBuilder.write(out, true);
        } catch (IOException | AsterixException e) {
            throw new HyracksDataException(e);
        }

    }

    public FeedState getFeedState() {
        return feedState;
    }

    public void setFeedState(FeedState feedState) {
        this.feedState = feedState;
    }

    public String getAdapterFactory() {
        return adapterFactory;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public FunctionSignature getFunction() {
        return signature;
    }

}