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

import static org.apache.asterix.om.types.AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entitytupletranslators.AbstractTupleTranslator;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class ViewDetails implements IDatasetDetails {

    private static final long serialVersionUID = 1L;

    public static List<DependencyKind> DEPENDENCIES_SCHEMA =
            Arrays.asList(DependencyKind.DATASET, DependencyKind.FUNCTION, DependencyKind.TYPE, DependencyKind.SYNONYM);

    private final String viewBody;

    private final List<List<Triple<DataverseName, String, String>>> dependencies;

    // Typed view parameters

    private final Boolean defaultNull;

    private final String datetimeFormat;

    private final String dateFormat;

    private final String timeFormat;

    private final List<String> primaryKeyFields;

    public ViewDetails(String viewBody, List<List<Triple<DataverseName, String, String>>> dependencies,
            Boolean defaultNull, List<String> primaryKeyFields, String datetimeFormat, String dateFormat,
            String timeFormat) {
        this.viewBody = Objects.requireNonNull(viewBody);
        this.dependencies = Objects.requireNonNull(dependencies);
        this.defaultNull = defaultNull;
        this.datetimeFormat = datetimeFormat;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.primaryKeyFields = primaryKeyFields;
    }

    @Override
    public DatasetConfig.DatasetType getDatasetType() {
        return DatasetConfig.DatasetType.VIEW;
    }

    public String getViewBody() {
        return viewBody;
    }

    public List<List<Triple<DataverseName, String, String>>> getDependencies() {
        return dependencies;
    }

    // Typed view fields

    public Boolean getDefaultNull() {
        return defaultNull;
    }

    public List<String> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    public String getDatetimeFormat() {
        return datetimeFormat;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    @Override
    public void writeDatasetDetailsRecordType(DataOutput out) throws HyracksDataException {
        IARecordBuilder viewRecordBuilder = new RecordBuilder();
        viewRecordBuilder.reset(RecordUtil.FULLY_OPEN_RECORD_TYPE);

        ArrayBackedValueStorage fieldName = new ArrayBackedValueStorage();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();

        AMutableString aString = new AMutableString("");
        ISerializerDeserializer<AString> stringSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
        ISerializerDeserializer<ABoolean> booleanSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
        ISerializerDeserializer<ANull> nullSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);

        // write field 'Definition'
        fieldName.reset();
        aString.setValue(MetadataRecordTypes.FIELD_NAME_DEFINITION);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(viewBody);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        viewRecordBuilder.addField(fieldName, fieldValue);

        // write field 'Dependencies'
        if (!dependencies.isEmpty()) {
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_DEPENDENCIES);
            stringSerde.serialize(aString, fieldName.getDataOutput());

            OrderedListBuilder dependenciesListBuilder = new OrderedListBuilder();
            OrderedListBuilder dependencyListBuilder = new OrderedListBuilder();
            OrderedListBuilder dependencyNameListBuilder = new OrderedListBuilder();
            List<String> dependencySubnames = new ArrayList<>(3);

            dependenciesListBuilder.reset(FULL_OPEN_ORDEREDLIST_TYPE);
            for (List<Triple<DataverseName, String, String>> dependenciesList : dependencies) {
                dependencyListBuilder.reset(FULL_OPEN_ORDEREDLIST_TYPE);
                for (Triple<DataverseName, String, String> dependency : dependenciesList) {
                    dependencyNameListBuilder.reset(FULL_OPEN_ORDEREDLIST_TYPE);
                    dependencySubnames.clear();
                    AbstractTupleTranslator.getDependencySubNames(dependency, dependencySubnames);
                    for (String subName : dependencySubnames) {
                        itemValue.reset();
                        aString.setValue(subName);
                        stringSerde.serialize(aString, itemValue.getDataOutput());
                        dependencyNameListBuilder.addItem(itemValue);
                    }
                    itemValue.reset();
                    dependencyNameListBuilder.write(itemValue.getDataOutput(), true);
                    dependencyListBuilder.addItem(itemValue);
                }
                itemValue.reset();
                dependencyListBuilder.write(itemValue.getDataOutput(), true);
                dependenciesListBuilder.addItem(itemValue);
            }
            fieldValue.reset();
            dependenciesListBuilder.write(fieldValue.getDataOutput(), true);
            viewRecordBuilder.addField(fieldName, fieldValue);
        }

        // write field 'DefaultNull'
        if (defaultNull != null && defaultNull) {
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_DEFAULT);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            nullSerde.serialize(ANull.NULL, fieldValue.getDataOutput());
            viewRecordBuilder.addField(fieldName, fieldValue);
        }

        // write field 'PrimaryKey'
        if (primaryKeyFields != null && !primaryKeyFields.isEmpty()) {
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_PRIMARY_KEY);
            stringSerde.serialize(aString, fieldName.getDataOutput());

            // write as list of lists to be consistent with how InternalDatasetDetails writes its primary key
            OrderedListBuilder primaryKeyListBuilder = new OrderedListBuilder();
            OrderedListBuilder listBuilder = new OrderedListBuilder();

            primaryKeyListBuilder.reset(FULL_OPEN_ORDEREDLIST_TYPE);
            for (String field : primaryKeyFields) {
                listBuilder.reset(FULL_OPEN_ORDEREDLIST_TYPE);
                itemValue.reset();
                aString.setValue(field);
                stringSerde.serialize(aString, itemValue.getDataOutput());
                listBuilder.addItem(itemValue);
                itemValue.reset();
                listBuilder.write(itemValue.getDataOutput(), true);
                primaryKeyListBuilder.addItem(itemValue);
            }
            fieldValue.reset();
            primaryKeyListBuilder.write(fieldValue.getDataOutput(), true);
            viewRecordBuilder.addField(fieldName, fieldValue);

            // write field 'PrimaryKeyEnforced'
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_PRIMARY_KEY_ENFORCED);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            booleanSerde.serialize(ABoolean.FALSE, fieldValue.getDataOutput());
            viewRecordBuilder.addField(fieldName, fieldValue);
        }

        // write field 'Format'
        if (datetimeFormat != null || dateFormat != null || timeFormat != null) {
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_DATA_FORMAT);
            stringSerde.serialize(aString, fieldName.getDataOutput());

            OrderedListBuilder formatListBuilder = new OrderedListBuilder();
            formatListBuilder.reset(FULL_OPEN_ORDEREDLIST_TYPE);
            for (String format : new String[] { datetimeFormat, dateFormat, timeFormat }) {
                itemValue.reset();
                if (format == null) {
                    nullSerde.serialize(ANull.NULL, itemValue.getDataOutput());
                } else {
                    aString.setValue(format);
                    stringSerde.serialize(aString, itemValue.getDataOutput());
                }
                formatListBuilder.addItem(itemValue);
            }
            fieldValue.reset();
            formatListBuilder.write(fieldValue.getDataOutput(), true);
            viewRecordBuilder.addField(fieldName, fieldValue);
        }

        viewRecordBuilder.write(out, true);
    }

    public static List<List<Triple<DataverseName, String, String>>> createDependencies(
            List<Triple<DataverseName, String, String>> datasetDependencies,
            List<Triple<DataverseName, String, String>> functionDependencies,
            List<Triple<DataverseName, String, String>> typeDependencies,
            List<Triple<DataverseName, String, String>> synonymDependencies) {
        List<List<Triple<DataverseName, String, String>>> depList = new ArrayList<>(DEPENDENCIES_SCHEMA.size());
        depList.add(datasetDependencies);
        depList.add(functionDependencies);
        depList.add(typeDependencies);
        if (!synonymDependencies.isEmpty()) {
            depList.add(synonymDependencies);
        }
        return depList;
    }
}
