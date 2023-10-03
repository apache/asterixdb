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

import static org.apache.asterix.metadata.entitytupletranslators.AbstractTupleTranslator.writeDateTimeFormats;
import static org.apache.asterix.metadata.entitytupletranslators.AbstractTupleTranslator.writeDeps;
import static org.apache.asterix.om.types.AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE;

import java.io.DataOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.common.metadata.DependencyFullyQualifiedName;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.bootstrap.DatasetEntity;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class ViewDetails implements IDatasetDetails {

    private static final long serialVersionUID = 2L;

    public static List<DependencyKind> DEPENDENCIES_SCHEMA =
            Arrays.asList(DependencyKind.DATASET, DependencyKind.FUNCTION, DependencyKind.TYPE, DependencyKind.SYNONYM);

    private final String viewBody;

    private final List<List<DependencyFullyQualifiedName>> dependencies;

    // Typed view parameters

    private final Boolean defaultNull;

    private final String datetimeFormat;

    private final String dateFormat;

    private final String timeFormat;

    private final List<String> primaryKeyFields;

    private final List<ForeignKey> foreignKeys;

    public ViewDetails(String viewBody, List<List<DependencyFullyQualifiedName>> dependencies, Boolean defaultNull,
            List<String> primaryKeyFields, List<ForeignKey> foreignKeys, String datetimeFormat, String dateFormat,
            String timeFormat) {
        this.viewBody = Objects.requireNonNull(viewBody);
        this.dependencies = Objects.requireNonNull(dependencies);
        this.defaultNull = defaultNull;
        this.primaryKeyFields = primaryKeyFields;
        this.foreignKeys = foreignKeys;
        this.datetimeFormat = datetimeFormat;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
    }

    @Override
    public DatasetConfig.DatasetType getDatasetType() {
        return DatasetConfig.DatasetType.VIEW;
    }

    public String getViewBody() {
        return viewBody;
    }

    public List<List<DependencyFullyQualifiedName>> getDependencies() {
        return dependencies;
    }

    // Typed view fields

    public Boolean getDefaultNull() {
        return defaultNull;
    }

    public List<String> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
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
    public void writeDatasetDetailsRecordType(DataOutput out, DatasetEntity datasetEntity) throws HyracksDataException {
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

            boolean writeDatabase = datasetEntity.databaseNameIndex() >= 0;
            dependenciesListBuilder.reset(FULL_OPEN_ORDEREDLIST_TYPE);
            for (List<DependencyFullyQualifiedName> dependenciesList : dependencies) {
                dependencyListBuilder.reset(FULL_OPEN_ORDEREDLIST_TYPE);
                writeDeps(dependencyListBuilder, itemValue, dependenciesList, dependencyNameListBuilder,
                        FULL_OPEN_ORDEREDLIST_TYPE, writeDatabase, aString, stringSerde);

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

            // write value as list of lists to be consistent with how InternalDatasetDetails writes its primary key
            fieldValue.reset();
            OrderedListBuilder keyListBuilder = new OrderedListBuilder();
            OrderedListBuilder fieldPathListBuilder = new OrderedListBuilder();
            writeKeyFieldsList(primaryKeyFields, keyListBuilder, fieldPathListBuilder, aString, stringSerde, itemValue);
            keyListBuilder.write(fieldValue.getDataOutput(), true);
            viewRecordBuilder.addField(fieldName, fieldValue);

            // write field 'PrimaryKeyEnforced'
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_PRIMARY_KEY_ENFORCED);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            booleanSerde.serialize(ABoolean.FALSE, fieldValue.getDataOutput());
            viewRecordBuilder.addField(fieldName, fieldValue);
        }

        // write field 'ForeignKeys'
        if (foreignKeys != null && !foreignKeys.isEmpty()) {
            OrderedListBuilder foreignKeysListBuilder = new OrderedListBuilder();
            foreignKeysListBuilder.reset(FULL_OPEN_ORDEREDLIST_TYPE);

            IARecordBuilder foreignKeyRecordBuilder = new RecordBuilder();
            OrderedListBuilder keyListBuilder = new OrderedListBuilder();
            OrderedListBuilder fieldPathListBuilder = new OrderedListBuilder();

            for (ViewDetails.ForeignKey foreignKey : foreignKeys) {
                foreignKeyRecordBuilder.reset(RecordUtil.FULLY_OPEN_RECORD_TYPE);

                // write field 'ForeignKey'
                fieldName.reset();
                aString.setValue(MetadataRecordTypes.FIELD_NAME_FOREIGN_KEY);
                stringSerde.serialize(aString, fieldName.getDataOutput());
                // write value as list of lists to be consistent with how InternalDatasetDetails writes its primary key
                fieldValue.reset();
                writeKeyFieldsList(foreignKey.getForeignKeyFields(), keyListBuilder, fieldPathListBuilder, aString,
                        stringSerde, itemValue);
                keyListBuilder.write(fieldValue.getDataOutput(), true);
                foreignKeyRecordBuilder.addField(fieldName, fieldValue);

                // write field 'RefDatabaseName'
                if (datasetEntity.databaseNameIndex() >= 0) {
                    fieldName.reset();
                    aString.setValue(MetadataRecordTypes.FIELD_NAME_REF_DATABASE_NAME);
                    stringSerde.serialize(aString, fieldName.getDataOutput());
                    fieldValue.reset();
                    aString.setValue(foreignKey.getReferencedDatasetName().getDatabaseName());
                    stringSerde.serialize(aString, fieldValue.getDataOutput());
                    foreignKeyRecordBuilder.addField(fieldName, fieldValue);
                }

                // write field 'RefDataverseName'
                fieldName.reset();
                aString.setValue(MetadataRecordTypes.FIELD_NAME_REF_DATAVERSE_NAME);
                stringSerde.serialize(aString, fieldName.getDataOutput());
                fieldValue.reset();
                aString.setValue(foreignKey.getReferencedDatasetName().getDataverseName().getCanonicalForm());
                stringSerde.serialize(aString, fieldValue.getDataOutput());
                foreignKeyRecordBuilder.addField(fieldName, fieldValue);

                // write field 'RefDatasetName'
                fieldName.reset();
                aString.setValue(MetadataRecordTypes.FIELD_NAME_REF_DATASET_NAME);
                stringSerde.serialize(aString, fieldName.getDataOutput());
                fieldValue.reset();
                aString.setValue(foreignKey.getReferencedDatasetName().getDatasetName());
                stringSerde.serialize(aString, fieldValue.getDataOutput());
                foreignKeyRecordBuilder.addField(fieldName, fieldValue);

                // write field 'IsEnforced'
                fieldName.reset();
                aString.setValue(MetadataRecordTypes.FIELD_NAME_IS_ENFORCED);
                stringSerde.serialize(aString, fieldName.getDataOutput());
                fieldValue.reset();
                booleanSerde.serialize(ABoolean.FALSE, fieldValue.getDataOutput());
                foreignKeyRecordBuilder.addField(fieldName, fieldValue);

                fieldValue.reset();
                foreignKeyRecordBuilder.write(fieldValue.getDataOutput(), true);
                foreignKeysListBuilder.addItem(fieldValue);
            }

            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_FOREIGN_KEYS);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            foreignKeysListBuilder.write(fieldValue.getDataOutput(), true);

            viewRecordBuilder.addField(fieldName, fieldValue);
        }

        // write field 'Format'
        writeDateTimeFormats(datetimeFormat, dateFormat, timeFormat, viewRecordBuilder, aString, nullSerde, stringSerde,
                fieldName, fieldValue, itemValue);
        viewRecordBuilder.write(out, true);
    }

    private void writeKeyFieldsList(List<String> keyFields, OrderedListBuilder keyListBuilder,
            OrderedListBuilder fieldListBuilder, AMutableString aString, ISerializerDeserializer<AString> stringSerde,
            ArrayBackedValueStorage itemValue) throws HyracksDataException {
        keyListBuilder.reset(FULL_OPEN_ORDEREDLIST_TYPE);
        for (String field : keyFields) {
            fieldListBuilder.reset(FULL_OPEN_ORDEREDLIST_TYPE);
            itemValue.reset();
            aString.setValue(field);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            fieldListBuilder.addItem(itemValue);
            itemValue.reset();
            fieldListBuilder.write(itemValue.getDataOutput(), true);
            keyListBuilder.addItem(itemValue);
        }
    }

    public static List<List<DependencyFullyQualifiedName>> createDependencies(
            List<DependencyFullyQualifiedName> datasetDependencies,
            List<DependencyFullyQualifiedName> functionDependencies,
            List<DependencyFullyQualifiedName> typeDependencies,
            List<DependencyFullyQualifiedName> synonymDependencies) {
        List<List<DependencyFullyQualifiedName>> depList = new ArrayList<>(DEPENDENCIES_SCHEMA.size());
        depList.add(datasetDependencies);
        depList.add(functionDependencies);
        depList.add(typeDependencies);
        if (!synonymDependencies.isEmpty()) {
            depList.add(synonymDependencies);
        }
        return depList;
    }

    public static final class ForeignKey implements Serializable {

        private static final long serialVersionUID = 1L;

        private final List<String> foreignKeyFields;

        private final DatasetFullyQualifiedName referencedDatasetName;

        public ForeignKey(List<String> foreignKeyFields, DatasetFullyQualifiedName referencedDatasetName) {
            this.foreignKeyFields = Objects.requireNonNull(foreignKeyFields);
            this.referencedDatasetName = Objects.requireNonNull(referencedDatasetName);
        }

        public List<String> getForeignKeyFields() {
            return foreignKeyFields;
        }

        public DatasetFullyQualifiedName getReferencedDatasetName() {
            return referencedDatasetName;
        }
    }
}
