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
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class ViewDetails implements IDatasetDetails {

    private static final long serialVersionUID = 1L;

    private final String viewBody;

    private final List<List<Triple<DataverseName, String, String>>> dependencies;

    public ViewDetails(String viewBody, List<List<Triple<DataverseName, String, String>>> dependencies) {
        this.viewBody = Objects.requireNonNull(viewBody);
        this.dependencies = Objects.requireNonNull(dependencies);
    }

    public String getViewBody() {
        return viewBody;
    }

    public List<List<Triple<DataverseName, String, String>>> getDependencies() {
        return dependencies;
    }

    @Override
    public DatasetConfig.DatasetType getDatasetType() {
        return DatasetConfig.DatasetType.VIEW;
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

        viewRecordBuilder.write(out, true);
    }

    public static List<DependencyKind> DEPENDENCIES_SCHEMA =
            Arrays.asList(DependencyKind.DATASET, DependencyKind.FUNCTION, DependencyKind.TYPE, DependencyKind.SYNONYM);

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
