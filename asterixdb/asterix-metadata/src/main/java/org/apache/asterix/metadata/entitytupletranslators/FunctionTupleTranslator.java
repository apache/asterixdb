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

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_ARGTYPES_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DETERMINISTIC_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_LIBRARY_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_NULLABILITY_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_NULLCALL_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_WITHPARAM_LIST_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.PROPERTIES_NAME_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.PROPERTIES_VALUE_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.TYPE_DV_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.TYPE_NAME_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.TYPE_UNKNOWNABLE_FIELD_NAME;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Function metadata entity to an ITupleReference and vice versa.
 */
public class FunctionTupleTranslator extends AbstractTupleTranslator<Function> {

    // Payload field containing serialized Function.
    private static final int FUNCTION_PAYLOAD_TUPLE_FIELD_INDEX = 3;

    protected final TxnId txnId;
    protected final MetadataNode metadataNode;

    protected OrderedListBuilder dependenciesListBuilder;
    protected OrderedListBuilder dependencyListBuilder;
    protected OrderedListBuilder dependencyNameListBuilder;
    protected List<String> dependencySubnames;
    protected AOrderedListType stringList;
    protected AOrderedListType listOfLists;

    protected FunctionTupleTranslator(TxnId txnId, MetadataNode metadataNode, boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FUNCTION_DATASET, FUNCTION_PAYLOAD_TUPLE_FIELD_INDEX);
        this.txnId = txnId;
        this.metadataNode = metadataNode;
        if (getTuple) {
            dependenciesListBuilder = new OrderedListBuilder();
            dependencyListBuilder = new OrderedListBuilder();
            dependencyNameListBuilder = new OrderedListBuilder();
            dependencySubnames = new ArrayList<>(3);
            stringList = new AOrderedListType(BuiltinType.ASTRING, null);
            listOfLists = new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null);
        }
    }

    private String getFunctionLibrary(ARecord functionRecord) {
        final ARecordType functionType = functionRecord.getType();
        final int functionLibraryIdx = functionType.getFieldIndex(FUNCTION_ARECORD_FUNCTION_LIBRARY_FIELD_NAME);
        return functionLibraryIdx >= 0 ? ((AString) functionRecord.getValueByPos(functionLibraryIdx)).getStringValue()
                : null;
    }

    private boolean getUnknownable(ARecord functionRecord) {
        final ARecordType functionType = functionRecord.getType();
        final int functionLibraryIdx = functionType.getFieldIndex(FUNCTION_ARECORD_FUNCTION_NULLABILITY_FIELD_NAME);
        return functionLibraryIdx >= 0
                ? Boolean.getBoolean(((AString) functionRecord.getValueByPos(functionLibraryIdx)).getStringValue())
                : false;
    }

    private boolean getNullCall(ARecord functionRecord) {
        final ARecordType functionType = functionRecord.getType();
        final int functionLibraryIdx = functionType.getFieldIndex(FUNCTION_ARECORD_FUNCTION_NULLCALL_FIELD_NAME);
        return functionLibraryIdx >= 0
                ? Boolean.getBoolean(((AString) functionRecord.getValueByPos(functionLibraryIdx)).getStringValue())
                : false;
    }

    private boolean getDeterministic(ARecord functionRecord) {
        final ARecordType functionType = functionRecord.getType();
        final int functionLibraryIdx = functionType.getFieldIndex(FUNCTION_ARECORD_FUNCTION_DETERMINISTIC_FIELD_NAME);
        return functionLibraryIdx >= 0
                ? Boolean.getBoolean(((AString) functionRecord.getValueByPos(functionLibraryIdx)).getStringValue())
                : false;
    }

    private Map<String, String> getFunctionWithParams(ARecord functionRecord) {
        String key = "";
        String value = "";
        Map<String, String> adaptorConfiguration = new HashMap<>();

        final ARecordType functionType = functionRecord.getType();
        final int functionLibraryIdx = functionType.getFieldIndex(FUNCTION_ARECORD_FUNCTION_WITHPARAM_LIST_NAME);
        if (functionLibraryIdx >= 0) {
            IACursor cursor = ((AOrderedList) functionRecord.getValueByPos(functionLibraryIdx)).getCursor();
            while (cursor.next()) {
                ARecord field = (ARecord) cursor.get();
                final ARecordType fieldType = field.getType();
                final int keyIdx = fieldType.getFieldIndex(PROPERTIES_NAME_FIELD_NAME);
                if (keyIdx >= 0) {
                    key = ((AString) field.getValueByPos(keyIdx)).getStringValue();
                }
                final int valueIdx = fieldType.getFieldIndex(PROPERTIES_VALUE_FIELD_NAME);
                if (valueIdx >= 0) {
                    value = ((AString) field.getValueByPos(valueIdx)).getStringValue();
                }
                adaptorConfiguration.put(key, value);
            }
        }
        return adaptorConfiguration;
    }

    private Pair<DataverseName, IAType> resolveTypePair(String dvCanonicalForm, String typeName, boolean unknownable)
            throws AlgebricksException {
        DataverseName dvName = DataverseName.createFromCanonicalForm(dvCanonicalForm);
        Pair<DataverseName, IAType> typePair = new Pair<>(null, null);
        typePair.first = dvName;
        if (BuiltinType.ANY.getTypeName().equalsIgnoreCase(typeName)) {
            typePair.second = BuiltinType.ANY;
        } else {
            typePair.second = BuiltinTypeMap.getTypeFromTypeName(metadataNode, txnId, dvName, typeName, unknownable);
        }
        return typePair;
    }

    private List<Pair<DataverseName, IAType>> getFunctionTypeArgs(ARecord functionRecord) throws AlgebricksException {
        String dv = "";
        String type = "";
        boolean unknownable = false;
        List<Pair<DataverseName, IAType>> functionArgs = new ArrayList<>();

        final ARecordType functionType = functionRecord.getType();
        final int functionLibraryIdx = functionType.getFieldIndex(FUNCTION_ARECORD_FUNCTION_ARGTYPES_FIELD_NAME);
        if (functionLibraryIdx >= 0) {
            IACursor cursor = ((AOrderedList) functionRecord.getValueByPos(functionLibraryIdx)).getCursor();
            while (cursor.next()) {
                ARecord field = (ARecord) cursor.get();
                final ARecordType fieldType = field.getType();
                final int keyIdx = fieldType.getFieldIndex(TYPE_DV_FIELD_NAME);
                if (keyIdx >= 0) {
                    dv = ((AString) field.getValueByPos(keyIdx)).getStringValue();
                }
                final int valueIdx = fieldType.getFieldIndex(TYPE_NAME_FIELD_NAME);
                if (valueIdx >= 0) {
                    type = ((AString) field.getValueByPos(valueIdx)).getStringValue();
                }
                final int unknownableIdx = fieldType.getFieldIndex(TYPE_UNKNOWNABLE_FIELD_NAME);
                if (unknownableIdx >= 0) {
                    unknownable = Boolean.valueOf(((AString) field.getValueByPos(valueIdx)).getStringValue());
                }
                functionArgs.add(resolveTypePair(dv, type, unknownable));
            }
        }
        return functionArgs;
    }

    private Pair<DataverseName, IAType> getFunctionReturnType(ARecord functionRecord) throws AlgebricksException {
        String returnType = ((AString) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_RETURN_TYPE_FIELD_INDEX)).getStringValue();
        DataverseName complete = DataverseName.createFromCanonicalForm(returnType);
        DataverseName dvName = DataverseName.create(complete.getParts(), 0, complete.getParts().size() - 1);
        String typeName = complete.getParts().get(complete.getParts().size() - 1);
        return resolveTypePair(dvName.getCanonicalForm(), typeName, getUnknownable(functionRecord));
    }

    protected Function createMetadataEntityFromARecord(ARecord functionRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) functionRecord.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_DATAVERSENAME_FIELD_INDEX))
                        .getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String functionName =
                ((AString) functionRecord.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTIONNAME_FIELD_INDEX))
                        .getStringValue();
        String arity = ((AString) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_ARITY_FIELD_INDEX)).getStringValue();

        IACursor argCursor = ((AOrderedList) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX)).getCursor();
        List<String> argNames = new ArrayList<>();
        while (argCursor.next()) {
            argNames.add(((AString) argCursor.get()).getStringValue());
        }

        String definition = ((AString) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DEFINITION_FIELD_INDEX)).getStringValue();

        String language = ((AString) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_LANGUAGE_FIELD_INDEX)).getStringValue();

        String functionKind =
                ((AString) functionRecord.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_KIND_FIELD_INDEX))
                        .getStringValue();

        IACursor dependenciesCursor = ((AOrderedList) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DEPENDENCIES_FIELD_INDEX)).getCursor();
        List<List<Triple<DataverseName, String, String>>> dependencies = new ArrayList<>();
        while (dependenciesCursor.next()) {
            List<Triple<DataverseName, String, String>> dependencyList = new ArrayList<>();
            IACursor qualifiedDependencyCursor = ((AOrderedList) dependenciesCursor.get()).getCursor();
            while (qualifiedDependencyCursor.next()) {
                Triple<DataverseName, String, String> dependency =
                        getDependency((AOrderedList) qualifiedDependencyCursor.get());
                dependencyList.add(dependency);
            }
            dependencies.add(dependencyList);
        }

        List<Pair<DataverseName, IAType>> args = getFunctionTypeArgs(functionRecord);
        Pair<DataverseName, IAType> returnType = getFunctionReturnType(functionRecord);
        String functionLibrary = getFunctionLibrary(functionRecord);
        Map<String, String> params = getFunctionWithParams(functionRecord);
        boolean unknownable = getUnknownable(functionRecord);
        boolean nullCall = getNullCall(functionRecord);
        boolean deterministic = getDeterministic(functionRecord);

        FunctionSignature signature = new FunctionSignature(dataverseName, functionName, Integer.parseInt(arity));
        return new Function(signature, args, argNames, returnType, definition, language, unknownable, nullCall,
                deterministic, functionLibrary, functionKind, dependencies, params);
    }

    private Triple<DataverseName, String, String> getDependency(AOrderedList dependencySubnames) {
        String dataverseCanonicalName = ((AString) dependencySubnames.getItem(0)).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String second = null, third = null;
        int ln = dependencySubnames.size();
        if (ln > 1) {
            second = ((AString) dependencySubnames.getItem(1)).getStringValue();
            if (ln > 2) {
                third = ((AString) dependencySubnames.getItem(2)).getStringValue();
            }
        }
        return new Triple<>(dataverseName, second, third);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Function function) throws HyracksDataException {
        String dataverseCanonicalName = function.getDataverseName().getCanonicalForm();

        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(function.getName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(String.valueOf(function.getArity()));
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the fourth field of the tuple

        recordBuilder.reset(MetadataRecordTypes.FUNCTION_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(function.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTIONNAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(String.valueOf(function.getArity()));
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_ARITY_FIELD_INDEX, fieldValue);

        // write field 3
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset((AOrderedListType) MetadataRecordTypes.FUNCTION_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX]);
        for (String p : function.getArgNames()) {
            itemValue.reset();
            aString.setValue(p == null ? BuiltinType.ANY.toString() : p);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(
                function.getReturnType().getFirst().getCanonicalForm() + '.' + function.getReturnType().getSecond());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_RETURN_TYPE_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        aString.setValue(function.getFunctionBody());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DEFINITION_FIELD_INDEX, fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(function.getLanguage());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_LANGUAGE_FIELD_INDEX, fieldValue);

        // write field 7
        fieldValue.reset();
        aString.setValue(function.getKind());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_KIND_FIELD_INDEX, fieldValue);

        // write field 8
        dependenciesListBuilder.reset((AOrderedListType) MetadataRecordTypes.FUNCTION_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DEPENDENCIES_FIELD_INDEX]);
        List<List<Triple<DataverseName, String, String>>> dependenciesList = function.getDependencies();
        for (List<Triple<DataverseName, String, String>> dependencies : dependenciesList) {
            dependencyListBuilder.reset(listOfLists);
            for (Triple<DataverseName, String, String> dependency : dependencies) {
                dependencyNameListBuilder.reset(stringList);
                for (String subName : getDependencySubNames(dependency)) {
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
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DEPENDENCIES_FIELD_INDEX, fieldValue);

        writeOpenFields(function);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    protected void writeOpenFields(Function function) throws HyracksDataException {
        writeArgTypes(function);
        writeWithParameters(function);
        writeLibrary(function);
        writeUnknownable(function);
        writeNullCall(function);
        writeDeterministic(function);
    }

    protected void writeWithParameters(Function function) throws HyracksDataException {
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_WITHPARAM_LIST_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        listBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
        for (Map.Entry<String, String> property : function.getParams().entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            itemValue.reset();
            writePropertyTypeRecord(name, value, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeArgTypes(Function function) throws HyracksDataException {
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_ARGTYPES_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        listBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
        for (Pair<DataverseName, IAType> p : function.getArguments()) {
            String dv = p.getFirst().getCanonicalForm();
            String type = p.getSecond().getTypeName();
            itemValue.reset();
            writeTypeRecord(dv, type, isNullableType(p.getSecond()), itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(fieldName, fieldValue);
    }

    protected boolean isNullableType(IAType definedType) {
        if ((definedType.getTypeTag() != ATypeTag.UNION) || (definedType.getTypeTag() == ATypeTag.ANY)) {
            return false;
        }

        return ((AUnionType) definedType).isNullableType();
    }

    protected void writeLibrary(Function function) throws HyracksDataException {
        if (null == function.getLibrary()) {
            return;
        }
        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_LIBRARY_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(function.getLibrary());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeUnknownable(Function function) throws HyracksDataException {
        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_NULLABILITY_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(Boolean.toString(function.isUnknownable()));
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeNullCall(Function function) throws HyracksDataException {
        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_NULLCALL_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(Boolean.toString(function.isNullCall()));
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeDeterministic(Function function) throws HyracksDataException {
        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_DETERMINISTIC_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(Boolean.toString(function.isDeterministic()));
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }

    public void writePropertyTypeRecord(String name, String value, DataOutput out) throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        ArrayBackedValueStorage fieldName = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        AMutableString aString = new AMutableString("");
        ISerializerDeserializer<AString> stringSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);

        // write field 0
        fieldName.reset();
        aString.setValue("name");
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(name);
        stringSerde.serialize(aString, fieldValue.getDataOutput());

        propertyRecordBuilder.addField(fieldName, fieldValue);

        // write field 1
        fieldName.reset();
        aString.setValue("value");
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());

        propertyRecordBuilder.addField(fieldName, fieldValue);

        propertyRecordBuilder.write(out, true);
    }

    public void writeTypeRecord(String dataverse, String type, boolean unknownable, DataOutput out)
            throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        ArrayBackedValueStorage fieldName = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        AMutableString aString = new AMutableString("");
        ISerializerDeserializer<AString> stringSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);

        // write field 0
        fieldName.reset();
        aString.setValue(TYPE_DV_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(dataverse);
        stringSerde.serialize(aString, fieldValue.getDataOutput());

        propertyRecordBuilder.addField(fieldName, fieldValue);

        // write field 1
        fieldName.reset();
        aString.setValue(TYPE_NAME_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(type);
        stringSerde.serialize(aString, fieldValue.getDataOutput());

        propertyRecordBuilder.addField(fieldName, fieldValue);

        // write field 2
        fieldName.reset();
        aString.setValue(TYPE_UNKNOWNABLE_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(Boolean.toString(unknownable));
        stringSerde.serialize(aString, fieldValue.getDataOutput());

        propertyRecordBuilder.addField(fieldName, fieldValue);

        propertyRecordBuilder.write(out, true);
    }

    private List<String> getDependencySubNames(Triple<DataverseName, String, String> dependency) {
        dependencySubnames.clear();
        dependencySubnames.add(dependency.first.getCanonicalForm());
        if (dependency.second != null) {
            dependencySubnames.add(dependency.second);
        }
        if (dependency.third != null) {
            dependencySubnames.add(dependency.third);
        }
        return dependencySubnames;
    }
}
