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

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_LIBRARY_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_LIBRARY_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_RETURN_TYPE_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_RETURN_TYPE_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_TYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_VALUE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DETERMINISTIC_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_EXTERNAL_IDENTIFIER_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_LIBRARY_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_NULLCALL_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAMTYPES_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_RESOURCES_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_WITHPARAMS_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.PROPERTIES_NAME_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.PROPERTIES_VALUE_FIELD_NAME;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.DependencyFullyQualifiedName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.bootstrap.FunctionEntity;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.BuiltinTypeMap;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Function metadata entity to an ITupleReference and vice versa.
 */
public class FunctionTupleTranslator extends AbstractDatatypeTupleTranslator<Function> {

    private final FunctionEntity functionEntity;
    protected OrderedListBuilder dependenciesListBuilder;
    protected OrderedListBuilder dependencyListBuilder;
    protected OrderedListBuilder dependencyNameListBuilder;
    protected List<String> dependencySubnames;
    protected AOrderedListType stringList;
    protected AOrderedListType listOfLists;

    protected FunctionTupleTranslator(TxnId txnId, MetadataNode metadataNode, boolean getTuple,
            FunctionEntity functionEntity) {
        super(txnId, metadataNode, getTuple, functionEntity.getIndex(), functionEntity.payloadPosition());
        this.functionEntity = functionEntity;
        if (getTuple) {
            dependenciesListBuilder = new OrderedListBuilder();
            dependencyListBuilder = new OrderedListBuilder();
            dependencyNameListBuilder = new OrderedListBuilder();
            dependencySubnames = new ArrayList<>(4);
            stringList = new AOrderedListType(BuiltinType.ASTRING, null);
            listOfLists = new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null);
        }
    }

    protected Function createMetadataEntityFromARecord(ARecord functionRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) functionRecord.getValueByPos(functionEntity.dataverseNameIndex())).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        int databaseNameIndex = functionEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) functionRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        String functionName =
                ((AString) functionRecord.getValueByPos(functionEntity.functionNameIndex())).getStringValue();
        int arity = Integer.parseInt(
                ((AString) functionRecord.getValueByPos(functionEntity.functionArityIndex())).getStringValue());

        IACursor paramNameCursor =
                ((AOrderedList) functionRecord.getValueByPos(functionEntity.functionParamListIndex())).getCursor();
        List<String> paramNames = new ArrayList<>();
        while (paramNameCursor.next()) {
            paramNames.add(((AString) paramNameCursor.get()).getStringValue());
        }

        List<TypeSignature> paramTypes = getParamTypes(functionRecord, databaseName, dataverseName);

        TypeSignature returnType;
        String returnTypeName =
                ((AString) functionRecord.getValueByPos(functionEntity.functionReturnTypeIndex())).getStringValue();
        if (returnTypeName.isEmpty()) {
            returnType = null; // == any
        } else {
            returnType = getTypeSignature(functionRecord, returnTypeName, FIELD_NAME_RETURN_TYPE_DATABASE_NAME,
                    FIELD_NAME_RETURN_TYPE_DATAVERSE_NAME, databaseName, dataverseName);
        }

        String definition =
                ((AString) functionRecord.getValueByPos(functionEntity.functionDefinitionIndex())).getStringValue();
        String language =
                ((AString) functionRecord.getValueByPos(functionEntity.functionLanguageIndex())).getStringValue();
        String functionKind =
                ((AString) functionRecord.getValueByPos(functionEntity.functionKindIndex())).getStringValue();

        Map<String, String> resources = null;
        String libraryDatabaseName = null;
        DataverseName libraryDataverseName = null;
        String libraryName;
        List<String> externalIdentifier = null;
        AOrderedList externalIdentifierList =
                getOrderedList(functionRecord, FUNCTION_ARECORD_FUNCTION_EXTERNAL_IDENTIFIER_FIELD_NAME);
        if (externalIdentifierList != null) {
            externalIdentifier = new ArrayList<>(externalIdentifierList.size());
            IACursor externalIdentifierCursor = externalIdentifierList.getCursor();
            while (externalIdentifierCursor.next()) {
                externalIdentifier.add(((AString) externalIdentifierCursor.get()).getStringValue());
            }
            libraryName = getString(functionRecord, MetadataRecordTypes.FIELD_NAME_LIBRARY_NAME);
            String libraryDataverseCanonicalName = getString(functionRecord, FIELD_NAME_LIBRARY_DATAVERSE_NAME);
            libraryDataverseName = DataverseName.createFromCanonicalForm(libraryDataverseCanonicalName);
            libraryDatabaseName = getString(functionRecord, FIELD_NAME_LIBRARY_DATABASE_NAME);
            if (libraryDatabaseName == null) {
                libraryDatabaseName = MetadataUtil.databaseFor(libraryDataverseName);
            }
            resources = getResources(functionRecord, FUNCTION_ARECORD_FUNCTION_RESOURCES_FIELD_NAME);
            definition = null;
        } else {
            // back-compat. get external identifier from function body
            libraryName = getString(functionRecord, FUNCTION_ARECORD_FUNCTION_LIBRARY_FIELD_NAME);
            if (libraryName != null) {
                libraryDatabaseName = databaseName;
                libraryDataverseName = dataverseName;
                externalIdentifier =
                        decodeExternalIdentifierBackCompat(definition, ExternalFunctionLanguage.valueOf(language));
                resources = getResources(functionRecord, FUNCTION_ARECORD_FUNCTION_WITHPARAMS_FIELD_NAME);
            }
        }

        Boolean nullCall = null;
        Boolean deterministic = null;
        if (externalIdentifier != null) {
            nullCall = getBoolean(functionRecord, FUNCTION_ARECORD_FUNCTION_NULLCALL_FIELD_NAME);
            deterministic = getBoolean(functionRecord, FUNCTION_ARECORD_FUNCTION_DETERMINISTIC_FIELD_NAME);
        }

        IACursor dependenciesCursor =
                ((AOrderedList) functionRecord.getValueByPos(functionEntity.functionDependenciesIndex())).getCursor();
        List<List<DependencyFullyQualifiedName>> dependencies = new ArrayList<>();
        while (dependenciesCursor.next()) {
            List<DependencyFullyQualifiedName> dependencyList = new ArrayList<>();
            IACursor qualifiedDependencyCursor = ((AOrderedList) dependenciesCursor.get()).getCursor();
            while (qualifiedDependencyCursor.next()) {
                DependencyFullyQualifiedName dependency = getDependency((AOrderedList) qualifiedDependencyCursor.get());
                dependencyList.add(dependency);
            }
            dependencies.add(dependencyList);
        }

        FunctionSignature signature = new FunctionSignature(databaseName, dataverseName, functionName, arity);

        return new Function(signature, paramNames, paramTypes, returnType, definition, functionKind, language,
                libraryDatabaseName, libraryDataverseName, libraryName, externalIdentifier, nullCall, deterministic,
                resources, dependencies);
    }

    private List<TypeSignature> getParamTypes(ARecord functionRecord, String functionDatabaseName,
            DataverseName functionDataverseName) throws AlgebricksException {
        ARecordType functionRecordType = functionRecord.getType();
        int paramTypesFieldIdx = functionRecordType.getFieldIndex(FUNCTION_ARECORD_FUNCTION_PARAMTYPES_FIELD_NAME);
        if (paramTypesFieldIdx < 0) {
            return null;
        }

        AOrderedList paramTypeList = (AOrderedList) functionRecord.getValueByPos(paramTypesFieldIdx);
        List<TypeSignature> paramTypes = new ArrayList<>(paramTypeList.size());
        IACursor cursor = paramTypeList.getCursor();
        while (cursor.next()) {
            IAObject paramTypeObject = cursor.get();
            TypeSignature paramType;
            switch (paramTypeObject.getType().getTypeTag()) {
                case NULL:
                    paramType = null; // == any
                    break;
                case OBJECT:
                    ARecord paramTypeRecord = (ARecord) paramTypeObject;
                    String paramTypeName = getString(paramTypeRecord, FIELD_NAME_TYPE);
                    paramType = getTypeSignature(paramTypeRecord, paramTypeName, FIELD_NAME_DATABASE_NAME,
                            FIELD_NAME_DATAVERSE_NAME, functionDatabaseName, functionDataverseName);
                    break;
                default:
                    throw new AsterixException(ErrorCode.METADATA_ERROR, paramTypeObject.getType().getTypeName());
            }
            paramTypes.add(paramType);
        }
        return paramTypes;
    }

    private TypeSignature getTypeSignature(ARecord record, String typeName, String dbFieldName, String dvFieldName,
            String functionDatabaseName, DataverseName functionDataverseName) throws AlgebricksException {
        // back-compat: handle "any"
        if (BuiltinType.ANY.getTypeName().equals(typeName)) {
            return null; // == any
        }
        BuiltinType builtinType = BuiltinTypeMap.getBuiltinType(typeName);
        if (builtinType != null) {
            return new TypeSignature(builtinType);
        }

        String typeDatabaseName;
        DataverseName typeDataverseName;
        String typeDataverseNameCanonical = getString(record, dvFieldName);
        if (typeDataverseNameCanonical == null) {
            typeDataverseName = functionDataverseName;
            typeDatabaseName = functionDatabaseName;
        } else {
            typeDataverseName = DataverseName.createFromCanonicalForm(typeDataverseNameCanonical);
            typeDatabaseName = getString(record, dbFieldName);
            if (typeDatabaseName == null) {
                typeDatabaseName = MetadataUtil.databaseFor(typeDataverseName);
            }
        }

        return new TypeSignature(typeDatabaseName, typeDataverseName, typeName);
    }

    private Map<String, String> getResources(ARecord functionRecord, String resourcesFieldName) {
        Map<String, String> adaptorConfiguration = null;
        final ARecordType functionType = functionRecord.getType();
        final int functionLibraryIdx = functionType.getFieldIndex(resourcesFieldName);
        if (functionLibraryIdx >= 0) {
            adaptorConfiguration = new HashMap<>();
            IACursor cursor = ((AOrderedList) functionRecord.getValueByPos(functionLibraryIdx)).getCursor();
            while (cursor.next()) {
                ARecord field = (ARecord) cursor.get();
                final ARecordType fieldType = field.getType();
                final int keyIdx = fieldType.getFieldIndex(PROPERTIES_NAME_FIELD_NAME);
                String key = keyIdx >= 0 ? ((AString) field.getValueByPos(keyIdx)).getStringValue() : "";
                final int valueIdx = fieldType.getFieldIndex(PROPERTIES_VALUE_FIELD_NAME);
                String value = valueIdx >= 0 ? ((AString) field.getValueByPos(valueIdx)).getStringValue() : "";
                adaptorConfiguration.put(key, value);
            }
        }
        return adaptorConfiguration;
    }

    private String getString(ARecord aRecord, String fieldName) {
        final ARecordType functionType = aRecord.getType();
        final int fieldIndex = functionType.getFieldIndex(fieldName);
        return fieldIndex >= 0 ? ((AString) aRecord.getValueByPos(fieldIndex)).getStringValue() : null;
    }

    private Boolean getBoolean(ARecord aRecord, String fieldName) {
        final ARecordType functionType = aRecord.getType();
        final int fieldIndex = functionType.getFieldIndex(fieldName);
        return fieldIndex >= 0 ? ((ABoolean) aRecord.getValueByPos(fieldIndex)).getBoolean() : null;
    }

    private AOrderedList getOrderedList(ARecord aRecord, String fieldName) {
        final ARecordType aRecordType = aRecord.getType();
        final int fieldIndex = aRecordType.getFieldIndex(fieldName);
        return fieldIndex >= 0 ? ((AOrderedList) aRecord.getValueByPos(fieldIndex)) : null;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Function function) throws HyracksDataException {
        DataverseName dataverseName = function.getDataverseName();
        String dataverseCanonicalName = dataverseName.getCanonicalForm();

        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
        if (functionEntity.databaseNameIndex() >= 0) {
            aString.setValue(function.getDatabaseName());
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
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

        recordBuilder.reset(functionEntity.getRecordType());

        if (functionEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(function.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(functionEntity.databaseNameIndex(), fieldValue);
        }
        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(functionEntity.dataverseNameIndex(), fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(function.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(functionEntity.functionNameIndex(), fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(String.valueOf(function.getArity()));
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(functionEntity.functionArityIndex(), fieldValue);

        // write field 3
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset((AOrderedListType) functionEntity.getRecordType().getFieldTypes()[functionEntity
                .functionParamListIndex()]);
        for (String p : function.getParameterNames()) {
            itemValue.reset();
            aString.setValue(p);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(functionEntity.functionParamListIndex(), fieldValue);

        // write field 4
        // Note: return type's dataverse name is written later in the open part
        TypeSignature returnType = function.getReturnType();
        fieldValue.reset();
        aString.setValue(returnType != null ? returnType.getName() : "");
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(functionEntity.functionReturnTypeIndex(), fieldValue);

        // write field 5
        fieldValue.reset();
        aString.setValue(function.isExternal() ? "" : function.getFunctionBody());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(functionEntity.functionDefinitionIndex(), fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(function.getLanguage());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(functionEntity.functionLanguageIndex(), fieldValue);

        // write field 7
        fieldValue.reset();
        aString.setValue(function.getKind());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(functionEntity.functionKindIndex(), fieldValue);

        // write field 8
        dependenciesListBuilder.reset((AOrderedListType) functionEntity.getRecordType().getFieldTypes()[functionEntity
                .functionDependenciesIndex()]);
        List<List<DependencyFullyQualifiedName>> dependenciesList = function.getDependencies();
        boolean writeDatabase = functionEntity.databaseNameIndex() >= 0;
        for (List<DependencyFullyQualifiedName> dependencies : dependenciesList) {
            dependencyListBuilder.reset(listOfLists);
            writeDeps(dependencyListBuilder, itemValue, dependencies, dependencyNameListBuilder, stringList,
                    writeDatabase, aString, stringSerde);
            itemValue.reset();
            dependencyListBuilder.write(itemValue.getDataOutput(), true);
            dependenciesListBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        dependenciesListBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(functionEntity.functionDependenciesIndex(), fieldValue);

        writeOpenFields(function);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    protected void writeOpenFields(Function function) throws HyracksDataException {
        writeReturnTypeDataverseName(function);
        writeParameterTypes(function);
        writeResources(function);
        writeLibrary(function);
        writeNullCall(function);
        writeDeterministic(function);
    }

    protected void writeResources(Function function) throws HyracksDataException {
        Map<String, String> withParams = function.getResources();
        if (withParams == null || withParams.isEmpty()) {
            return;
        }

        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
        for (Map.Entry<String, String> property : withParams.entrySet()) {
            itemValue.reset();
            writePropertyTypeRecord(property.getKey(), property.getValue(), itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);

        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_RESOURCES_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());

        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeParameterTypes(Function function) throws HyracksDataException {
        List<TypeSignature> parameterTypes = function.getParameterTypes();
        if (parameterTypes == null) {
            return;
        }
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
        for (TypeSignature paramType : parameterTypes) {
            itemValue.reset();
            if (paramType == null) {
                nullSerde.serialize(ANull.NULL, itemValue.getDataOutput());
            } else {
                writeTypeRecord(paramType.getDatabaseName(), paramType.getDataverseName(), paramType.getName(),
                        function.getDatabaseName(), function.getDataverseName(), itemValue.getDataOutput());
            }
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);

        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_PARAMTYPES_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());

        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeLibrary(Function function) throws HyracksDataException {
        if (!function.isExternal()) {
            return;
        }

        if (functionEntity.databaseNameIndex() >= 0) {
            fieldName.reset();
            aString.setValue(FIELD_NAME_LIBRARY_DATABASE_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aString.setValue(function.getLibraryDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }

        fieldName.reset();
        aString.setValue(FIELD_NAME_LIBRARY_DATAVERSE_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(function.getLibraryDataverseName().getCanonicalForm());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);

        fieldName.reset();
        aString.setValue(MetadataRecordTypes.FIELD_NAME_LIBRARY_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(function.getLibraryName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);

        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_EXTERNAL_IDENTIFIER_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset(stringList);
        for (String externalIdPart : function.getExternalIdentifier()) {
            itemValue.reset();
            aString.setValue(externalIdPart);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeReturnTypeDataverseName(Function function) throws HyracksDataException {
        TypeSignature returnType = function.getReturnType();
        if (returnType == null) {
            return;
        }
        DataverseName returnTypeDataverseName = returnType.getDataverseName();
        String returnTypeDatabaseName = returnType.getDatabaseName();
        if (returnTypeDataverseName == null || (returnTypeDatabaseName.equals(function.getDatabaseName())
                && returnTypeDataverseName.equals(function.getDataverseName()))) {
            return;
        }
        if (functionEntity.databaseNameIndex() >= 0) {
            fieldName.reset();
            aString.setValue(FIELD_NAME_RETURN_TYPE_DATABASE_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aString.setValue(returnTypeDatabaseName);
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }
        fieldName.reset();
        aString.setValue(FIELD_NAME_RETURN_TYPE_DATAVERSE_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(returnTypeDataverseName.getCanonicalForm());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeNullCall(Function function) throws HyracksDataException {
        if (function.getNullCall() == null) {
            return;
        }
        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_NULLCALL_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        booleanSerde.serialize(ABoolean.valueOf(function.getNullCall()), fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeDeterministic(Function function) throws HyracksDataException {
        if (function.getDeterministic() == null) {
            return;
        }
        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_DETERMINISTIC_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        booleanSerde.serialize(ABoolean.valueOf(function.getDeterministic()), fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }

    public void writePropertyTypeRecord(String name, String value, DataOutput out) throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        propertyRecordBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);

        // write field 0
        fieldName.reset();
        aString.setValue(FIELD_NAME_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(name);
        stringSerde.serialize(aString, fieldValue.getDataOutput());

        propertyRecordBuilder.addField(fieldName, fieldValue);

        // write field 1
        fieldName.reset();
        aString.setValue(FIELD_NAME_VALUE);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());

        propertyRecordBuilder.addField(fieldName, fieldValue);

        propertyRecordBuilder.write(out, true);
    }

    public void writeTypeRecord(String typeDatabaseName, DataverseName typeDataverseName, String typeName,
            String functionDatabaseName, DataverseName functionDataverseName, DataOutput out)
            throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        propertyRecordBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);

        // write field "Type"
        fieldName.reset();
        aString.setValue(FIELD_NAME_TYPE);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(typeName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(fieldName, fieldValue);

        // write field "DataverseName"
        boolean skipTypeDataverseName = typeDataverseName == null
                || (typeDatabaseName.equals(functionDatabaseName) && typeDataverseName.equals(functionDataverseName));
        if (!skipTypeDataverseName) {
            if (functionEntity.databaseNameIndex() >= 0) {
                fieldName.reset();
                aString.setValue(FIELD_NAME_DATABASE_NAME);
                stringSerde.serialize(aString, fieldName.getDataOutput());
                fieldValue.reset();
                aString.setValue(typeDatabaseName);
                stringSerde.serialize(aString, fieldValue.getDataOutput());
                propertyRecordBuilder.addField(fieldName, fieldValue);
            }

            fieldName.reset();
            aString.setValue(FIELD_NAME_DATAVERSE_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aString.setValue(typeDataverseName.getCanonicalForm());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            propertyRecordBuilder.addField(fieldName, fieldValue);
        }

        propertyRecordBuilder.write(out, true);
    }

    // back-compat
    private static List<String> decodeExternalIdentifierBackCompat(String encodedValue,
            ExternalFunctionLanguage language) throws AlgebricksException {
        switch (language) {
            case JAVA:
                // input: class
                //
                // output:
                // [0] = class
                return Collections.singletonList(encodedValue);

            case PYTHON:
                // input:
                //  case 1 (method): package.module:class.method
                //  case 2 (function): package.module:function
                //
                // output:
                //  case 1:
                //    [0] = package.module
                //    [1] = class.method
                //  case 2:
                //    [0] = package.module
                //    [1] = function
                int idx = encodedValue.lastIndexOf(':');
                if (idx < 0) {
                    throw new AsterixException(ErrorCode.METADATA_ERROR, encodedValue);
                }
                return Arrays.asList(encodedValue.substring(0, idx), encodedValue.substring(idx + 1));

            default:
                throw new AsterixException(ErrorCode.METADATA_ERROR, language);
        }
    }
}
