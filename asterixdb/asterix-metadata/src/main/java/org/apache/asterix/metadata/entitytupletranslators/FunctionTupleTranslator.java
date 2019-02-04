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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Function metadata entity to an ITupleReference and vice versa.
 */
public class FunctionTupleTranslator extends AbstractTupleTranslator<Function> {
    private static final long serialVersionUID = 1147594449575992161L;

    // Field indexes of serialized Function in a tuple.
    // First key field.
    public static final int FUNCTION_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int FUNCTION_FUNCTIONNAME_TUPLE_FIELD_INDEX = 1;
    // Third key field.
    public static final int FUNCTION_FUNCTIONARITY_TUPLE_FIELD_INDEX = 2;

    // Payload field containing serialized Function.
    public static final int FUNCTION_PAYLOAD_TUPLE_FIELD_INDEX = 3;

    private transient OrderedListBuilder dependenciesListBuilder = new OrderedListBuilder();
    private transient OrderedListBuilder dependencyListBuilder = new OrderedListBuilder();
    private transient OrderedListBuilder dependencyNameListBuilder = new OrderedListBuilder();
    private transient AOrderedListType stringList = new AOrderedListType(BuiltinType.ASTRING, null);
    private transient AOrderedListType ListofLists =
            new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null);

    private ISerializerDeserializer<ARecord> recordSerDes =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(MetadataRecordTypes.FUNCTION_RECORDTYPE);

    protected FunctionTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FUNCTION_DATASET.getFieldCount());
    }

    @Override
    public Function getMetadataEntityFromTuple(ITupleReference frameTuple) throws HyracksDataException {
        byte[] serRecord = frameTuple.getFieldData(FUNCTION_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(FUNCTION_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(FUNCTION_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord functionRecord = recordSerDes.deserialize(in);
        return createFunctionFromARecord(functionRecord);
    }

    private Function createFunctionFromARecord(ARecord functionRecord) {
        String dataverseName =
                ((AString) functionRecord.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_DATAVERSENAME_FIELD_INDEX))
                        .getStringValue();
        String functionName =
                ((AString) functionRecord.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTIONNAME_FIELD_INDEX))
                        .getStringValue();
        String arity = ((AString) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_ARITY_FIELD_INDEX)).getStringValue();

        IACursor cursor = ((AOrderedList) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX)).getCursor();
        List<String> params = new ArrayList<>();
        while (cursor.next()) {
            params.add(((AString) cursor.get()).getStringValue());
        }

        String returnType = ((AString) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_RETURN_TYPE_FIELD_INDEX)).getStringValue();

        String definition = ((AString) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DEFINITION_FIELD_INDEX)).getStringValue();

        String language = ((AString) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_LANGUAGE_FIELD_INDEX)).getStringValue();

        String functionKind =
                ((AString) functionRecord.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_KIND_FIELD_INDEX))
                        .getStringValue();

        IACursor dependenciesCursor = ((AOrderedList) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DEPENDENCIES_FIELD_INDEX)).getCursor();
        List<List<List<String>>> dependencies = new ArrayList<>();
        AOrderedList dependencyList;
        AOrderedList qualifiedList;
        int i = 0;
        while (dependenciesCursor.next()) {
            dependencies.add(new ArrayList<>());
            dependencyList = (AOrderedList) dependenciesCursor.get();
            IACursor qualifiedDependencyCursor = dependencyList.getCursor();
            int j = 0;
            while (qualifiedDependencyCursor.next()) {
                qualifiedList = (AOrderedList) qualifiedDependencyCursor.get();
                IACursor qualifiedNameCursor = qualifiedList.getCursor();
                dependencies.get(i).add(new ArrayList<>());
                while (qualifiedNameCursor.next()) {
                    dependencies.get(i).get(j).add(((AString) qualifiedNameCursor.get()).getStringValue());
                }
                j++;
            }
            i++;

        }

        FunctionSignature signature = new FunctionSignature(dataverseName, functionName, Integer.parseInt(arity));
        return new Function(signature, params, returnType, definition, language, functionKind, dependencies);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Function function)
            throws HyracksDataException, AlgebricksException {
        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(function.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(function.getName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(function.getArity() + "");
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the fourth field of the tuple

        recordBuilder.reset(MetadataRecordTypes.FUNCTION_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(function.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(function.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTIONNAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(function.getArity() + "");
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_ARITY_FIELD_INDEX, fieldValue);

        // write field 3
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset((AOrderedListType) MetadataRecordTypes.FUNCTION_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX]);
        for (String param : function.getArguments()) {
            itemValue.reset();
            aString.setValue(param);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(function.getReturnType());
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
        List<List<List<String>>> dependenciesList = function.getDependencies();
        for (List<List<String>> dependencies : dependenciesList) {
            dependencyListBuilder.reset(ListofLists);
            for (List<String> dependency : dependencies) {
                dependencyNameListBuilder.reset(stringList);
                for (String subName : dependency) {
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

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

}
