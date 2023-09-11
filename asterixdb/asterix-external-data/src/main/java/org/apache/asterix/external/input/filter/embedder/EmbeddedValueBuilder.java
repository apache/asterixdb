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
package org.apache.asterix.external.input.filter.embedder;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.external.input.filter.ExternalFilterValueEvaluator;
import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringWriter;

class EmbeddedValueBuilder {
    private final ARecordType allPaths;
    private final ExternalDataPrefix prefix;
    private final Map<IAType, BitSet> setValues;
    private final Stack<RecordBuilder> recordBuilders;
    private final Map<IAType, ArrayBackedValueStorage> embeddedValues;
    private final Map<String, IValueReference> fieldNames;
    private final AStringSerializerDeserializer stringSerDer;

    EmbeddedValueBuilder(ARecordType allPaths, ExternalDataPrefix prefix, Map<IAType, BitSet> setValues) {
        this.allPaths = allPaths;
        this.prefix = prefix;
        this.setValues = setValues;
        recordBuilders = new Stack<>();
        embeddedValues = new HashMap<>();
        fieldNames = new HashMap<>();
        stringSerDer = new AStringSerializerDeserializer(new UTF8StringWriter());
        setValues.put(allPaths, new BitSet(allPaths.getFieldTypes().length));
    }

    void build(String path) throws HyracksDataException {
        List<String> values = prefix.getValues(prefix.removeProtocolContainerPair(path));
        build(allPaths, values);
    }

    IValueReference getValue(IAType type) {
        return embeddedValues.get(type);
    }

    private IValueReference build(ARecordType recordType, List<String> values) throws HyracksDataException {
        RecordBuilder recordBuilder = getOrCreateRecordBuilder(recordType);
        String[] fieldNames = recordType.getFieldNames();
        IAType[] fieldTypes = recordType.getFieldTypes();

        for (int i = 0; i < fieldTypes.length; i++) {
            IValueReference valueRef = getValue(fieldTypes[i], values);
            IValueReference fieldNameRef = getOrCreateFieldName(fieldNames[i]);
            recordBuilder.addField(fieldNameRef, valueRef);
        }

        ArrayBackedValueStorage storage = getOrCreateValueStorage(recordType);
        recordBuilder.write(storage.getDataOutput(), true);
        recordBuilders.push(recordBuilder);
        return storage;
    }

    private IValueReference build(IAType flatType, List<String> values) throws HyracksDataException {
        ProjectionFiltrationTypeUtil.RenamedType leaf = (ProjectionFiltrationTypeUtil.RenamedType) flatType;
        ArrayBackedValueStorage storage = getOrCreateValueStorage(flatType);
        ATypeTag typeTag = prefix.getComputedFieldTypes().get(leaf.getIndex()).getTypeTag();
        ExternalFilterValueEvaluator.writeValue(typeTag, values.get(leaf.getIndex()), storage, stringSerDer);
        return storage;
    }

    private IValueReference getValue(IAType type, List<String> values) throws HyracksDataException {
        ATypeTag typeTag = type.getTypeTag();
        if (typeTag == ATypeTag.OBJECT) {
            return build((ARecordType) type, values);
        } else {
            return build(type, values);
        }
    }

    private RecordBuilder getOrCreateRecordBuilder(ARecordType recordType) {
        RecordBuilder recordBuilder;
        if (recordBuilders.isEmpty()) {
            recordBuilder = new RecordBuilder();
            setValues.put(recordType, new BitSet(recordType.getFieldTypes().length));
        } else {
            recordBuilder = recordBuilders.pop();
        }
        recordBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        return recordBuilder;
    }

    private ArrayBackedValueStorage getOrCreateValueStorage(IAType type) {
        ArrayBackedValueStorage storage = embeddedValues.computeIfAbsent(type, k -> new ArrayBackedValueStorage());
        storage.reset();
        return storage;
    }

    private IValueReference getOrCreateFieldName(String fieldName) throws HyracksDataException {
        IValueReference fieldNameRef = fieldNames.get(fieldName);
        if (fieldNameRef == null) {
            ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
            serializeTag(ATypeTag.STRING, storage);
            stringSerDer.serialize(fieldName, storage.getDataOutput());
            fieldNameRef = storage;
            fieldNames.put(fieldName, fieldNameRef);
        }

        return fieldNameRef;
    }

    private static void serializeTag(ATypeTag tag, ArrayBackedValueStorage storage) throws HyracksDataException {
        try {
            storage.getDataOutput().writeByte(tag.serialize());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
