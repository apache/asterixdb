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

import static org.apache.asterix.om.types.AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.Collection;
import java.util.List;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.DependencyFullyQualifiedName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.api.IMetadataEntityTupleTranslator;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Contains common members shared across all concrete implementations of
 * IMetadataEntityTupleTranslator.
 */
public abstract class AbstractTupleTranslator<T> implements IMetadataEntityTupleTranslator<T> {

    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ANull> nullSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AString> stringSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ABoolean> booleanSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AInt32> int32Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    protected final ISerializerDeserializer<ARecord> recordSerDes;

    protected AMutableString aString = new AMutableString("");

    protected final ARecordType payloadRecordType;
    protected final int payloadTupleFieldIndex;
    protected final IARecordBuilder recordBuilder;
    protected final ArrayBackedValueStorage fieldName;
    protected final ArrayBackedValueStorage fieldValue;
    protected final ArrayTupleBuilder tupleBuilder;
    protected final ArrayTupleReference tuple;

    @SuppressWarnings("unchecked")
    protected AbstractTupleTranslator(boolean getTuple, IMetadataIndex metadataIndex, int payloadTupleFieldIndex) {
        payloadRecordType = metadataIndex.getPayloadRecordType();
        recordSerDes = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(payloadRecordType);
        this.payloadTupleFieldIndex = payloadTupleFieldIndex;
        if (getTuple) {
            recordBuilder = new RecordBuilder();
            fieldName = payloadRecordType.isOpen() ? new ArrayBackedValueStorage() : null;
            fieldValue = new ArrayBackedValueStorage();
            tupleBuilder = new ArrayTupleBuilder(metadataIndex.getFieldCount());
            tuple = new ArrayTupleReference();
        } else {
            recordBuilder = null;
            fieldName = null;
            fieldValue = null;
            tupleBuilder = null;
            tuple = null;
        }
    }

    @Override
    public final T getMetadataEntityFromTuple(ITupleReference frameTuple)
            throws HyracksDataException, AlgebricksException {
        byte[] serRecord = frameTuple.getFieldData(payloadTupleFieldIndex);
        int recordStartOffset = frameTuple.getFieldStart(payloadTupleFieldIndex);
        int recordLength = frameTuple.getFieldLength(payloadTupleFieldIndex);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord datasetRecord = recordSerDes.deserialize(in);
        return createMetadataEntityFromARecord(datasetRecord);
    }

    protected abstract T createMetadataEntityFromARecord(ARecord aRecord)
            throws HyracksDataException, AlgebricksException;

    public static void getDependencySubNames(DependencyFullyQualifiedName dependency,
            Collection<? super String> outSubnames) {
        outSubnames.add(dependency.getDatabaseName());
        outSubnames.add(dependency.getDataverseName().getCanonicalForm());
        if (dependency.getSubName1() != null) {
            outSubnames.add(dependency.getSubName1());
        }
        if (dependency.getSubName2() != null) {
            outSubnames.add(dependency.getSubName2());
        }
    }

    public static DependencyFullyQualifiedName getDependency(AOrderedList dependencySubnames)
            throws AlgebricksException {
        // there must be at least one item (the dataverse)
        int currentIdx = 0;
        IAObject databaseMarker = dependencySubnames.getItem(currentIdx);
        String databaseName;
        DataverseName dataverseName;
        if (((AString) databaseMarker).getStringValue().isEmpty()) {
            // move to database value
            currentIdx++;
            databaseName = ((AString) dependencySubnames.getItem(currentIdx)).getStringValue();
            // move to dataverse value
            currentIdx++;
            String dataverseCanonicalName = ((AString) dependencySubnames.getItem(currentIdx)).getStringValue();
            dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        } else {
            String dataverseCanonicalName = ((AString) dependencySubnames.getItem(currentIdx)).getStringValue();
            dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        currentIdx++;
        String second = null, third = null;
        int ln = dependencySubnames.size();
        if (currentIdx < ln) {
            second = ((AString) dependencySubnames.getItem(currentIdx)).getStringValue();
            currentIdx++;
            if (currentIdx < ln) {
                third = ((AString) dependencySubnames.getItem(currentIdx)).getStringValue();
            }
        }
        return new DependencyFullyQualifiedName(databaseName, dataverseName, second, third);
    }

    protected static String getStringValue(IAObject obj) {
        return obj.getType().getTypeTag() == ATypeTag.STRING ? ((AString) obj).getStringValue() : null;
    }

    protected static Triple<String, String, String> getDateTimeFormats(ARecord record) {
        Triple<String, String, String> formats = new Triple<>(null, null, null);
        int formatFieldPos = record.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_DATA_FORMAT);
        if (formatFieldPos >= 0) {
            IACursor formatCursor = ((AOrderedList) record.getValueByPos(formatFieldPos)).getCursor();
            if (formatCursor.next()) {
                formats.first = getStringValue(formatCursor.get());
                if (formatCursor.next()) {
                    formats.second = getStringValue(formatCursor.get());
                    if (formatCursor.next()) {
                        formats.third = getStringValue(formatCursor.get());
                    }
                }
            }
        }
        return formats;
    }

    public static void writeDateTimeFormats(String datetimeFormat, String dateFormat, String timeFormat,
            IARecordBuilder recordBuilder, AMutableString aString, ISerializerDeserializer<ANull> nullSerde,
            ISerializerDeserializer<AString> stringSerde, ArrayBackedValueStorage nameValue,
            ArrayBackedValueStorage fieldValue, ArrayBackedValueStorage itemValue) throws HyracksDataException {
        if (datetimeFormat != null || dateFormat != null || timeFormat != null) {
            nameValue.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_DATA_FORMAT);
            stringSerde.serialize(aString, nameValue.getDataOutput());

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
            recordBuilder.addField(nameValue, fieldValue);
        }
    }

    public static void writeString(ArrayBackedValueStorage itemValue, AMutableString aString, String value,
            ISerializerDeserializer<AString> stringSerde) throws HyracksDataException {
        itemValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, itemValue.getDataOutput());
    }

    public static void writeDeps(OrderedListBuilder dependencyListBuilder, ArrayBackedValueStorage itemValue,
            List<DependencyFullyQualifiedName> dependencies, OrderedListBuilder dependencyNameListBuilder,
            AOrderedListType stringList, boolean writeDatabase, AMutableString aString,
            ISerializerDeserializer<AString> stringSerde) throws HyracksDataException {
        for (DependencyFullyQualifiedName dependency : dependencies) {
            dependencyNameListBuilder.reset(stringList);

            if (writeDatabase) {
                writeString(itemValue, aString, "", stringSerde);
                dependencyNameListBuilder.addItem(itemValue);

                writeString(itemValue, aString, dependency.getDatabaseName(), stringSerde);
                dependencyNameListBuilder.addItem(itemValue);
            }

            // write dataverse
            writeString(itemValue, aString, dependency.getDataverseName().getCanonicalForm(), stringSerde);
            dependencyNameListBuilder.addItem(itemValue);

            // write subName1
            String subName1 = dependency.getSubName1();
            if (subName1 != null) {
                writeString(itemValue, aString, subName1, stringSerde);
                dependencyNameListBuilder.addItem(itemValue);
            }

            // write subName2
            String subName2 = dependency.getSubName2();
            if (subName2 != null) {
                writeString(itemValue, aString, subName2, stringSerde);
                dependencyNameListBuilder.addItem(itemValue);
            }
            itemValue.reset();
            dependencyNameListBuilder.write(itemValue.getDataOutput(), true);
            dependencyListBuilder.addItem(itemValue);
        }
    }
}
