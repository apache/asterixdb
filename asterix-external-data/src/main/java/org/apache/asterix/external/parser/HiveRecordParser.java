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
package org.apache.asterix.external.parser;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringWriter;

@SuppressWarnings("deprecation")
public class HiveRecordParser implements IRecordDataParser<Writable> {

    private ARecordType aRecord;
    private SerDe hiveSerde;
    private StructObjectInspector oi;
    private IARecordBuilder recBuilder;
    private ArrayBackedValueStorage fieldValueBuffer;
    private ArrayBackedValueStorage listItemBuffer;
    private byte[] fieldTypeTags;
    private IAType[] fieldTypes;
    private OrderedListBuilder orderedListBuilder;
    private UnorderedListBuilder unorderedListBuilder;
    private List<? extends StructField> fieldRefs;
    private UTF8StringWriter utf8Writer = new UTF8StringWriter();

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType recordType) throws HyracksDataException {
        try {
            this.aRecord = recordType;
            int n = aRecord.getFieldNames().length;
            fieldTypes = aRecord.getFieldTypes();
            JobConf hadoopConfiguration = HDFSUtils.configureHDFSJobConf(configuration);
            //create the hive table schema.
            Properties tbl = new Properties();
            tbl.put(Constants.LIST_COLUMNS, getCommaDelimitedColNames(aRecord));
            tbl.put(Constants.LIST_COLUMN_TYPES, getColTypes(aRecord));
            String hiveSerdeClassName = configuration.get(ExternalDataConstants.KEY_HIVE_SERDE);
            if (hiveSerdeClassName == null) {
                throw new IllegalArgumentException("no hive serde provided for hive deserialized records");
            }
            hiveSerde = (SerDe) Class.forName(hiveSerdeClassName).newInstance();
            hiveSerde.initialize(hadoopConfiguration, tbl);
            oi = (StructObjectInspector) hiveSerde.getObjectInspector();

            fieldValueBuffer = new ArrayBackedValueStorage();
            recBuilder = new RecordBuilder();
            recBuilder.reset(aRecord);
            recBuilder.init();
            fieldTypeTags = new byte[n];
            for (int i = 0; i < n; i++) {
                ATypeTag tag = aRecord.getFieldTypes()[i].getTypeTag();
                fieldTypeTags[i] = tag.serialize();
            }
            fieldRefs = oi.getAllStructFieldRefs();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void parse(IRawRecord<? extends Writable> record, DataOutput out) throws Exception {
        Writable hiveRawRecord = record.get();
        Object hiveObject = hiveSerde.deserialize(hiveRawRecord);
        int n = aRecord.getFieldNames().length;
        List<Object> attributesValues = oi.getStructFieldsDataAsList(hiveObject);
        recBuilder.reset(aRecord);
        recBuilder.init();
        for (int i = 0; i < n; i++) {
            final Object value = attributesValues.get(i);
            final ObjectInspector foi = fieldRefs.get(i).getFieldObjectInspector();
            fieldValueBuffer.reset();
            final DataOutput dataOutput = fieldValueBuffer.getDataOutput();
            dataOutput.writeByte(fieldTypeTags[i]);
            //get field type
            parseItem(fieldTypes[i], value, foi, dataOutput, false);
            recBuilder.addField(i, fieldValueBuffer);
        }
        recBuilder.write(out, true);

    }

    private void parseItem(IAType itemType, Object value, ObjectInspector foi, DataOutput dataOutput,
            boolean primitiveOnly) throws IOException {
        switch (itemType.getTypeTag()) {
            case BOOLEAN:
                parseBoolean(value, (BooleanObjectInspector) foi, dataOutput);
                break;
            case TIME:
                parseTime(value, (TimestampObjectInspector) foi, dataOutput);
                break;
            case DATE:
                parseDate(value, (TimestampObjectInspector) foi, dataOutput);
                break;
            case DATETIME:
                parseDateTime(value, (TimestampObjectInspector) foi, dataOutput);
                break;
            case DOUBLE:
                parseDouble(value, (DoubleObjectInspector) foi, dataOutput);
                break;
            case FLOAT:
                parseFloat(value, (FloatObjectInspector) foi, dataOutput);
                break;
            case INT8:
                parseInt8(value, (ByteObjectInspector) foi, dataOutput);
                break;
            case INT16:
                parseInt16(value, (ShortObjectInspector) foi, dataOutput);
                break;
            case INT32:
                parseInt32(value, (IntObjectInspector) foi, dataOutput);
                break;
            case INT64:
                parseInt64(value, (LongObjectInspector) foi, dataOutput);
                break;
            case STRING:
                parseString(value, (StringObjectInspector) foi, dataOutput);
                break;
            case ORDEREDLIST:
                if (primitiveOnly) {
                    throw new HyracksDataException("doesn't support hive data with list of non-primitive types");
                }
                parseOrderedList((AOrderedListType) itemType, value, (ListObjectInspector) foi);
                break;
            case UNORDEREDLIST:
                if (primitiveOnly) {
                    throw new HyracksDataException("doesn't support hive data with list of non-primitive types");
                }
                parseUnorderedList((AUnorderedListType) itemType, value, (ListObjectInspector) foi);
                break;
            default:
                throw new HyracksDataException("Can't get hive type for field of type " + itemType.getTypeTag());
        }
    }

    @Override
    public Class<? extends Writable> getRecordClass() {
        return Writable.class;
    }

    private Object getColTypes(ARecordType record) throws Exception {
        int n = record.getFieldTypes().length;
        if (n < 1) {
            throw new HyracksDataException("Failed to get columns of record");
        }
        //First Column
        String cols = getHiveTypeString(record.getFieldTypes(), 0);
        for (int i = 1; i < n; i++) {
            cols = cols + "," + getHiveTypeString(record.getFieldTypes(), i);
        }
        return cols;
    }

    private String getCommaDelimitedColNames(ARecordType record) throws Exception {
        if (record.getFieldNames().length < 1) {
            throw new HyracksDataException("Can't deserialize hive records with no closed columns");
        }

        String cols = record.getFieldNames()[0];
        for (int i = 1; i < record.getFieldNames().length; i++) {
            cols = cols + "," + record.getFieldNames()[i];
        }
        return cols;
    }

    private String getHiveTypeString(IAType[] types, int i) throws Exception {
        final IAType type = types[i];
        ATypeTag tag = type.getTypeTag();
        if (tag == ATypeTag.UNION) {
            if (NonTaggedFormatUtil.isOptional(type)) {
                throw new NotImplementedException("Non-optional UNION type is not supported.");
            }
            tag = ((AUnionType) type).getNullableType().getTypeTag();
        }
        if (tag == null) {
            throw new NotImplementedException("Failed to get the type information for field " + i + ".");
        }
        switch (tag) {
            case BOOLEAN:
                return Constants.BOOLEAN_TYPE_NAME;
            case DATE:
                return Constants.DATE_TYPE_NAME;
            case DATETIME:
                return Constants.DATETIME_TYPE_NAME;
            case DOUBLE:
                return Constants.DOUBLE_TYPE_NAME;
            case FLOAT:
                return Constants.FLOAT_TYPE_NAME;
            case INT16:
                return Constants.SMALLINT_TYPE_NAME;
            case INT32:
                return Constants.INT_TYPE_NAME;
            case INT64:
                return Constants.BIGINT_TYPE_NAME;
            case INT8:
                return Constants.TINYINT_TYPE_NAME;
            case ORDEREDLIST:
                return Constants.LIST_TYPE_NAME;
            case STRING:
                return Constants.STRING_TYPE_NAME;
            case TIME:
                return Constants.DATETIME_TYPE_NAME;
            case UNORDEREDLIST:
                return Constants.LIST_TYPE_NAME;
            default:
                throw new HyracksDataException("Can't get hive type for field of type " + tag);
        }
    }

    private void parseInt64(Object obj, LongObjectInspector foi, DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(foi.get(obj));
    }

    private void parseInt32(Object obj, IntObjectInspector foi, DataOutput dataOutput) throws IOException {
        if (obj == null) {
            throw new HyracksDataException("can't parse null field");
        }
        dataOutput.writeInt(foi.get(obj));
    }

    private void parseInt16(Object obj, ShortObjectInspector foi, DataOutput dataOutput) throws IOException {
        dataOutput.writeShort(foi.get(obj));
    }

    private void parseFloat(Object obj, FloatObjectInspector foi, DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(foi.get(obj));
    }

    private void parseDouble(Object obj, DoubleObjectInspector foi, DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(foi.get(obj));
    }

    private void parseDateTime(Object obj, TimestampObjectInspector foi, DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(foi.getPrimitiveJavaObject(obj).getTime());
    }

    private void parseDate(Object obj, TimestampObjectInspector foi, DataOutput dataOutput) throws IOException {
        long chrononTimeInMs = foi.getPrimitiveJavaObject(obj).getTime();
        short temp = 0;
        if (chrononTimeInMs < 0 && chrononTimeInMs % GregorianCalendarSystem.CHRONON_OF_DAY != 0) {
            temp = 1;
        }
        dataOutput.writeInt((int) (chrononTimeInMs / GregorianCalendarSystem.CHRONON_OF_DAY) - temp);
    }

    private void parseBoolean(Object obj, BooleanObjectInspector foi, DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(foi.get(obj));
    }

    private void parseInt8(Object obj, ByteObjectInspector foi, DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(foi.get(obj));
    }

    private void parseString(Object obj, StringObjectInspector foi, DataOutput dataOutput) throws IOException {
        utf8Writer.writeUTF8(foi.getPrimitiveJavaObject(obj), dataOutput);
    }

    private void parseTime(Object obj, TimestampObjectInspector foi, DataOutput dataOutput) throws IOException {
        dataOutput.writeInt((int) (foi.getPrimitiveJavaObject(obj).getTime() % 86400000));
    }

    private void parseOrderedList(AOrderedListType aOrderedListType, Object obj, ListObjectInspector foi)
            throws IOException {
        OrderedListBuilder orderedListBuilder = getOrderedListBuilder();
        IAType itemType = null;
        if (aOrderedListType != null)
            itemType = aOrderedListType.getItemType();
        orderedListBuilder.reset(aOrderedListType);

        int n = foi.getListLength(obj);
        for (int i = 0; i < n; i++) {
            Object element = foi.getListElement(obj, i);
            ObjectInspector eoi = foi.getListElementObjectInspector();
            if (element == null) {
                throw new HyracksDataException("can't parse hive list with null values");
            }
            parseItem(itemType, element, eoi, listItemBuffer.getDataOutput(), true);
            orderedListBuilder.addItem(listItemBuffer);
        }
        orderedListBuilder.write(fieldValueBuffer.getDataOutput(), true);
    }

    private void parseUnorderedList(AUnorderedListType uoltype, Object obj, ListObjectInspector oi) throws IOException {
        UnorderedListBuilder unorderedListBuilder = getUnorderedListBuilder();
        IAType itemType = null;
        if (uoltype != null)
            itemType = uoltype.getItemType();
        byte tagByte = itemType.getTypeTag().serialize();
        unorderedListBuilder.reset(uoltype);

        int n = oi.getListLength(obj);
        for (int i = 0; i < n; i++) {
            Object element = oi.getListElement(obj, i);
            ObjectInspector eoi = oi.getListElementObjectInspector();
            if (element == null) {
                throw new HyracksDataException("can't parse hive list with null values");
            }
            listItemBuffer.reset();
            final DataOutput dataOutput = listItemBuffer.getDataOutput();
            dataOutput.writeByte(tagByte);
            parseItem(itemType, element, eoi, dataOutput, true);
            unorderedListBuilder.addItem(listItemBuffer);
        }
        unorderedListBuilder.write(fieldValueBuffer.getDataOutput(), true);
    }

    private OrderedListBuilder getOrderedListBuilder() {
        if (orderedListBuilder != null)
            return orderedListBuilder;
        else {
            orderedListBuilder = new OrderedListBuilder();
            return orderedListBuilder;
        }
    }

    private UnorderedListBuilder getUnorderedListBuilder() {
        if (unorderedListBuilder != null)
            return unorderedListBuilder;
        else {
            unorderedListBuilder = new UnorderedListBuilder();
            return unorderedListBuilder;
        }
    }

}
