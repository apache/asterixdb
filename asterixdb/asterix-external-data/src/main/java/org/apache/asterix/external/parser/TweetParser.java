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

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringWriter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataOutput;
import java.io.IOException;

public class TweetParser extends AbstractDataParser implements IRecordDataParser<String> {
    private final IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool = new ListObjectPool<>(
            new RecordBuilderFactory());
    private final IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool = new ListObjectPool<>(
            new ListBuilderFactory());
    private final IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool = new ListObjectPool<>(
            new AbvsBuilderFactory());
    private ARecordType recordType;
    private UTF8StringWriter utf8Writer = new UTF8StringWriter();

    public TweetParser(ARecordType recordType) {
        this.recordType = recordType;
        aPoint = new AMutablePoint(0, 0);
    }

    private void parseUnorderedList(JSONArray jArray, DataOutput output) throws IOException, JSONException {
        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        UnorderedListBuilder unorderedListBuilder = (UnorderedListBuilder) getUnorderedListBuilder();

        unorderedListBuilder.reset(null);
        for (int iter1 = 0; iter1 < jArray.length(); iter1++) {
            itemBuffer.reset();
            if (writeField(jArray.get(iter1), null, itemBuffer.getDataOutput())) {
                unorderedListBuilder.addItem(itemBuffer);
            }
        }
        unorderedListBuilder.write(output, true);
    }

    private boolean writeField(Object fieldObj, IAType fieldType, DataOutput out) throws IOException, JSONException {
        boolean writeResult = true;
        if (fieldType != null) {
            switch (fieldType.getTypeTag()) {
                case STRING:
                    out.write(BuiltinType.ASTRING.getTypeTag().serialize());
                    utf8Writer.writeUTF8(fieldObj.toString(), out);
                    break;
                case INT64:
                    aInt64.setValue((long) fieldObj);
                    int64Serde.serialize(aInt64, out);
                    break;
                case INT32:
                    out.write(BuiltinType.AINT32.getTypeTag().serialize());
                    out.writeInt((Integer) fieldObj);
                    break;
                case DOUBLE:
                    out.write(BuiltinType.ADOUBLE.getTypeTag().serialize());
                    out.writeDouble((Double) fieldObj);
                    break;
                case BOOLEAN:
                    out.write(BuiltinType.ABOOLEAN.getTypeTag().serialize());
                    out.writeBoolean((Boolean) fieldObj);
                    break;
                case RECORD:
                    writeRecord((JSONObject) fieldObj, out, (ARecordType) fieldType);
                    break;
                default:
                    writeResult = false;
            }
        } else {
            if (fieldObj == JSONObject.NULL) {
                nullSerde.serialize(ANull.NULL, out);
            } else if (fieldObj instanceof Integer) {
                out.write(BuiltinType.AINT32.getTypeTag().serialize());
                out.writeInt((Integer) fieldObj);
            } else if (fieldObj instanceof Boolean) {
                out.write(BuiltinType.ABOOLEAN.getTypeTag().serialize());
                out.writeBoolean((Boolean) fieldObj);
            } else if (fieldObj instanceof Double) {
                out.write(BuiltinType.ADOUBLE.getTypeTag().serialize());
                out.writeDouble((Double) fieldObj);
            } else if (fieldObj instanceof Long) {
                out.write(BuiltinType.AINT64.getTypeTag().serialize());
                out.writeLong((Long) fieldObj);
            } else if (fieldObj instanceof String) {
                out.write(BuiltinType.ASTRING.getTypeTag().serialize());
                utf8Writer.writeUTF8((String) fieldObj, out);
            } else if (fieldObj instanceof JSONArray) {
                if (((JSONArray) fieldObj).length() != 0) {
                    parseUnorderedList((JSONArray) fieldObj, out);
                } else {
                    writeResult = false;
                }
            } else if (fieldObj instanceof JSONObject) {
                if (((JSONObject) fieldObj).length() != 0) {
                    writeRecord((JSONObject) fieldObj, out, null);
                } else {
                    writeResult = false;
                }
            }
        }
        return writeResult;
    }

    private int checkAttrNameIdx(String[] nameList, String name) {
        int idx = 0;
        if (nameList != null) {
            for (String nln : nameList) {
                if (name.equals(nln)) {
                    return idx;
                }
                idx++;
            }
        }
        return -1;
    }

    public void writeRecord(JSONObject obj, DataOutput out, ARecordType curRecType) throws IOException, JSONException {
        IAType[] curTypes = null;
        String[] curFNames = null;
        int fieldN;
        int attrIdx;

        ArrayBackedValueStorage fieldValueBuffer = getTempBuffer();
        ArrayBackedValueStorage fieldNameBuffer = getTempBuffer();
        IARecordBuilder recBuilder = getRecordBuilder();

        if (curRecType != null) {
            curTypes = curRecType.getFieldTypes();
            curFNames = curRecType.getFieldNames();
        }

        recBuilder.reset(curRecType);
        recBuilder.init();

        if (curRecType != null && !curRecType.isOpen()) {
            // closed record type
            fieldN = curFNames.length;
            for (int iter1 = 0; iter1 < fieldN; iter1++) {
                fieldValueBuffer.reset();
                DataOutput fieldOutput = fieldValueBuffer.getDataOutput();
                if (obj.isNull(curFNames[iter1])) {
                    if (curRecType.isClosedField(curFNames[iter1])) {
                        throw new HyracksDataException("Closed field " + curFNames[iter1] + " has null value.");
                    } else {
                        continue;
                    }
                } else {
                    if (writeField(obj.get(curFNames[iter1]), curTypes[iter1], fieldOutput)) {
                        recBuilder.addField(iter1, fieldValueBuffer);
                    }
                }
            }
        } else {
            //open record type
            int closedFieldCount = 0;
            IAType curFieldType = null;
            for (String attrName : JSONObject.getNames(obj)) {
                if (obj.isNull(attrName) || obj.length() == 0) {
                    continue;
                }
                attrIdx = checkAttrNameIdx(curFNames, attrName);
                if (curRecType != null) {
                    curFieldType = curRecType.getFieldType(attrName);
                }
                fieldValueBuffer.reset();
                fieldNameBuffer.reset();
                DataOutput fieldOutput = fieldValueBuffer.getDataOutput();
                if (writeField(obj.get(attrName), curFieldType, fieldOutput)) {
                    if (attrIdx == -1) {
                        aString.setValue(attrName);
                        stringSerde.serialize(aString, fieldNameBuffer.getDataOutput());
                        recBuilder.addField(fieldNameBuffer, fieldValueBuffer);
                    } else {
                        recBuilder.addField(attrIdx, fieldValueBuffer);
                        closedFieldCount++;
                    }
                }
            }
            if (curRecType != null && closedFieldCount < curFNames.length) {
                throw new HyracksDataException("Non-null field is null");
            }
        }
        recBuilder.write(out, true);
    }

    private IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.RECORD);
    }

    private IAsterixListBuilder getUnorderedListBuilder() {
        return listBuilderPool.allocate(ATypeTag.UNORDEREDLIST);
    }

    private ArrayBackedValueStorage getTempBuffer() {
        return (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.BINARY);
    }

    @Override
    public void parse(IRawRecord<? extends String> record, DataOutput out) throws HyracksDataException {
        try {
            //TODO get rid of this temporary json
            resetPools();
            JSONObject jsObj = new JSONObject(record.get());
            writeRecord(jsObj, out, recordType);
        } catch (JSONException | IOException e) {
            throw new HyracksDataException(e);
        }
    }

    private void resetPools() {
        listBuilderPool.reset();
        recordBuilderPool.reset();
        abvsBuilderPool.reset();
    }
}
