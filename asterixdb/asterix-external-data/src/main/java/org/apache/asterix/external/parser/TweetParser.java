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
import java.util.Iterator;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class was introduced to parse Twitter data. As Tweets are JSON formatted records, we can use the JSON parser to
 * to parse them instead of having this dedicated parser. In the future, we could either deprecate this class, or add
 * some Tweet specific parsing processes into this class.
 */

public class TweetParser extends AbstractDataParser implements IRecordDataParser<char[]> {
    private final IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool =
            new ListObjectPool<>(new RecordBuilderFactory());
    private final IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool =
            new ListObjectPool<>(new ListBuilderFactory());
    private final IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool =
            new ListObjectPool<>(new AbvsBuilderFactory());
    private ARecordType recordType;
    private UTF8StringWriter utf8Writer = new UTF8StringWriter();

    public TweetParser(ARecordType recordType) {
        this.recordType = recordType;
    }

    private void parseArrayList(JsonNode jArray, DataOutput output) throws IOException {
        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        OrderedListBuilder arrayBuilder = (OrderedListBuilder) getArrayBuilder();

        arrayBuilder.reset(null);
        for (int iter1 = 0; iter1 < jArray.size(); iter1++) {
            itemBuffer.reset();
            if (writeField(jArray.get(iter1), null, itemBuffer.getDataOutput())) {
                arrayBuilder.addItem(itemBuffer);
            }
        }
        arrayBuilder.write(output, true);
    }

    private boolean writeField(JsonNode fieldObj, IAType originalFieldType, DataOutput out) throws IOException {
        boolean writeResult = true;
        IAType fieldType = originalFieldType;
        if (originalFieldType instanceof AUnionType) {
            fieldType = ((AUnionType) originalFieldType).getActualType();
        }
        if (fieldType != null) {
            switch (fieldType.getTypeTag()) {
                case STRING:
                    out.write(BuiltinType.ASTRING.getTypeTag().serialize());
                    utf8Writer.writeUTF8(fieldObj.asText(), out);
                    break;
                case BIGINT:
                    aInt64.setValue(fieldObj.asLong());
                    int64Serde.serialize(aInt64, out);
                    break;
                case INTEGER:
                    out.write(BuiltinType.AINT32.getTypeTag().serialize());
                    out.writeInt(fieldObj.asInt());
                    break;
                case DOUBLE:
                    out.write(BuiltinType.ADOUBLE.getTypeTag().serialize());
                    out.writeDouble(fieldObj.asDouble());
                    break;
                case BOOLEAN:
                    out.write(BuiltinType.ABOOLEAN.getTypeTag().serialize());
                    out.writeBoolean(fieldObj.asBoolean());
                    break;
                case OBJECT:
                    writeRecord(fieldObj, out, (ARecordType) fieldType);
                    break;
                case ARRAY:
                    parseArrayList(fieldObj, out);
                    break;
                default:
                    writeResult = false;
            }
        } else {
            if (fieldObj.isNull()) {
                nullSerde.serialize(ANull.NULL, out);
            } else if (fieldObj.isInt()) {
                out.write(BuiltinType.AINT32.getTypeTag().serialize());
                out.writeInt(fieldObj.asInt());
            } else if (fieldObj.isBoolean()) {
                out.write(BuiltinType.ABOOLEAN.getTypeTag().serialize());
                out.writeBoolean(fieldObj.asBoolean());
            } else if (fieldObj.isDouble()) {
                out.write(BuiltinType.ADOUBLE.getTypeTag().serialize());
                out.writeDouble(fieldObj.asDouble());
            } else if (fieldObj.isLong()) {
                out.write(BuiltinType.AINT64.getTypeTag().serialize());
                out.writeLong(fieldObj.asLong());
            } else if (fieldObj.isTextual()) {
                out.write(BuiltinType.ASTRING.getTypeTag().serialize());
                utf8Writer.writeUTF8(fieldObj.asText(), out);
            } else if (fieldObj.isArray()) {
                if ((fieldObj).size() != 0) {
                    parseArrayList(fieldObj, out);
                } else {
                    writeResult = false;
                }
            } else if (fieldObj.isObject()) {
                if ((fieldObj).size() != 0) {
                    writeRecord(fieldObj, out, null);
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

    public void writeRecord(JsonNode obj, DataOutput out, ARecordType curRecType) throws IOException {
        IAType[] curTypes = null;
        String[] curFNames = null;
        int fieldN;
        int attrIdx;
        int expectedFieldsCount = 0;

        ArrayBackedValueStorage fieldValueBuffer = getTempBuffer();
        ArrayBackedValueStorage fieldNameBuffer = getTempBuffer();
        IARecordBuilder recBuilder = getRecordBuilder();

        if (curRecType != null) {
            curTypes = curRecType.getFieldTypes();
            curFNames = curRecType.getFieldNames();
            for (IAType curType : curTypes) {
                if (!(curType instanceof AUnionType)) {
                    expectedFieldsCount++;
                }
            }
        }

        recBuilder.reset(curRecType);
        recBuilder.init();

        if (curRecType != null && !curRecType.isOpen()) {
            // closed record type
            fieldN = curFNames.length;
            for (int iter1 = 0; iter1 < fieldN; iter1++) {
                fieldValueBuffer.reset();
                DataOutput fieldOutput = fieldValueBuffer.getDataOutput();
                if (obj.get(curFNames[iter1]).isNull() && !(curTypes[iter1] instanceof AUnionType)) {
                    if (curRecType.isClosedField(curFNames[iter1])) {
                        throw new RuntimeDataException(ErrorCode.PARSER_TWEET_PARSER_CLOSED_FIELD_NULL,
                                curFNames[iter1]);
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
            String attrName;
            Iterator<String> iter = obj.fieldNames();
            while (iter.hasNext()) {
                attrName = iter.next();
                if (obj.get(attrName) == null || obj.get(attrName).isNull() || obj.size() == 0) {
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
            if (curRecType != null && closedFieldCount < expectedFieldsCount) {
                throw new HyracksDataException("Non-null field is null");
            }
        }
        recBuilder.write(out, true);
    }

    private IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.OBJECT);
    }

    private IAsterixListBuilder getArrayBuilder() {
        return listBuilderPool.allocate(ATypeTag.ARRAY);
    }

    private ArrayBackedValueStorage getTempBuffer() {
        return (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.BINARY);
    }

    @Override
    public void parse(IRawRecord<? extends char[]> record, DataOutput out) throws HyracksDataException {
        try {
            //TODO get rid of this temporary json
            resetPools();
            ObjectMapper om = new ObjectMapper();
            writeRecord(om.readTree(record.getBytes()), out, recordType);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void resetPools() {
        listBuilderPool.reset();
        recordBuilderPool.reset();
        abvsBuilderPool.reset();
    }
}
