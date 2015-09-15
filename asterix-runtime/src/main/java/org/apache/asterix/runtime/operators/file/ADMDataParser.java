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
package org.apache.asterix.runtime.operators.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.List;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ITypeConvertComputer;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.operators.file.adm.AdmLexer;
import org.apache.asterix.runtime.operators.file.adm.AdmLexerException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * Parser for ADM formatted data.
 */
public class ADMDataParser extends AbstractDataParser {

    protected AdmLexer admLexer;
    protected ARecordType recordType;
    protected boolean datasetRec;

    private int nullableFieldId = 0;
    private ArrayBackedValueStorage castBuffer = new ArrayBackedValueStorage();

    private IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool = new ListObjectPool<IARecordBuilder, ATypeTag>(
            new RecordBuilderFactory());
    private IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool = new ListObjectPool<IAsterixListBuilder, ATypeTag>(
            new ListBuilderFactory());
    private IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool = new ListObjectPool<IMutableValueStorage, ATypeTag>(
            new AbvsBuilderFactory());

    private String mismatchErrorMessage = "Mismatch Type, expecting a value of type ";
    private String mismatchErrorMessage2 = " got a value of type ";

    static class ParseException extends AsterixException {
        private static final long serialVersionUID = 1L;
        private String filename;
        private int line = -1;
        private int column = -1;

        public ParseException(String message) {
            super(message);
        }

        public ParseException(Throwable cause) {
            super(cause);
        }

        public ParseException(String message, Throwable cause) {
            super(message, cause);
        }

        public ParseException(Throwable cause, String filename, int line, int column) {
            super(cause);
            setLocation(filename, line, column);
        }

        public void setLocation(String filename, int line, int column) {
            this.filename = filename;
            this.line = line;
            this.column = column;
        }

        @Override
        public String getMessage() {
            StringBuilder msg = new StringBuilder("Parse error");
            if (filename != null) {
                msg.append(" in file " + filename);
            }
            if (line >= 0) {
                if (column >= 0) {
                    msg.append(" at (" + line + ", " + column + ")");
                } else {
                    msg.append(" in line " + line);
                }
            }
            return msg.append(": " + super.getMessage()).toString();
        }
    }

    public ADMDataParser() {
        this(null);
    }

    public ADMDataParser(String filename) {
        this.filename = filename;
    }

    @Override
    public boolean parse(DataOutput out) throws AsterixException {
        try {
            resetPools();
            return parseAdmInstance(recordType, datasetRec, out);
        } catch (IOException e) {
            throw new ParseException(e, filename, admLexer.getLine(), admLexer.getColumn());
        } catch (AdmLexerException e) {
            throw new AsterixException(e);
        } catch (ParseException e) {
            e.setLocation(filename, admLexer.getLine(), admLexer.getColumn());
            throw e;
        }
    }

    @Override
    public void initialize(InputStream in, ARecordType recordType, boolean datasetRec) throws AsterixException {
        this.recordType = recordType;
        this.datasetRec = datasetRec;
        try {
            admLexer = new AdmLexer(new java.io.InputStreamReader(in));
        } catch (IOException e) {
            throw new ParseException(e);
        }
    }

    protected boolean parseAdmInstance(IAType objectType, boolean datasetRec, DataOutput out) throws AsterixException,
            IOException, AdmLexerException {
        int token = admLexer.next();
        if (token == AdmLexer.TOKEN_EOF) {
            return false;
        } else {
            admFromLexerStream(token, objectType, out, datasetRec);
            return true;
        }
    }

    private void admFromLexerStream(int token, IAType objectType, DataOutput out, Boolean datasetRec)
            throws AsterixException, IOException, AdmLexerException {

        switch (token) {
            case AdmLexer.TOKEN_NULL_LITERAL: {
                if (checkType(ATypeTag.NULL, objectType)) {
                    nullSerde.serialize(ANull.NULL, out);
                } else {
                    throw new ParseException("This field can not be null");
                }
                break;
            }
            case AdmLexer.TOKEN_TRUE_LITERAL: {
                if (checkType(ATypeTag.BOOLEAN, objectType)) {
                    booleanSerde.serialize(ABoolean.TRUE, out);
                } else {
                    throw new ParseException(mismatchErrorMessage + objectType.getTypeName());
                }
                break;
            }
            case AdmLexer.TOKEN_BOOLEAN_CONS: {
                parseConstructor(ATypeTag.BOOLEAN, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_FALSE_LITERAL: {
                if (checkType(ATypeTag.BOOLEAN, objectType)) {
                    booleanSerde.serialize(ABoolean.FALSE, out);
                } else {
                    throw new ParseException(mismatchErrorMessage + objectType.getTypeName());
                }
                break;
            }
            case AdmLexer.TOKEN_DOUBLE_LITERAL: {
                parseToNumericTarget(ATypeTag.DOUBLE, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_DOUBLE_CONS: {
                parseConstructor(ATypeTag.DOUBLE, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_FLOAT_LITERAL: {
                parseToNumericTarget(ATypeTag.FLOAT, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_FLOAT_CONS: {
                parseConstructor(ATypeTag.FLOAT, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT8_LITERAL: {
                parseAndCastNumeric(ATypeTag.INT8, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT8_CONS: {
                parseConstructor(ATypeTag.INT8, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT16_LITERAL: {
                parseAndCastNumeric(ATypeTag.INT16, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT16_CONS: {
                parseConstructor(ATypeTag.INT16, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT_LITERAL: {
                // For an INT value without any suffix, we return it as INT64 type value since it is the default integer type.
                parseAndCastNumeric(ATypeTag.INT64, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT32_LITERAL: {
                parseAndCastNumeric(ATypeTag.INT32, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT32_CONS: {
                parseConstructor(ATypeTag.INT32, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT64_LITERAL: {
                parseAndCastNumeric(ATypeTag.INT64, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT64_CONS: {
                parseConstructor(ATypeTag.INT64, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_STRING_LITERAL: {
                if (checkType(ATypeTag.STRING, objectType)) {
                    final String tokenImage = admLexer.getLastTokenImage().substring(1,
                            admLexer.getLastTokenImage().length() - 1);
                    aString.setValue(admLexer.containsEscapes() ? replaceEscapes(tokenImage) : tokenImage);
                    stringSerde.serialize(aString, out);
                } else if (checkType(ATypeTag.UUID, objectType)) {
                    // Dealing with UUID type that is represented by a string
                    final String tokenImage = admLexer.getLastTokenImage().substring(1,
                            admLexer.getLastTokenImage().length() - 1);
                    aUUID.fromStringToAMuatbleUUID(tokenImage);
                    uuidSerde.serialize(aUUID, out);
                } else {
                    throw new ParseException(mismatchErrorMessage + objectType.getTypeName());
                }
                break;
            }
            case AdmLexer.TOKEN_STRING_CONS: {
                parseConstructor(ATypeTag.STRING, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_HEX_CONS:
            case AdmLexer.TOKEN_BASE64_CONS: {
                if (checkType(ATypeTag.BINARY, objectType)) {
                    if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_OPEN) {
                        if (admLexer.next() == AdmLexer.TOKEN_STRING_LITERAL) {
                            parseToBinaryTarget(token, admLexer.getLastTokenImage(), out);
                            if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_CLOSE) {
                                break;
                            }
                        }
                    }
                }
                throw new ParseException(mismatchErrorMessage + objectType.getTypeName());
            }
            case AdmLexer.TOKEN_DATE_CONS: {
                parseConstructor(ATypeTag.DATE, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_TIME_CONS: {
                parseConstructor(ATypeTag.TIME, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_DATETIME_CONS: {
                parseConstructor(ATypeTag.DATETIME, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INTERVAL_DATE_CONS: {
                if (checkType(ATypeTag.INTERVAL, objectType)) {
                    if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_OPEN) {
                        if (admLexer.next() == AdmLexer.TOKEN_STRING_LITERAL) {
                            AIntervalSerializerDeserializer.parseDate(admLexer.getLastTokenImage(), out);

                            if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_CLOSE) {
                                break;
                            }
                        }
                    }
                }
                throw new ParseException("Wrong interval data parsing for date interval.");
            }
            case AdmLexer.TOKEN_INTERVAL_TIME_CONS: {
                if (checkType(ATypeTag.INTERVAL, objectType)) {
                    if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_OPEN) {
                        if (admLexer.next() == AdmLexer.TOKEN_STRING_LITERAL) {
                            AIntervalSerializerDeserializer.parseTime(admLexer.getLastTokenImage(), out);

                            if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_CLOSE) {
                                break;
                            }
                        }
                    }
                }
                throw new ParseException("Wrong interval data parsing for time interval.");
            }
            case AdmLexer.TOKEN_INTERVAL_DATETIME_CONS: {
                if (checkType(ATypeTag.INTERVAL, objectType)) {
                    if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_OPEN) {
                        if (admLexer.next() == AdmLexer.TOKEN_STRING_LITERAL) {
                            AIntervalSerializerDeserializer.parseDatetime(admLexer.getLastTokenImage(), out);

                            if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_CLOSE) {
                                break;
                            }
                        }
                    }
                }
                throw new ParseException("Wrong interval data parsing for datetime interval.");
            }
            case AdmLexer.TOKEN_DURATION_CONS: {
                parseConstructor(ATypeTag.DURATION, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_YEAR_MONTH_DURATION_CONS: {
                parseConstructor(ATypeTag.YEARMONTHDURATION, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_DAY_TIME_DURATION_CONS: {
                parseConstructor(ATypeTag.DAYTIMEDURATION, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_POINT_CONS: {
                parseConstructor(ATypeTag.POINT, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_POINT3D_CONS: {
                parseConstructor(ATypeTag.POINT3D, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_CIRCLE_CONS: {
                parseConstructor(ATypeTag.CIRCLE, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_RECTANGLE_CONS: {
                parseConstructor(ATypeTag.RECTANGLE, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_LINE_CONS: {
                parseConstructor(ATypeTag.LINE, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_POLYGON_CONS: {
                parseConstructor(ATypeTag.POLYGON, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_START_UNORDERED_LIST: {
                if (checkType(ATypeTag.UNORDEREDLIST, objectType)) {
                    objectType = getComplexType(objectType, ATypeTag.UNORDEREDLIST);
                    parseUnorderedList((AUnorderedListType) objectType, out);
                } else {
                    throw new ParseException(mismatchErrorMessage + objectType.getTypeTag());
                }
                break;
            }

            case AdmLexer.TOKEN_START_ORDERED_LIST: {
                if (checkType(ATypeTag.ORDEREDLIST, objectType)) {
                    objectType = getComplexType(objectType, ATypeTag.ORDEREDLIST);
                    parseOrderedList((AOrderedListType) objectType, out);
                } else {
                    throw new ParseException(mismatchErrorMessage + objectType.getTypeTag());
                }
                break;
            }
            case AdmLexer.TOKEN_START_RECORD: {
                if (checkType(ATypeTag.RECORD, objectType)) {
                    objectType = getComplexType(objectType, ATypeTag.RECORD);
                    parseRecord((ARecordType) objectType, out, datasetRec);
                } else {
                    throw new ParseException(mismatchErrorMessage + objectType.getTypeTag());
                }
                break;
            }
            case AdmLexer.TOKEN_UUID_CONS: {
                parseConstructor(ATypeTag.UUID, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_EOF: {
                break;
            }
            default: {
                throw new ParseException("Unexpected ADM token kind: " + AdmLexer.tokenKindToString(token) + ".");
            }
        }

    }

    private String replaceEscapes(String tokenImage) throws ParseException {
        char[] chars = tokenImage.toCharArray();
        int len = chars.length;
        int readpos = 0;
        int writepos = 0;
        int movemarker = 0;
        while (readpos < len) {
            if (chars[readpos] == '\\') {
                moveChars(chars, movemarker, readpos, readpos - writepos);
                switch (chars[readpos + 1]) {
                    case '\\':
                    case '\"':
                    case '/':
                        chars[writepos] = chars[readpos + 1];
                        break;
                    case 'b':
                        chars[writepos] = '\b';
                        break;
                    case 'f':
                        chars[writepos] = '\f';
                        break;
                    case 'n':
                        chars[writepos] = '\n';
                        break;
                    case 'r':
                        chars[writepos] = '\r';
                        break;
                    case 't':
                        chars[writepos] = '\t';
                        break;
                    case 'u':
                        chars[writepos] = (char) Integer.parseInt(new String(chars, readpos + 2, 4), 16);
                        readpos += 4;
                        break;
                    default:
                        throw new ParseException("Illegal escape '\\" + chars[readpos + 1] + "'");
                }
                ++readpos;
                movemarker = readpos + 1;
            }
            ++writepos;
            ++readpos;
        }
        moveChars(chars, movemarker, len, readpos - writepos);
        return new String(chars, 0, len - (readpos - writepos));
    }

    private static void moveChars(final char[] chars, final int start, final int end, final int offset) {
        if (offset == 0) {
            return;
        }
        for (int i = start; i < end; ++i) {
            chars[i - offset] = chars[i];
        }
    }

    private IAType getComplexType(IAType aObjectType, ATypeTag tag) {
        if (aObjectType == null) {
            return null;
        }

        if (aObjectType.getTypeTag() == tag) {
            return aObjectType;
        }

        if (aObjectType.getTypeTag() == ATypeTag.UNION) {
            List<IAType> unionList = ((AUnionType) aObjectType).getUnionList();
            for (int i = 0; i < unionList.size(); i++) {
                if (unionList.get(i).getTypeTag() == tag) {
                    return unionList.get(i);
                }
            }
        }
        return null; // wont get here
    }

    private ATypeTag getTargetTypeTag(ATypeTag expectedTypeTag, IAType aObjectType) throws IOException {
        if (aObjectType == null) {
            return expectedTypeTag;
        }
        if (aObjectType.getTypeTag() != ATypeTag.UNION) {
            final ATypeTag typeTag = aObjectType.getTypeTag();
            if (ATypeHierarchy.canPromote(expectedTypeTag, typeTag)
                    || ATypeHierarchy.canDemote(expectedTypeTag, typeTag)) {
                return typeTag;
            } else {
                return null;
            }
            //            return ATypeHierarchy.canPromote(expectedTypeTag, typeTag) ? typeTag : null;
        } else { // union
            List<IAType> unionList = ((AUnionType) aObjectType).getUnionList();
            for (IAType t : unionList) {
                final ATypeTag typeTag = t.getTypeTag();
                if (ATypeHierarchy.canPromote(expectedTypeTag, typeTag)
                        || ATypeHierarchy.canDemote(expectedTypeTag, typeTag)) {
                    return typeTag;
                }
            }
        }
        return null;
    }

    private boolean checkType(ATypeTag expectedTypeTag, IAType aObjectType) throws IOException {
        return getTargetTypeTag(expectedTypeTag, aObjectType) != null;
    }

    private void parseRecord(ARecordType recType, DataOutput out, Boolean datasetRec) throws IOException,
            AsterixException, AdmLexerException {

        ArrayBackedValueStorage fieldValueBuffer = getTempBuffer();
        ArrayBackedValueStorage fieldNameBuffer = getTempBuffer();
        IARecordBuilder recBuilder = getRecordBuilder();

        BitSet nulls = null;
        if (datasetRec) {
            if (recType != null) {
                nulls = new BitSet(recType.getFieldNames().length);
                recBuilder.reset(recType);
            } else {
                recBuilder.reset(null);
            }
        } else if (recType != null) {
            nulls = new BitSet(recType.getFieldNames().length);
            recBuilder.reset(recType);
        } else {
            recBuilder.reset(null);
        }

        recBuilder.init();
        int token;
        boolean inRecord = true;
        boolean expectingRecordField = false;
        boolean first = true;

        Boolean openRecordField = false;
        int fieldId = 0;
        IAType fieldType = null;
        do {
            token = admLexer.next();
            switch (token) {
                case AdmLexer.TOKEN_END_RECORD: {
                    if (expectingRecordField) {
                        throw new ParseException("Found END_RECORD while expecting a record field.");
                    }
                    inRecord = false;
                    break;
                }
                case AdmLexer.TOKEN_STRING_LITERAL: {
                    // we've read the name of the field
                    // now read the content
                    fieldNameBuffer.reset();
                    fieldValueBuffer.reset();
                    expectingRecordField = false;

                    if (recType != null) {
                        String fldName = admLexer.getLastTokenImage().substring(1,
                                admLexer.getLastTokenImage().length() - 1);
                        fieldId = recBuilder.getFieldId(fldName);
                        if (fieldId < 0 && !recType.isOpen()) {
                            throw new ParseException("This record is closed, you can not add extra fields !!");
                        } else if (fieldId < 0 && recType.isOpen()) {
                            aStringFieldName.setValue(admLexer.getLastTokenImage().substring(1,
                                    admLexer.getLastTokenImage().length() - 1));
                            stringSerde.serialize(aStringFieldName, fieldNameBuffer.getDataOutput());
                            openRecordField = true;
                            fieldType = null;
                        } else {
                            // a closed field
                            nulls.set(fieldId);
                            fieldType = recType.getFieldTypes()[fieldId];
                            openRecordField = false;
                        }
                    } else {
                        aStringFieldName.setValue(admLexer.getLastTokenImage().substring(1,
                                admLexer.getLastTokenImage().length() - 1));
                        stringSerde.serialize(aStringFieldName, fieldNameBuffer.getDataOutput());
                        openRecordField = true;
                        fieldType = null;
                    }

                    token = admLexer.next();
                    if (token != AdmLexer.TOKEN_COLON) {
                        throw new ParseException("Unexpected ADM token kind: " + AdmLexer.tokenKindToString(token)
                                + " while expecting \":\".");
                    }

                    token = admLexer.next();
                    this.admFromLexerStream(token, fieldType, fieldValueBuffer.getDataOutput(), false);
                    if (openRecordField) {
                        if (fieldValueBuffer.getByteArray()[0] != ATypeTag.NULL.serialize()) {
                            recBuilder.addField(fieldNameBuffer, fieldValueBuffer);
                        }
                    } else if (NonTaggedFormatUtil.isOptional(recType)) {
                        if (fieldValueBuffer.getByteArray()[0] != ATypeTag.NULL.serialize()) {
                            recBuilder.addField(fieldId, fieldValueBuffer);
                        }
                    } else {
                        recBuilder.addField(fieldId, fieldValueBuffer);
                    }

                    break;
                }
                case AdmLexer.TOKEN_COMMA: {
                    if (first) {
                        throw new ParseException("Found COMMA before any record field.");
                    }
                    if (expectingRecordField) {
                        throw new ParseException("Found COMMA while expecting a record field.");
                    }
                    expectingRecordField = true;
                    break;
                }
                default: {
                    throw new ParseException("Unexpected ADM token kind: " + AdmLexer.tokenKindToString(token)
                            + " while parsing record fields.");
                }
            }
            first = false;
        } while (inRecord);

        if (recType != null) {
            nullableFieldId = checkNullConstraints(recType, nulls);
            if (nullableFieldId != -1) {
                throw new ParseException("Field: " + recType.getFieldNames()[nullableFieldId] + " can not be null");
            }
        }
        recBuilder.write(out, true);
    }

    private int checkNullConstraints(ARecordType recType, BitSet nulls) {
        boolean isNull = false;
        for (int i = 0; i < recType.getFieldTypes().length; i++) {
            if (nulls.get(i) == false) {
                IAType type = recType.getFieldTypes()[i];
                if (type.getTypeTag() != ATypeTag.NULL && type.getTypeTag() != ATypeTag.UNION) {
                    return i;
                }

                if (type.getTypeTag() == ATypeTag.UNION) { // union
                    List<IAType> unionList = ((AUnionType) type).getUnionList();
                    for (int j = 0; j < unionList.size(); j++) {
                        if (unionList.get(j).getTypeTag() == ATypeTag.NULL) {
                            isNull = true;
                            break;
                        }
                    }
                    if (!isNull) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    private void parseOrderedList(AOrderedListType oltype, DataOutput out) throws IOException, AsterixException,
            AdmLexerException {
        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        OrderedListBuilder orderedListBuilder = (OrderedListBuilder) getOrderedListBuilder();

        IAType itemType = null;
        if (oltype != null) {
            itemType = oltype.getItemType();
        }
        orderedListBuilder.reset(oltype);

        int token;
        boolean inList = true;
        boolean expectingListItem = false;
        boolean first = true;
        do {
            token = admLexer.next();
            if (token == AdmLexer.TOKEN_END_ORDERED_LIST) {
                if (expectingListItem) {
                    throw new ParseException("Found END_COLLECTION while expecting a list item.");
                }
                inList = false;
            } else if (token == AdmLexer.TOKEN_COMMA) {
                if (first) {
                    throw new ParseException("Found COMMA before any list item.");
                }
                if (expectingListItem) {
                    throw new ParseException("Found COMMA while expecting a list item.");
                }
                expectingListItem = true;
            } else {
                expectingListItem = false;
                itemBuffer.reset();

                admFromLexerStream(token, itemType, itemBuffer.getDataOutput(), false);
                orderedListBuilder.addItem(itemBuffer);
            }
            first = false;
        } while (inList);
        orderedListBuilder.write(out, true);
    }

    private void parseUnorderedList(AUnorderedListType uoltype, DataOutput out) throws IOException, AsterixException,
            AdmLexerException {
        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        UnorderedListBuilder unorderedListBuilder = (UnorderedListBuilder) getUnorderedListBuilder();

        IAType itemType = null;

        if (uoltype != null) {
            itemType = uoltype.getItemType();
        }
        unorderedListBuilder.reset(uoltype);

        int token;
        boolean inList = true;
        boolean expectingListItem = false;
        boolean first = true;
        do {
            token = admLexer.next();
            if (token == AdmLexer.TOKEN_END_RECORD) {
                if (admLexer.next() == AdmLexer.TOKEN_END_RECORD) {
                    if (expectingListItem) {
                        throw new ParseException("Found END_COLLECTION while expecting a list item.");
                    } else {
                        inList = false;
                    }
                } else {
                    throw new ParseException("Found END_RECORD while expecting a list item.");
                }
            } else if (token == AdmLexer.TOKEN_COMMA) {
                if (first) {
                    throw new ParseException("Found COMMA before any list item.");
                }
                if (expectingListItem) {
                    throw new ParseException("Found COMMA while expecting a list item.");
                }
                expectingListItem = true;
            } else {
                expectingListItem = false;
                itemBuffer.reset();
                admFromLexerStream(token, itemType, itemBuffer.getDataOutput(), false);
                unorderedListBuilder.addItem(itemBuffer);
            }
            first = false;
        } while (inList);
        unorderedListBuilder.write(out, true);
    }

    private IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.RECORD);
    }

    private IAsterixListBuilder getOrderedListBuilder() {
        return listBuilderPool.allocate(ATypeTag.ORDEREDLIST);
    }

    private IAsterixListBuilder getUnorderedListBuilder() {
        return listBuilderPool.allocate(ATypeTag.UNORDEREDLIST);
    }

    private ArrayBackedValueStorage getTempBuffer() {
        return (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.BINARY);
    }

    private void parseToBinaryTarget(int lexerToken, String tokenImage, DataOutput out) throws ParseException,
            HyracksDataException {
        switch (lexerToken) {
            case AdmLexer.TOKEN_HEX_CONS: {
                parseHexBinaryString(tokenImage.toCharArray(), 1, tokenImage.length() - 2, out);
                break;
            }
            case AdmLexer.TOKEN_BASE64_CONS: {
                parseBase64BinaryString(tokenImage.toCharArray(), 1, tokenImage.length() - 2, out);
                break;
            }
        }
    }

    private void parseToNumericTarget(ATypeTag typeTag, IAType objectType, DataOutput out) throws AsterixException,
            IOException {
        final ATypeTag targetTypeTag = getTargetTypeTag(typeTag, objectType);
        if (targetTypeTag == null || !parseValue(admLexer.getLastTokenImage(), targetTypeTag, out)) {
            throw new ParseException(mismatchErrorMessage + objectType.getTypeName() + mismatchErrorMessage2 + typeTag);
        }
    }

    private void parseAndCastNumeric(ATypeTag typeTag, IAType objectType, DataOutput out) throws AsterixException,
            IOException {
        final ATypeTag targetTypeTag = getTargetTypeTag(typeTag, objectType);
        DataOutput dataOutput = out;
        if (targetTypeTag != typeTag) {
            castBuffer.reset();
            dataOutput = castBuffer.getDataOutput();
        }

        if (targetTypeTag == null || !parseValue(admLexer.getLastTokenImage(), typeTag, dataOutput)) {
            throw new ParseException(mismatchErrorMessage + objectType.getTypeName() + mismatchErrorMessage2 + typeTag);
        }

        // If two type tags are not the same, either we try to promote or demote source type to the target type
        if (targetTypeTag != typeTag) {
            if (ATypeHierarchy.canPromote(typeTag, targetTypeTag)) {
                // can promote typeTag to targetTypeTag
                ITypeConvertComputer promoteComputer = ATypeHierarchy.getTypePromoteComputer(typeTag, targetTypeTag);
                if (promoteComputer == null) {
                    throw new AsterixException("Can't cast the " + typeTag + " type to the " + targetTypeTag + " type.");
                }
                // do the promotion; note that the type tag field should be skipped
                promoteComputer.convertType(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                        castBuffer.getLength() - 1, out);
            } else if (ATypeHierarchy.canDemote(typeTag, targetTypeTag)) {
                //can demote source type to the target type
                ITypeConvertComputer demoteComputer = ATypeHierarchy.getTypeDemoteComputer(typeTag, targetTypeTag);
                if (demoteComputer == null) {
                    throw new AsterixException("Can't cast the " + typeTag + " type to the " + targetTypeTag + " type.");
                }
                // do the demotion; note that the type tag field should be skipped
                demoteComputer.convertType(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                        castBuffer.getLength() - 1, out);
            }
        }
    }

    private void parseConstructor(ATypeTag typeTag, IAType objectType, DataOutput out) throws AsterixException,
            AdmLexerException, IOException {
        final ATypeTag targetTypeTag = getTargetTypeTag(typeTag, objectType);
        if (targetTypeTag != null) {
            DataOutput dataOutput = out;
            if (targetTypeTag != typeTag) {
                castBuffer.reset();
                dataOutput = castBuffer.getDataOutput();
            }
            int token = admLexer.next();
            if (token == AdmLexer.TOKEN_CONSTRUCTOR_OPEN) {
                token = admLexer.next();
                if (token == AdmLexer.TOKEN_STRING_LITERAL) {
                    final String unquoted = admLexer.getLastTokenImage().substring(1,
                            admLexer.getLastTokenImage().length() - 1);
                    if (!parseValue(unquoted, typeTag, dataOutput)) {
                        throw new ParseException("Missing deserializer method for constructor: "
                                + AdmLexer.tokenKindToString(token) + ".");
                    }
                    token = admLexer.next();
                    if (token == AdmLexer.TOKEN_CONSTRUCTOR_CLOSE) {
                        if (targetTypeTag != typeTag) {
                            ITypeConvertComputer promoteComputer = ATypeHierarchy.getTypePromoteComputer(typeTag,
                                    targetTypeTag);
                            // the availability if the promote computer should be consistent with the availability of a target type
                            assert promoteComputer != null;
                            // do the promotion; note that the type tag field should be skipped
                            promoteComputer.convertType(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                                    castBuffer.getLength() - 1, out);
                        }
                        return;
                    }
                }
            }
        }
        throw new ParseException(mismatchErrorMessage + objectType.getTypeName() + ". Got " + typeTag + " instead.");
    }

    private boolean parseValue(final String unquoted, ATypeTag typeTag, DataOutput out) throws AsterixException,
            HyracksDataException, IOException {
        switch (typeTag) {
            case BOOLEAN:
                parseBoolean(unquoted, out);
                return true;
            case INT8:
                parseInt8(unquoted, out);
                return true;
            case INT16:
                parseInt16(unquoted, out);
                return true;
            case INT32:
                parseInt32(unquoted, out);
                return true;
            case INT64:
                parseInt64(unquoted, out);
                return true;
            case FLOAT:
                aFloat.setValue(Float.parseFloat(unquoted));
                floatSerde.serialize(aFloat, out);
                return true;
            case DOUBLE:
                aDouble.setValue(Double.parseDouble(unquoted));
                doubleSerde.serialize(aDouble, out);
                return true;
            case STRING:
                aString.setValue(unquoted);
                stringSerde.serialize(aString, out);
                return true;
            case TIME:
                parseTime(unquoted, out);
                return true;
            case DATE:
                parseDate(unquoted, out);
                return true;
            case DATETIME:
                parseDateTime(unquoted, out);
                return true;
            case DURATION:
                parseDuration(unquoted, out);
                return true;
            case DAYTIMEDURATION:
                parseDateTimeDuration(unquoted, out);
                return true;
            case YEARMONTHDURATION:
                parseYearMonthDuration(unquoted, out);
                return true;
            case POINT:
                parsePoint(unquoted, out);
                return true;
            case POINT3D:
                parse3DPoint(unquoted, out);
                return true;
            case CIRCLE:
                parseCircle(unquoted, out);
                return true;
            case RECTANGLE:
                parseRectangle(unquoted, out);
                return true;
            case LINE:
                parseLine(unquoted, out);
                return true;
            case POLYGON:
                APolygonSerializerDeserializer.parse(unquoted, out);
                return true;
            case UUID:
                aUUID.fromStringToAMuatbleUUID(unquoted);
                uuidSerde.serialize(aUUID, out);
                return true;
            default:
                return false;
        }
    }

    private void parseBoolean(String bool, DataOutput out) throws AsterixException, HyracksDataException {
        String errorMessage = "This can not be an instance of boolean";
        if (bool.equals("true")) {
            booleanSerde.serialize(ABoolean.TRUE, out);
        } else if (bool.equals("false")) {
            booleanSerde.serialize(ABoolean.FALSE, out);
        } else {
            throw new ParseException(errorMessage);
        }
    }

    private void parseInt8(String int8, DataOutput out) throws AsterixException, HyracksDataException {
        String errorMessage = "This can not be an instance of int8";
        boolean positive = true;
        byte value = 0;
        int offset = 0;

        if (int8.charAt(offset) == '+') {
            offset++;
        } else if (int8.charAt(offset) == '-') {
            offset++;
            positive = false;
        }
        for (; offset < int8.length(); offset++) {
            if (int8.charAt(offset) >= '0' && int8.charAt(offset) <= '9') {
                value = (byte) (value * 10 + int8.charAt(offset) - '0');
            } else if (int8.charAt(offset) == 'i' && int8.charAt(offset + 1) == '8' && offset + 2 == int8.length()) {
                break;
            } else {
                throw new ParseException(errorMessage);
            }
        }
        if (value < 0) {
            throw new ParseException(errorMessage);
        }
        if (value > 0 && !positive) {
            value *= -1;
        }
        aInt8.setValue(value);
        int8Serde.serialize(aInt8, out);
    }

    private void parseInt16(String int16, DataOutput out) throws AsterixException, HyracksDataException {
        String errorMessage = "This can not be an instance of int16";
        boolean positive = true;
        short value = 0;
        int offset = 0;

        if (int16.charAt(offset) == '+') {
            offset++;
        } else if (int16.charAt(offset) == '-') {
            offset++;
            positive = false;
        }
        for (; offset < int16.length(); offset++) {
            if (int16.charAt(offset) >= '0' && int16.charAt(offset) <= '9') {
                value = (short) (value * 10 + int16.charAt(offset) - '0');
            } else if (int16.charAt(offset) == 'i' && int16.charAt(offset + 1) == '1'
                    && int16.charAt(offset + 2) == '6' && offset + 3 == int16.length()) {
                break;
            } else {
                throw new ParseException(errorMessage);
            }
        }
        if (value < 0) {
            throw new ParseException(errorMessage);
        }
        if (value > 0 && !positive) {
            value *= -1;
        }
        aInt16.setValue(value);
        int16Serde.serialize(aInt16, out);
    }

    private void parseInt32(String int32, DataOutput out) throws AsterixException, HyracksDataException {
        String errorMessage = "This can not be an instance of int32";
        boolean positive = true;
        int value = 0;
        int offset = 0;

        if (int32.charAt(offset) == '+') {
            offset++;
        } else if (int32.charAt(offset) == '-') {
            offset++;
            positive = false;
        }
        for (; offset < int32.length(); offset++) {
            if (int32.charAt(offset) >= '0' && int32.charAt(offset) <= '9') {
                value = (value * 10 + int32.charAt(offset) - '0');
            } else if (int32.charAt(offset) == 'i' && int32.charAt(offset + 1) == '3'
                    && int32.charAt(offset + 2) == '2' && offset + 3 == int32.length()) {
                break;
            } else {
                throw new ParseException(errorMessage);
            }
        }
        if (value < 0) {
            throw new ParseException(errorMessage);
        }
        if (value > 0 && !positive) {
            value *= -1;
        }

        aInt32.setValue(value);
        int32Serde.serialize(aInt32, out);
    }

    private void parseInt64(String int64, DataOutput out) throws AsterixException, HyracksDataException {
        String errorMessage = "This can not be an instance of int64";
        boolean positive = true;
        long value = 0;
        int offset = 0;

        if (int64.charAt(offset) == '+') {
            offset++;
        } else if (int64.charAt(offset) == '-') {
            offset++;
            positive = false;
        }
        for (; offset < int64.length(); offset++) {
            if (int64.charAt(offset) >= '0' && int64.charAt(offset) <= '9') {
                value = (value * 10 + int64.charAt(offset) - '0');
            } else if (int64.charAt(offset) == 'i' && int64.charAt(offset + 1) == '6'
                    && int64.charAt(offset + 2) == '4' && offset + 3 == int64.length()) {
                break;
            } else {
                throw new ParseException(errorMessage);
            }
        }
        if (value < 0) {
            throw new ParseException(errorMessage);
        }
        if (value > 0 && !positive) {
            value *= -1;
        }

        aInt64.setValue(value);
        int64Serde.serialize(aInt64, out);
    }

    /**
     * Resets the pools before parsing a top-level record.
     * In this way the elements in those pools can be re-used.
     */
    private void resetPools() {
        listBuilderPool.reset();
        recordBuilderPool.reset();
        abvsBuilderPool.reset();
    }
}
