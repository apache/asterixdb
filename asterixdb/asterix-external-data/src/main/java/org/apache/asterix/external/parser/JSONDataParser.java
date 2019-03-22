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
import java.io.InputStream;
import java.util.BitSet;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.parser.jackson.ADMToken;
import org.apache.asterix.external.parser.jackson.GeometryCoParser;
import org.apache.asterix.external.parser.jackson.ParserContext;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.asterix.runtime.exceptions.UnsupportedTypeException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;

/**
 * JSON format parser using Jakson parser.
 */
public class JSONDataParser extends AbstractNestedDataParser<ADMToken>
        implements IStreamDataParser, IRecordDataParser<char[]> {

    protected final ParserContext parserContext;
    protected final JsonFactory jsonFactory;
    protected final ARecordType rootType;
    protected final GeometryCoParser geometryCoParser;

    protected JsonParser jsonParser;

    /**
     * Initialize JSONDataParser with GeometryCoParser
     *
     * @param recordType
     *            defined type.
     * @param jsonFactory
     *            Jackson JSON parser factory.
     */
    public JSONDataParser(ARecordType recordType, JsonFactory jsonFactory) {
        // recordType currently cannot be null, however this is to guarantee for any future changes.
        this.rootType = recordType != null ? recordType : RecordUtil.FULLY_OPEN_RECORD_TYPE;
        this.jsonFactory = jsonFactory;
        //GeometyCoParser to parse GeoJSON objects to AsterixDB internal spatial types.
        geometryCoParser = new GeometryCoParser(jsonParser);
        parserContext = new ParserContext();
    }

    /*
     ****************************************************
     * Public methods
     ****************************************************
     */

    @Override
    public final void parse(IRawRecord<? extends char[]> record, DataOutput out) throws HyracksDataException {
        try {
            //TODO(wyk): find a way to reset byte[] instead of creating a new parser for each record.
            jsonParser = jsonFactory.createParser(record.get(), 0, record.size());
            geometryCoParser.reset(jsonParser);
            nextToken();
            parseObject(rootType, out);
        } catch (IOException e) {
            throw new RuntimeDataException(ErrorCode.RECORD_READER_MALFORMED_INPUT_STREAM, e);
        }
    }

    @Override
    public void setInputStream(InputStream in) throws IOException {
        setInput(jsonFactory.createParser(in));
    }

    public void setInputNode(JsonNode node) {
        setInput(new TreeTraversingParser(node));
    }

    private void setInput(JsonParser parser) {
        jsonParser = parser;
        geometryCoParser.reset(jsonParser);
    }

    @Override
    public boolean parse(DataOutput out) throws HyracksDataException {
        try {
            if (nextToken() == ADMToken.EOF) {
                return false;
            }
            parseObject(rootType, out);
            return true;
        } catch (IOException e) {
            throw new RuntimeDataException(ErrorCode.RECORD_READER_MALFORMED_INPUT_STREAM, e);
        }
    }

    public boolean parseAnyValue(DataOutput out) throws HyracksDataException {
        try {
            if (nextToken() == ADMToken.EOF) {
                return false;
            }
            parseValue(BuiltinType.ANY, out);
            return true;
        } catch (IOException e) {
            throw new RuntimeDataException(ErrorCode.RECORD_READER_MALFORMED_INPUT_STREAM, e);
        }
    }

    @Override
    public boolean reset(InputStream in) throws IOException {
        setInputStream(in);
        return true;
    }

    /*
     ****************************************************
     * Abstract method implementation
     ****************************************************
     */

    /**
     * Jackson token to ADM token mapper
     */
    @Override
    protected final ADMToken advanceToNextToken() throws IOException {
        final JsonToken jsonToken = jsonParser.nextToken();
        if (jsonToken == null) {
            return ADMToken.EOF;
        }
        ADMToken token;
        switch (jsonToken) {
            case VALUE_FALSE:
                token = ADMToken.FALSE;
                break;
            case VALUE_TRUE:
                token = ADMToken.TRUE;
                break;
            case VALUE_STRING:
                token = ADMToken.STRING;
                break;
            case VALUE_NULL:
                token = ADMToken.NULL;
                break;
            case VALUE_NUMBER_FLOAT:
                token = ADMToken.DOUBLE;
                break;
            case VALUE_NUMBER_INT:
                token = ADMToken.INT;
                break;
            case START_OBJECT:
                token = ADMToken.OBJECT_START;
                break;
            case END_OBJECT:
                token = ADMToken.OBJECT_END;
                break;
            case START_ARRAY:
                token = ADMToken.ARRAY_START;
                break;
            case END_ARRAY:
                token = ADMToken.ARRAY_END;
                break;
            case FIELD_NAME:
                token = ADMToken.FIELD_NAME;
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, jsonParser.currentToken().toString());
        }

        return token;
    }
    /*
     ****************************************************
     * Overridden methods
     ****************************************************
     */

    /**
     * In the case of JSON, we can parse GeoJSON objects as internal AsterixDB spatial types.
     */
    @Override
    protected boolean isConvertable(ATypeTag parsedTypeTag, ATypeTag definedTypeTag) {
        if (parsedTypeTag == ATypeTag.OBJECT && (definedTypeTag == ATypeTag.POINT || definedTypeTag == ATypeTag.LINE
                || definedTypeTag == ATypeTag.POLYGON)) {
            return true;
        }
        return super.isConvertable(parsedTypeTag, definedTypeTag);
    }

    /*
     ****************************************************
     * Complex types parsers
     ****************************************************
     */

    @Override
    protected final void parseObject(ARecordType recordType, DataOutput out) throws IOException {
        final IMutableValueStorage valueBuffer = parserContext.enterObject();
        final IARecordBuilder objectBuilder = parserContext.getObjectBuilder(recordType);
        final BitSet nullBitMap = parserContext.getNullBitmap(recordType.getFieldTypes().length);
        while (nextToken() != ADMToken.OBJECT_END) {
            /*
             * Jackson parser calls String.intern() for field names (if enabled).
             * Calling getCurrentName() will not create multiple objects.
             */
            final String fieldName = jsonParser.getCurrentName();
            final int fieldIndex = recordType.getFieldIndex(fieldName);

            if (!recordType.isOpen() && fieldIndex < 0) {
                throw new RuntimeDataException(ErrorCode.PARSER_ADM_DATA_PARSER_EXTRA_FIELD_IN_CLOSED_RECORD,
                        fieldName);
            }
            valueBuffer.reset();
            nextToken();

            if (fieldIndex < 0) {
                //field is not defined and the type is open
                parseValue(BuiltinType.ANY, valueBuffer.getDataOutput());
                objectBuilder.addField(parserContext.getSerializedFieldName(fieldName), valueBuffer);
            } else {
                //field is defined
                final IAType fieldType = recordType.getFieldType(fieldName);

                //fail fast if the current field is not nullable
                if (currentToken() == ADMToken.NULL && !isNullableType(fieldType)) {
                    throw new RuntimeDataException(ErrorCode.PARSER_TWEET_PARSER_CLOSED_FIELD_NULL, fieldName);
                }

                nullBitMap.set(fieldIndex);
                parseValue(fieldType, valueBuffer.getDataOutput());
                objectBuilder.addField(fieldIndex, valueBuffer);
            }
        }

        /*
         * Check for any possible missed values for a defined (non-nullable) type.
         * Throws exception if there is a violation
         */
        if (nullBitMap != null) {
            checkOptionalConstraints(recordType, nullBitMap);
        }
        parserContext.exitObject(valueBuffer, nullBitMap, objectBuilder);
        objectBuilder.write(out, true);
    }

    /**
     * Geometry in GeoJSON is an object
     *
     * @param typeTag
     *            geometry typeTag
     * @param out
     * @throws IOException
     */
    private void parseGeometry(ATypeTag typeTag, DataOutput out) throws IOException {
        //Start the co-parser
        geometryCoParser.starGeometry();
        while (nextToken() != ADMToken.OBJECT_END) {
            if (currentToken() == ADMToken.FIELD_NAME) {
                geometryCoParser.checkFieldName(jsonParser.getCurrentName());
            } else if (!geometryCoParser.checkValue(currentToken())) {
                throw new IOException(geometryCoParser.getErrorMessage());
            }
        }

        geometryCoParser.serialize(typeTag, out);
    }

    @Override
    protected final void parseArray(AOrderedListType listType, DataOutput out) throws IOException {
        parseCollection(listType, ADMToken.ARRAY_END, out);
    }

    @Override
    protected void parseMultiset(AUnorderedList listType, DataOutput out) throws IOException {
        throw new UnsupportedTypeException("JSON parser", ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG);
    }

    protected final void parseCollection(AbstractCollectionType collectionType, ADMToken endToken, DataOutput out)
            throws IOException {
        final IMutableValueStorage valueBuffer = parserContext.enterCollection();
        final IAsterixListBuilder arrayBuilder = parserContext.getCollectionBuilder(collectionType);
        final boolean isOpen = collectionType.getItemType().getTypeTag() == ATypeTag.ANY;
        while (nextToken() != endToken) {
            valueBuffer.reset();
            if (isOpen) {
                parseValue(BuiltinType.ANY, valueBuffer.getDataOutput());
            } else {
                //fail fast if current value is null
                if (currentToken() == ADMToken.NULL) {
                    throw new RuntimeDataException(ErrorCode.PARSER_COLLECTION_ITEM_CANNOT_BE_NULL);
                }
                parseValue(collectionType.getItemType(), valueBuffer.getDataOutput());
            }
            arrayBuilder.addItem(valueBuffer);
        }
        parserContext.exitCollection(valueBuffer, arrayBuilder);
        arrayBuilder.write(out, true);
    }

    /*
     ****************************************************
     * Value parsers and serializers
     ****************************************************
     */

    /**
     * Parse JSON object or GeoJSON object.
     *
     * @param actualType
     * @param out
     * @throws IOException
     */
    private void parseObject(IAType actualType, DataOutput out) throws IOException {
        if (actualType.getTypeTag() == ATypeTag.OBJECT) {
            parseObject((ARecordType) actualType, out);
        } else {
            parseGeometry(actualType.getTypeTag(), out);
        }
    }

    protected void parseValue(IAType definedType, DataOutput out) throws IOException {
        final ATypeTag currentTypeTag = currentToken().getTypeTag();
        /*
         * In case of type mismatch, checkAndGetType will throw an exception.
         */
        final IAType actualType = checkAndGetType(definedType, currentTypeTag);

        switch (currentToken()) {
            case NULL:
                nullSerde.serialize(ANull.NULL, out);
                break;
            case FALSE:
                booleanSerde.serialize(ABoolean.FALSE, out);
                break;
            case TRUE:
                booleanSerde.serialize(ABoolean.TRUE, out);
                break;
            case INT:
            case DOUBLE:
                serailizeNumeric(actualType.getTypeTag(), out);
                break;
            case STRING:
                serializeString(actualType.getTypeTag(), out);
                break;
            case OBJECT_START:
                parseObject(actualType, out);
                break;
            case ARRAY_START:
                parseArray((AOrderedListType) actualType, out);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.PARSE_ERROR, jsonParser.currentToken().toString());
        }
    }

    /**
     * Given that numeric values may underflow or overflow, an exception will be thrown.
     *
     * @param numericType
     * @param out
     * @throws IOException
     */
    private void serailizeNumeric(ATypeTag numericType, DataOutput out) throws IOException {
        final ATypeTag typeToUse = numericType == ATypeTag.ANY ? currentToken().getTypeTag() : numericType;

        switch (typeToUse) {
            case BIGINT:
                aInt64.setValue(jsonParser.getLongValue());
                int64Serde.serialize(aInt64, out);
                break;
            case INTEGER:
                aInt32.setValue(jsonParser.getIntValue());
                int32Serde.serialize(aInt32, out);
                break;
            case SMALLINT:
                aInt16.setValue(jsonParser.getShortValue());
                int16Serde.serialize(aInt16, out);
                break;
            case TINYINT:
                aInt8.setValue(jsonParser.getByteValue());
                int8Serde.serialize(aInt8, out);
                break;
            case DOUBLE:
                aDouble.setValue(jsonParser.getDoubleValue());
                doubleSerde.serialize(aDouble, out);
                break;
            case FLOAT:
                aFloat.setValue(jsonParser.getFloatValue());
                floatSerde.serialize(aFloat, out);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, jsonParser.currentToken().toString());
        }
    }

    /**
     * Serialize the string value.
     * TODO(wyk) avoid String objects for type STRING
     *
     * @param stringVariantType
     * @param out
     * @throws IOException
     */
    private void serializeString(ATypeTag stringVariantType, DataOutput out) throws IOException {
        char[] buffer = jsonParser.getTextCharacters();
        int begin = jsonParser.getTextOffset();
        int len = jsonParser.getTextLength();
        final ATypeTag typeToUse = stringVariantType == ATypeTag.ANY ? currentToken().getTypeTag() : stringVariantType;

        switch (typeToUse) {
            case STRING:
                parseString(buffer, begin, len, out);
                break;
            case DATE:
                parseDate(buffer, begin, len, out);
                break;
            case DATETIME:
                parseDateTime(buffer, begin, len, out);
                break;
            case TIME:
                parseTime(buffer, begin, len, out);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, jsonParser.currentToken().toString());

        }
    }
}
