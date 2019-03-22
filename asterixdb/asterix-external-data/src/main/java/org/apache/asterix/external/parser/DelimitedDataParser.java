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
import java.io.InputStreamReader;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IDataParser;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.std.file.FieldCursorForDelimitedDataParser;

public class DelimitedDataParser extends AbstractDataParser implements IStreamDataParser, IRecordDataParser<char[]> {

    private final char fieldDelimiter;
    private final char quote;
    private final boolean hasHeader;
    private ARecordType recordType;
    private IARecordBuilder recBuilder;
    private ArrayBackedValueStorage fieldValueBuffer;
    private DataOutput fieldValueBufferOutput;
    private IValueParser[] valueParsers;
    private FieldCursorForDelimitedDataParser cursor;
    private byte[] fieldTypeTags;
    private int[] fldIds;
    private ArrayBackedValueStorage[] nameBuffers;
    private boolean areAllNullFields;

    public DelimitedDataParser(IValueParserFactory[] valueParserFactories, char fieldDelimter, char quote,
            boolean hasHeader, ARecordType recordType, boolean isStreamParser) throws HyracksDataException {
        this.fieldDelimiter = fieldDelimter;
        this.quote = quote;
        this.hasHeader = hasHeader;
        this.recordType = recordType;
        valueParsers = new IValueParser[valueParserFactories.length];
        for (int i = 0; i < valueParserFactories.length; ++i) {
            valueParsers[i] = valueParserFactories[i].createValueParser();
        }

        fieldValueBuffer = new ArrayBackedValueStorage();
        fieldValueBufferOutput = fieldValueBuffer.getDataOutput();
        recBuilder = new RecordBuilder();
        recBuilder.reset(recordType);
        recBuilder.init();

        int n = recordType.getFieldNames().length;
        fieldTypeTags = new byte[n];
        for (int i = 0; i < n; i++) {
            ATypeTag tag = recordType.getFieldTypes()[i].getTypeTag();
            fieldTypeTags[i] = tag.serialize();
        }

        fldIds = new int[n];
        nameBuffers = new ArrayBackedValueStorage[n];
        AMutableString str = new AMutableString(null);
        for (int i = 0; i < n; i++) {
            String name = recordType.getFieldNames()[i];
            fldIds[i] = recBuilder.getFieldId(name);
            if (fldIds[i] < 0) {
                if (!recordType.isOpen()) {
                    throw new RuntimeDataException(ErrorCode.PARSER_DELIMITED_ILLEGAL_FIELD, name, recordType);
                } else {
                    nameBuffers[i] = new ArrayBackedValueStorage();
                    str.setValue(name);
                    IDataParser.toBytes(str, nameBuffers[i], stringSerde);
                }
            }
        }
        if (!isStreamParser) {
            cursor = new FieldCursorForDelimitedDataParser(null, fieldDelimiter, quote);
        }
    }

    @Override
    public boolean parse(DataOutput out) throws HyracksDataException {
        try {
            while (cursor.nextRecord()) {
                parseRecord();
                if (!areAllNullFields) {
                    recBuilder.write(out, true);
                    return true;
                }
            }
            return false;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void parseRecord() throws HyracksDataException {
        recBuilder.reset(recordType);
        recBuilder.init();
        areAllNullFields = true;

        for (int i = 0; i < valueParsers.length; ++i) {
            try {
                if (!cursor.nextField()) {
                    break;
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            fieldValueBuffer.reset();

            try {
                if (cursor.fStart == cursor.fEnd && recordType.getFieldTypes()[i].getTypeTag() != ATypeTag.STRING
                        && recordType.getFieldTypes()[i].getTypeTag() != ATypeTag.NULL) {
                    // if the field is empty and the type is optional, insert
                    // NULL. Note that string type can also process empty field as an
                    // empty string
                    if (!NonTaggedFormatUtil.isOptional(recordType.getFieldTypes()[i])) {
                        throw new RuntimeDataException(ErrorCode.PARSER_DELIMITED_NONOPTIONAL_NULL, cursor.recordCount,
                                cursor.fieldCount);
                    }
                    fieldValueBufferOutput.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                } else {
                    fieldValueBufferOutput.writeByte(fieldTypeTags[i]);
                    // Eliminate doule quotes in the field that we are going to parse
                    if (cursor.isDoubleQuoteIncludedInThisField) {
                        cursor.eliminateDoubleQuote(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart);
                        cursor.fEnd -= cursor.doubleQuoteCount;
                        cursor.isDoubleQuoteIncludedInThisField = false;
                    }
                    valueParsers[i].parse(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart,
                            fieldValueBufferOutput);
                    areAllNullFields = false;
                }
                if (fldIds[i] < 0) {
                    recBuilder.addField(nameBuffers[i], fieldValueBuffer);
                } else {
                    recBuilder.addField(fldIds[i], fieldValueBuffer);
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    @Override
    public void parse(IRawRecord<? extends char[]> record, DataOutput out) throws HyracksDataException {
        try {
            cursor.nextRecord(record.get(), record.size());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        parseRecord();
        if (!areAllNullFields) {
            recBuilder.write(out, true);
        }
    }

    @Override
    public void setInputStream(InputStream in) throws IOException {
        cursor = new FieldCursorForDelimitedDataParser(new InputStreamReader(in), fieldDelimiter, quote);
        if (in != null && hasHeader) {
            cursor.nextRecord();
            while (cursor.nextField()) {
                ;
            }
        }
    }

    @Override
    public boolean reset(InputStream in) throws IOException {
        cursor = new FieldCursorForDelimitedDataParser(new InputStreamReader(in), fieldDelimiter, quote);
        return true;
    }
}
