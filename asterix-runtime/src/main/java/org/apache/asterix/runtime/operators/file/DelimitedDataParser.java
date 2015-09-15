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
import java.io.InputStreamReader;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.ANullSerializerDeserializer;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.std.file.FieldCursorForDelimitedDataParser;

public class DelimitedDataParser extends AbstractDataParser implements IDataParser {

    protected final IValueParserFactory[] valueParserFactories;
    protected final char fieldDelimiter;
    protected final char quote;
    protected final boolean hasHeader;
    protected final ARecordType recordType;
    private IARecordBuilder recBuilder;
    private ArrayBackedValueStorage fieldValueBuffer;
    private DataOutput fieldValueBufferOutput;
    private IValueParser[] valueParsers;
    private FieldCursorForDelimitedDataParser cursor;
    private byte[] fieldTypeTags;
    private int[] fldIds;
    private ArrayBackedValueStorage[] nameBuffers;
    private boolean areAllNullFields;

    public DelimitedDataParser(ARecordType recordType, IValueParserFactory[] valueParserFactories, char fieldDelimter,
            char quote, boolean hasHeader) {
        this.recordType = recordType;
        this.valueParserFactories = valueParserFactories;
        this.fieldDelimiter = fieldDelimter;
        this.quote = quote;
        this.hasHeader = hasHeader;
    }

    @Override
    public void initialize(InputStream in, ARecordType recordType, boolean datasetRec) throws AsterixException,
            IOException {

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
                    throw new HyracksDataException("Illegal field " + name + " in closed type " + recordType);
                } else {
                    nameBuffers[i] = new ArrayBackedValueStorage();
                    fieldNameToBytes(name, str, nameBuffers[i]);
                }
            }
        }

        cursor = new FieldCursorForDelimitedDataParser(new InputStreamReader(in), fieldDelimiter, quote);
    }

    @Override
    public boolean parse(DataOutput out) throws AsterixException, IOException {
        if (hasHeader && cursor.recordCount == 0) {
            // Consume all fields of first record
            cursor.nextRecord();
            while (cursor.nextField());
        }
        while (cursor.nextRecord()) {
            recBuilder.reset(recordType);
            recBuilder.init();
            areAllNullFields = true;

            for (int i = 0; i < valueParsers.length; ++i) {
                if (!cursor.nextField()) {
                    break;
                }
                fieldValueBuffer.reset();

                if (cursor.fStart == cursor.fEnd && recordType.getFieldTypes()[i].getTypeTag() != ATypeTag.STRING
                        && recordType.getFieldTypes()[i].getTypeTag() != ATypeTag.NULL) {
                    // if the field is empty and the type is optional, insert
                    // NULL. Note that string type can also process empty field as an
                    // empty string
                    if (!NonTaggedFormatUtil.isOptional(recordType.getFieldTypes()[i])) {
                        throw new AsterixException("At record: " + cursor.recordCount + " - Field " + cursor.fieldCount
                                + " is not an optional type so it cannot accept null value. ");
                    }
                    fieldValueBufferOutput.writeByte(ATypeTag.NULL.serialize());
                    ANullSerializerDeserializer.INSTANCE.serialize(ANull.NULL, out);
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
            }

            if (!areAllNullFields) {
                recBuilder.write(out, true);
                return true;
            }
        }
        return false;
    }

    protected void fieldNameToBytes(String fieldName, AMutableString str, ArrayBackedValueStorage buffer)
            throws HyracksDataException {
        buffer.reset();
        DataOutput out = buffer.getDataOutput();
        str.setValue(fieldName);
        try {
            stringSerde.serialize(str, out);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

}
