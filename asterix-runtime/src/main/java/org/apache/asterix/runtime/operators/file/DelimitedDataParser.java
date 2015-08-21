/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.operators.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ANullSerializerDeserializer;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FieldCursorForDelimitedDataParser;

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
