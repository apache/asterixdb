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
import java.io.Reader;
import java.util.Arrays;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ANullSerializerDeserializer;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class DelimitedDataParser extends AbstractDataParser implements IDataParser {

    protected final IValueParserFactory[] valueParserFactories;
    protected final char fieldDelimiter;
    protected final ARecordType recordType;

    private IARecordBuilder recBuilder;
    private ArrayBackedValueStorage fieldValueBuffer;
    private DataOutput fieldValueBufferOutput;
    private IValueParser[] valueParsers;
    private FieldCursor cursor;
    private byte[] fieldTypeTags;
    private int[] fldIds;
    private ArrayBackedValueStorage[] nameBuffers;

    private boolean areAllNullFields;

    public DelimitedDataParser(ARecordType recordType, IValueParserFactory[] valueParserFactories, char fieldDelimter) {
        this.recordType = recordType;
        this.valueParserFactories = valueParserFactories;
        this.fieldDelimiter = fieldDelimter;
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

        cursor = new FieldCursor(new InputStreamReader(in));

    }

    @Override
    public boolean parse(DataOutput out) throws AsterixException, IOException {
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
                    // if the field is empty and the type is optional, insert NULL
                    // note that string type can also process empty field as an empty string
                    if (recordType.getFieldTypes()[i].getTypeTag() != ATypeTag.UNION) {
                        throw new AsterixException("Field " + i + " cannot be NULL. ");
                    }
                    fieldValueBufferOutput.writeByte(ATypeTag.NULL.serialize());
                    ANullSerializerDeserializer.INSTANCE.serialize(ANull.NULL, out);
                } else {
                    fieldValueBufferOutput.writeByte(fieldTypeTags[i]);
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

    protected enum State {
        INIT,
        IN_RECORD,
        EOR,
        CR,
        EOF
    }

    protected class FieldCursor {
        private static final int INITIAL_BUFFER_SIZE = 4096;
        private static final int INCREMENT = 4096;

        private final Reader in;

        private char[] buffer;
        private int start;
        private int end;
        private State state;

        private int fStart;
        private int fEnd;

        public FieldCursor(Reader in) {
            this.in = in;
            buffer = new char[INITIAL_BUFFER_SIZE];
            start = 0;
            end = 0;
            state = State.INIT;
        }

        public boolean nextRecord() throws IOException {
            while (true) {
                switch (state) {
                    case INIT:
                        boolean eof = !readMore();
                        if (eof) {
                            state = State.EOF;
                            return false;
                        } else {
                            state = State.IN_RECORD;
                            return true;
                        }

                    case IN_RECORD:
                        int p = start;
                        while (true) {
                            if (p >= end) {
                                int s = start;
                                eof = !readMore();
                                if (eof) {
                                    state = State.EOF;
                                    return start < end;
                                }
                                p -= (s - start);
                            }
                            char ch = buffer[p];
                            if (ch == '\n') {
                                start = p + 1;
                                state = State.EOR;
                                break;
                            } else if (ch == '\r') {
                                start = p + 1;
                                state = State.CR;
                                break;
                            }
                            ++p;
                        }
                        break;

                    case CR:
                        if (start >= end) {
                            eof = !readMore();
                            if (eof) {
                                state = State.EOF;
                                return false;
                            }
                        }
                        char ch = buffer[start];
                        if (ch == '\n') {
                            ++start;
                            state = State.EOR;
                        } else {
                            state = State.IN_RECORD;
                            return true;
                        }

                    case EOR:
                        if (start >= end) {
                            eof = !readMore();
                            if (eof) {
                                state = State.EOF;
                                return false;
                            }
                        }
                        state = State.IN_RECORD;
                        return start < end;

                    case EOF:
                        return false;
                }
            }
        }

        public boolean nextField() throws IOException {
            switch (state) {
                case INIT:
                case EOR:
                case EOF:
                case CR:
                    return false;

                case IN_RECORD:
                    boolean eof;
                    int p = start;
                    while (true) {
                        if (p >= end) {
                            int s = start;
                            eof = !readMore();
                            p -= (s - start);
                            if (eof) {
                                state = State.EOF;
                                fStart = start;
                                fEnd = p;
                                return true;
                            }
                        }
                        char ch = buffer[p];
                        if (ch == fieldDelimiter) {
                            fStart = start;
                            fEnd = p;
                            start = p + 1;
                            return true;
                        } else if (ch == '\n') {
                            fStart = start;
                            fEnd = p;
                            start = p + 1;
                            state = State.EOR;
                            return true;
                        } else if (ch == '\r') {
                            fStart = start;
                            fEnd = p;
                            start = p + 1;
                            state = State.CR;
                            return true;
                        }
                        ++p;
                    }
            }
            throw new IllegalStateException();
        }

        protected boolean readMore() throws IOException {
            if (start > 0) {
                System.arraycopy(buffer, start, buffer, 0, end - start);
            }
            end -= start;
            start = 0;

            if (end == buffer.length) {
                buffer = Arrays.copyOf(buffer, buffer.length + INCREMENT);
            }

            int n = in.read(buffer, end, buffer.length - end);
            if (n < 0) {
                return false;
            }
            end += n;
            return true;
        }

        public int getfStart() {
            return fStart;
        }

        public void setfStart(int fStart) {
            this.fStart = fStart;
        }

        public int getfEnd() {
            return fEnd;
        }

        public void setfEnd(int fEnd) {
            this.fEnd = fEnd;
        }

        public char[] getBuffer() {
            return buffer;
        }
    }

}
