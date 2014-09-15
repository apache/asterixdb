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
package edu.uci.ics.hyracks.dataflow.std.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class DelimitedDataTupleParserFactory implements ITupleParserFactory {
    private static final long serialVersionUID = 1L;
    private IValueParserFactory[] valueParserFactories;
    private char fieldDelimiter;
    private char quote;

    private boolean isDoubleQuoteIncludedInThisField;
    private int doubleQuoteCount;

    private int lineCount;
    private int fieldCount;

    public DelimitedDataTupleParserFactory(IValueParserFactory[] fieldParserFactories, char fieldDelimiter) {
        this(fieldParserFactories,fieldDelimiter, '\"');
    }

    public DelimitedDataTupleParserFactory(IValueParserFactory[] fieldParserFactories, char fieldDelimiter, char quote) {
        this.valueParserFactories = fieldParserFactories;
        this.fieldDelimiter = fieldDelimiter;
        this.quote = quote;
        this.fieldCount = 0;
        this.lineCount = 1;
    }

    @Override
    public ITupleParser createTupleParser(final IHyracksTaskContext ctx) {
        return new ITupleParser() {
            @Override
            public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
                try {
                    IValueParser[] valueParsers = new IValueParser[valueParserFactories.length];
                    for (int i = 0; i < valueParserFactories.length; ++i) {
                        valueParsers[i] = valueParserFactories[i].createValueParser();
                    }
                    ByteBuffer frame = ctx.allocateFrame();
                    FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                    appender.reset(frame, true);
                    ArrayTupleBuilder tb = new ArrayTupleBuilder(valueParsers.length);
                    DataOutput dos = tb.getDataOutput();

                    FieldCursor cursor = new FieldCursor(new InputStreamReader(in));
                    while (cursor.nextRecord()) {
                        tb.reset();
                        for (int i = 0; i < valueParsers.length; ++i) {
                            if (!cursor.nextField()) {
                                break;
                            }
                            // Eliminate doule quotes in the field that we are going to parse
                            if (isDoubleQuoteIncludedInThisField) {
                                eliminateDoulbleQuote(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart);
                                cursor.fEnd -= doubleQuoteCount;
                                isDoubleQuoteIncludedInThisField = false;
                            }
                            valueParsers[i].parse(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart, dos);
                            tb.addFieldEndOffset();
                            fieldCount++;
                        }
                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            FrameUtils.flushFrame(frame, writer);
                            appender.reset(frame, true);
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                throw new HyracksDataException("Record size (" + tb.getSize()
                                        + ") larger than frame size (" + appender.getBuffer().capacity() + ")");
                            }
                        }
                    }
                    if (appender.getTupleCount() > 0) {
                        FrameUtils.flushFrame(frame, writer);
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

        };
    }

    private enum State {
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

        private int lastQuotePosition;
        private int lastDoubleQuotePosition;
        private int lastDelimiterPosition;
        private int quoteCount;
        private boolean startedQuote;

        public FieldCursor(Reader in) {
            this.in = in;
            buffer = new char[INITIAL_BUFFER_SIZE];
            start = 0;
            end = 0;
            state = State.INIT;
            lastDelimiterPosition = -99;
            lastQuotePosition = -99;
            lastDoubleQuotePosition = -99;
            quoteCount = 0;
            startedQuote = false;
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
                                lastQuotePosition -= (s - start);
                                lastDoubleQuotePosition -= (s - start);
                                lastDelimiterPosition -= (s - start);
                            }
                            char ch = buffer[p];
                            // We perform rough format correctness (delimiter, quote) check here
                            // to set the starting position of a record.
                            // In the field level, more checking will be conducted.
                            if (ch == quote) {
                                startedQuote = true;
                                // check two quotes in a row - "". This is an escaped quote
                                if (lastQuotePosition == p - 1 && start != p - 1 && lastDoubleQuotePosition != p - 1) {
                                    lastDoubleQuotePosition = p;
                                }
                                lastQuotePosition = p;
                            } else if (ch == fieldDelimiter) {
                                if (startedQuote && lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1) {
                                    startedQuote = false;
                                    lastDelimiterPosition = p;
                                }
                            } else if (ch == '\n' && !startedQuote) {
                                start = p + 1;
                                state = State.EOR;
                                lastDelimiterPosition = p;
                                break;
                            } else if (ch == '\r' && !startedQuote) {
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
                        if (ch == '\n' && !startedQuote) {
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
                        lastDelimiterPosition = start;
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
                    // reset quote related values
                    startedQuote = false;
                    isDoubleQuoteIncludedInThisField = false;
                    lastQuotePosition = -99;
                    lastDoubleQuotePosition = -99;
                    quoteCount = 0;
                    doubleQuoteCount = 0;

                    int p = start;
                    while (true) {
                        if (p >= end) {
                            int s = start;
                            eof = !readMore();
                            p -= (s - start);
                            lastQuotePosition -= (s - start);
                            lastDoubleQuotePosition -= (s - start);
                            lastDelimiterPosition -= (s - start);
                            if (eof) {
                                state = State.EOF;
                                if (startedQuote && lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1
                                        && quoteCount == doubleQuoteCount * 2 + 2) {
                                    // set the position of fStart to +1, fEnd to -1 to remove quote character
                                    fStart = start + 1;
                                    fEnd = p - 1;
                                } else {
                                    fStart = start;
                                    fEnd = p;
                                }
                                return true;
                            }
                        }
                        char ch = buffer[p];
                        if (ch == quote) {
                            // If this is first quote in the field, then it needs to be placed in the beginning.
                            if (!startedQuote) {
                                if (lastDelimiterPosition == p - 1 || lastDelimiterPosition == -99) {
                                    startedQuote = true;
                                } else {
                                    // In this case, we don't have a quote in the beginning of a field.
                                    throw new IOException(
                                            "At line: "
                                                    + lineCount
                                                    + ", field#: "
                                                    + (fieldCount+1)
                                                    + " - a quote enclosing a field needs to be placed in the beginning of that field.");
                                }
                            }
                            // Check double quotes - "". We check [start != p-2]
                            // to avoid false positive where there is no value in a field,
                            // since it looks like a double quote. However, it's not a double quote.
                            // (e.g. if field2 has no value:
                            //       field1,"",field3 ... )
                            if (lastQuotePosition == p - 1 && lastDelimiterPosition != p - 2
                                    && lastDoubleQuotePosition != p - 1) {
                                isDoubleQuoteIncludedInThisField = true;
                                doubleQuoteCount++;
                                lastDoubleQuotePosition = p;
                            }
                            lastQuotePosition = p;
                            quoteCount++;
                        } else if (ch == fieldDelimiter) {
                            // If there was no quote in the field,
                            // then we assume that the field contains a valid string.
                            if (!startedQuote) {
                                fStart = start;
                                fEnd = p;
                                start = p + 1;
                                lastDelimiterPosition = p;
                                return true;
                            } else if (startedQuote) {
                                if (lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1) {
                                    // There is a quote right before the delimiter (e.g. ",)  and it is not two quote,
                                    // then the field contains a valid string.
                                    // We set the position of fStart to +1, fEnd to -1 to remove quote character
                                    fStart = start + 1;
                                    fEnd = p - 1;
                                    start = p + 1;
                                    lastDelimiterPosition = p;
                                    return true;
                                } else if (lastQuotePosition < p - 1 && lastQuotePosition != lastDoubleQuotePosition
                                        && quoteCount == doubleQuoteCount * 2 + 2) {
                                    // There is a quote before the delimiter, however it is not directly placed before the delimiter.
                                    // In this case, we throw an exception.
                                    // quoteCount == doubleQuoteCount * 2 + 2 : only true when we have two quotes except double-quotes.
                                    throw new IOException("At line: " + lineCount + ", field#: " + (fieldCount+1)
                                            + " -  A quote enclosing a field needs to be followed by the delimiter.");
                                }
                            }
                            // If the control flow reaches here: we have a delimiter in this field and
                            // there should be a quote in the beginning and the end of
                            // this field. So, just continue reading next character
                        } else if (ch == '\n') {
                            if (!startedQuote) {
                                fStart = start;
                                fEnd = p;
                                start = p + 1;
                                state = State.EOR;
                                lineCount++;
                                lastDelimiterPosition = p;
                                return true;
                            } else if (startedQuote && lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1
                                    && quoteCount == doubleQuoteCount * 2 + 2) {
                                // set the position of fStart to +1, fEnd to -1 to remove quote character
                                fStart = start + 1;
                                fEnd = p - 1;
                                lastDelimiterPosition = p;
                                start = p + 1;
                                state = State.EOR;
                                lineCount++;
                                return true;
                            }
                        } else if (ch == '\r') {
                            if (!startedQuote) {
                                fStart = start;
                                fEnd = p;
                                start = p + 1;
                                state = State.CR;
                                lastDelimiterPosition = p;
                                return true;
                            } else if (startedQuote && lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1
                                    && quoteCount == doubleQuoteCount * 2 + 2) {
                                // set the position of fStart to +1, fEnd to -1 to remove quote character
                                fStart = start + 1;
                                fEnd = p - 1;
                                lastDelimiterPosition = p;
                                start = p + 1;
                                state = State.CR;
                                return true;
                            }
                        }
                        ++p;
                    }
            }
            throw new IllegalStateException();
        }

        protected void resetQuoteRelatedValue() {
            startedQuote = false;
            isDoubleQuoteIncludedInThisField = false;
            lastQuotePosition = -99;
            lastDoubleQuotePosition = -99;
            quoteCount = 0;
            doubleQuoteCount = 0;
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

    }

    // Eliminate escaped double quotes("") in a field
    protected void eliminateDoulbleQuote(char[] buffer, int start, int length) {
        int lastDoubleQuotePosition = -99;
        int writepos = start;
        int readpos = start;
        // Find positions where double quotes appear
        for (int i = 0; i < length; i++) {
            // Skip double quotes
            if (buffer[readpos] == quote && lastDoubleQuotePosition != readpos - 1) {
                lastDoubleQuotePosition = readpos;
                readpos++;
            } else {
                // Moving characters except double quote to the front
                if (writepos != readpos) {
                    buffer[writepos] = buffer[readpos];
                }
                writepos++;
                readpos++;
            }
        }
    }
}

