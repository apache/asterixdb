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
package org.apache.hyracks.dataflow.std.file;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;

public class FieldCursorForDelimitedDataParser {

    private enum State {
        INIT, //initial state
        IN_RECORD, //cursor is inside record
        EOR, //cursor is at end of record
        CR, //cursor at carriage return
        EOF, //end of stream reached
        FAILED // cursor failed to parse a field
    }

    public enum Result {
        OK,
        ERROR,
        END
    }

    private static final String CLOSING_Q = "missing a closing quote";
    private static final String OPENING_Q = "a quote should be in the beginning";
    private static final String DELIMITER_AFTER_Q = "a quote enclosing a field needs to be followed by the delimiter";
    private final IWarningCollector warnings;
    private final Supplier<String> dataSourceName;
    private char[] buffer; //buffer to holds the input coming form the underlying input stream
    private int fStart; //start position for field
    private int fEnd; //end position for field
    private long lineCount; //count of lines
    private int fieldCount; //count of fields in current record
    private int doubleQuoteCount; //count of double quotes
    private boolean isDoubleQuoteIncludedInThisField; //does current field include double quotes

    private static final int INITIAL_BUFFER_SIZE = 4096;//initial buffer size
    private static final int INCREMENT = 4096; //increment size

    private final Reader in; //the underlying buffer

    private int start; //start of valid buffer area
    private int end; //end of valid buffer area
    private State state; //state (see states above)

    private int lastQuotePosition; //position of last quote
    private int lastDoubleQuotePosition; //position of last double quote
    private int lastDelimiterPosition; //position of last delimiter
    private int quoteCount; //count of single quotes
    private boolean startedQuote; //whether a quote has been started

    private final char quote; //the quote character
    private final char fieldDelimiter; //the delimiter

    public FieldCursorForDelimitedDataParser(Reader in, char fieldDelimiter, char quote,
            IWarningCollector warningCollector, Supplier<String> dataSourceName) {
        this.warnings = warningCollector;
        this.dataSourceName = dataSourceName;
        this.in = in;
        if (in != null) {
            buffer = new char[INITIAL_BUFFER_SIZE];
            end = 0;
        } else {
            end = Integer.MAX_VALUE;
        }
        start = 0;
        state = State.INIT;
        this.quote = quote;
        this.fieldDelimiter = fieldDelimiter;
        lastDelimiterPosition = -1;
        lastQuotePosition = -1;
        lastDoubleQuotePosition = -1;
        quoteCount = 0;
        doubleQuoteCount = 0;
        startedQuote = false;
        isDoubleQuoteIncludedInThisField = false;
        lineCount = 1;
        fieldCount = 0;
    }

    public char[] getBuffer() {
        return buffer;
    }

    public int getFieldStart() {
        return fStart;
    }

    public int getFieldLength() {
        return fEnd - fStart;
    }

    public boolean isFieldEmpty() {
        return fStart == fEnd;
    }

    public boolean fieldHasDoubleQuote() {
        return isDoubleQuoteIncludedInThisField;
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public long getLineCount() {
        return lineCount;
    }

    public void nextRecord(char[] buffer, int recordLength, long lineNumber) {
        lineCount = lineNumber;
        fieldCount = 0;
        lastDelimiterPosition = -1;
        lastQuotePosition = -1;
        lastDoubleQuotePosition = -1;
        quoteCount = 0;
        doubleQuoteCount = 0;
        startedQuote = false;
        isDoubleQuoteIncludedInThisField = false;
        start = 0;
        end = recordLength;
        state = State.IN_RECORD;
        this.buffer = buffer;
    }

    public boolean nextRecord() throws IOException {
        fieldCount = 0;
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
                    char lastChar = '\0';
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
                            lastDelimiterPosition = p;
                            break;
                        }
                        // count lines inside quotes
                        if (ch == '\r' || (ch == '\n' && lastChar != '\r')) {
                            lineCount++;
                        }
                        lastChar = ch;
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
                    // if the next char "ch" is not \n, then count the \r
                    if (ch != '\n') {
                        lineCount++;
                    }
                    if (ch == '\n' && !startedQuote) {
                        ++start;
                        state = State.EOR;
                    } else {
                        state = State.IN_RECORD;
                        return true;
                    }

                case EOR:
                    lineCount++;
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
                case FAILED:
                    return false;
            }
        }
    }

    public Result nextField() throws IOException {
        switch (state) {
            case INIT:
            case EOR:
            case EOF:
            case CR:
                return Result.END;
            case FAILED:
                return Result.ERROR;

            case IN_RECORD:
                fieldCount++;
                // reset quote related values
                startedQuote = false;
                isDoubleQuoteIncludedInThisField = false;
                lastQuotePosition = -1;
                lastDoubleQuotePosition = -1;
                quoteCount = 0;
                doubleQuoteCount = 0;

                char lastChar = '\0';
                int p = start;
                while (true) {
                    if (p >= end) {
                        int s = start;
                        boolean eof = !readMore();
                        p -= (s - start);
                        lastQuotePosition -= (lastQuotePosition > -1) ? (s - start) : 0;
                        lastDoubleQuotePosition -= (lastDoubleQuotePosition > -1) ? (s - start) : 0;
                        lastDelimiterPosition -= (lastDelimiterPosition > -1) ? (s - start) : 0;
                        if (eof) {
                            state = State.EOF;
                            if (!startedQuote) {
                                fStart = start;
                                fEnd = p;
                            } else {
                                if (lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1
                                        && quoteCount == doubleQuoteCount * 2 + 2) {
                                    // set the position of fStart to +1, fEnd to -1 to remove quote character
                                    fStart = start + 1;
                                    fEnd = p - 1;
                                } else {
                                    state = State.FAILED;
                                    if (warnings.shouldWarn()) {
                                        warn(CLOSING_Q);
                                    }
                                    return Result.ERROR;
                                }
                            }
                            return Result.OK;
                        }
                    }
                    char ch = buffer[p];
                    if (ch == quote) {
                        // If this is first quote in the field, then it needs to be placed in the beginning.
                        if (!startedQuote) {
                            if (p == start) {
                                startedQuote = true;
                            } else {
                                // In this case, we don't have a quote in the beginning of a field.
                                state = State.FAILED;
                                if (warnings.shouldWarn()) {
                                    warn(OPENING_Q);
                                }
                                return Result.ERROR;
                            }
                        }
                        // Check double quotes - "". We check [start != p-2]
                        // to avoid false positive where there is no value in a field,
                        // since it looks like a double quote. However, it's not a double quote.
                        // (e.g. if field2 has no value:
                        //       field1,"",field3 ... )
                        if (lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1
                                && lastQuotePosition != start) {
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
                            return Result.OK;
                        }

                        if (lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1
                                && lastQuotePosition != start) {
                            // There is a quote right before the delimiter (e.g. ",)  and it is not two quote,
                            // then the field contains a valid string.
                            // We set the position of fStart to +1, fEnd to -1 to remove quote character
                            fStart = start + 1;
                            fEnd = p - 1;
                            start = p + 1;
                            lastDelimiterPosition = p;
                            startedQuote = false;
                            return Result.OK;
                        } else if (lastQuotePosition < p - 1 && lastQuotePosition != lastDoubleQuotePosition
                                && quoteCount == doubleQuoteCount * 2 + 2) {
                            // There is a quote before the delimiter, however it is not directly placed before the delimiter.
                            // In this case, we throw an exception.
                            // quoteCount == doubleQuoteCount * 2 + 2 : only true when we have two quotes except double-quotes.
                            state = State.FAILED;
                            if (warnings.shouldWarn()) {
                                warn(DELIMITER_AFTER_Q);
                            }
                            return Result.ERROR;
                        }
                        // If the control flow reaches here: we have a delimiter in this field and
                        // there should be a quote in the beginning and the end of
                        // this field. So, just continue reading next character
                    } else if (ch == '\n' || ch == '\r') {
                        if (!startedQuote) {
                            fStart = start;
                            fEnd = p;
                            start = p + 1;
                            state = ch == '\n' ? State.EOR : State.CR;
                            lastDelimiterPosition = p;
                            return Result.OK;
                        } else if (lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1
                                && quoteCount == doubleQuoteCount * 2 + 2) {
                            // set the position of fStart to +1, fEnd to -1 to remove quote character
                            fStart = start + 1;
                            fEnd = p - 1;
                            lastDelimiterPosition = p;
                            start = p + 1;
                            state = ch == '\n' ? State.EOR : State.CR;
                            startedQuote = false;
                            return Result.OK;
                        }
                    }
                    // count lines inside quotes
                    if (ch == '\r' || (ch == '\n' && lastChar != '\r')) {
                        lineCount++;
                    }
                    lastChar = ch;
                    ++p;
                }
        }
        throw new IllegalStateException();
    }

    private boolean readMore() throws IOException {
        if (in == null) {
            return false;
        }
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

    // Eliminate escaped double quotes("") in a field
    public void eliminateDoubleQuote() {
        int lastDoubleQuotePosition = -1;
        int writepos = fStart;
        int readpos = fStart;
        int length = fEnd - fStart;
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
        fEnd -= doubleQuoteCount;
        isDoubleQuoteIncludedInThisField = false;
    }

    private void warn(String message) {
        warnings.warn(Warning.forHyracks(null, ErrorCode.PARSING_ERROR, dataSourceName.get(), lineCount, fieldCount,
                message));
    }
}
