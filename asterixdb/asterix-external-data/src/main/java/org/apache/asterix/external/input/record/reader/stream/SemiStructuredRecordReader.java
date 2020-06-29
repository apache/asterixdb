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
package org.apache.asterix.external.input.record.reader.stream;

import static org.apache.asterix.external.util.ExternalDataConstants.CLOSING_BRACKET;
import static org.apache.asterix.external.util.ExternalDataConstants.COMMA;
import static org.apache.asterix.external.util.ExternalDataConstants.CR;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_RECORD_END;
import static org.apache.asterix.external.util.ExternalDataConstants.LF;
import static org.apache.asterix.external.util.ExternalDataConstants.OPEN_BRACKET;
import static org.apache.asterix.external.util.ExternalDataConstants.REC_ENDED_AT_EOF;
import static org.apache.asterix.external.util.ExternalDataConstants.SPACE;
import static org.apache.asterix.external.util.ExternalDataConstants.TAB;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.ParseUtil;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public class SemiStructuredRecordReader extends StreamRecordReader {

    private enum State {
        TOP_LEVEL, // valid chars at this state: '{' or '[' to start a new record or array of records
        ARRAY, // valid chars at this state: '{' or ']' to start the first nested record or close the array
        NESTED_OBJECT, // valid chars at this state: ',' or ']' to close the array or expect another nested record
        AFTER_COMMA // valid chars at this state: '{' to start a new nested record
    }

    private IWarningCollector warnings;
    private int depth;
    private boolean prevCharEscape;
    private boolean inString;
    private char recordStart;
    private char recordEnd;
    private boolean hasStarted;
    private boolean hasFinished;
    private boolean isLastCharCR;
    private State state = State.TOP_LEVEL;
    private long beginLineNumber = 1;
    private long lineNumber = 1;

    private static final List<String> recordReaderFormats = Collections.unmodifiableList(
            Arrays.asList(ExternalDataConstants.FORMAT_ADM, ExternalDataConstants.FORMAT_JSON_LOWER_CASE,
                    ExternalDataConstants.FORMAT_JSON_UPPER_CASE, ExternalDataConstants.FORMAT_SEMISTRUCTURED));
    private static final String REQUIRED_CONFIGS = "";

    @Override
    public void configure(IHyracksTaskContext ctx, AsterixInputStream stream, Map<String, String> config)
            throws HyracksDataException {
        super.configure(stream, config);
        stream.setNotificationHandler(this);
        warnings = ctx.getWarningCollector();
        // set record opening char
        recordStart = ExternalDataUtils.validateGetRecordStart(config);
        // set record ending char
        recordEnd = ExternalDataUtils.validateGetRecordEnd(config);
        if (recordStart == recordEnd) {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, KEY_RECORD_END, recordEnd);
        }
    }

    @Override
    public void notifyNewSource() {
        if (hasStarted && warnings.shouldWarn()) {
            ParseUtil.warn(warnings, getPreviousStreamName(), lineNumber, 0, REC_ENDED_AT_EOF);
        }
        beginLineNumber = 1;
        lineNumber = 1;
        state = State.TOP_LEVEL;
        resetForNewRecord();
    }

    @Override
    public LongSupplier getLineNumber() {
        return this::getBeginLineNumber;
    }

    private long getBeginLineNumber() {
        return beginLineNumber;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (done) {
            return false;
        }
        resetForNewRecord();
        beginLineNumber = lineNumber;
        do {
            int startPosn = bufferPosn; // starting from where we left off the last time
            if (bufferPosn >= bufferLength) {
                startPosn = bufferPosn = 0;
                bufferLength = reader.read(inputBuffer);
                if (bufferLength < 0) {
                    if (hasStarted && warnings.shouldWarn()) {
                        ParseUtil.warn(warnings, getDataSourceName().get(), lineNumber, 0, REC_ENDED_AT_EOF);
                    }
                    close();
                    return false; // EOF
                }
            }
            if (!hasStarted) {
                for (; bufferPosn < bufferLength; ++bufferPosn) { // search for record begin
                    char c = inputBuffer[bufferPosn];
                    if (c == LF || isLastCharCR) {
                        lineNumber++;
                    }
                    isLastCharCR = c == CR;
                    if (c == SPACE || c == TAB || c == LF || c == CR) {
                        continue;
                    }
                    if (c == recordStart && state != State.NESTED_OBJECT) {
                        // '{' is allowed at the top level, after '[' and after ','
                        if (state == State.ARRAY || state == State.AFTER_COMMA) {
                            state = State.NESTED_OBJECT;
                        }
                        beginLineNumber = lineNumber;
                        startPosn = bufferPosn;
                        hasStarted = true;
                        depth = 1;
                        ++bufferPosn;
                        break;
                    } else if (c == OPEN_BRACKET && state == State.TOP_LEVEL) {
                        // '[' is allowed at the top level only
                        state = State.ARRAY;
                    } else if (c == CLOSING_BRACKET && (state == State.ARRAY || state == State.NESTED_OBJECT)) {
                        // ']' is allowed after '[' and after capturing a record in an array
                        state = State.TOP_LEVEL;
                    } else if (c == COMMA && state == State.NESTED_OBJECT) {
                        // ',' is allowed after capturing a record in an array
                        state = State.AFTER_COMMA;
                    } else {
                        // corrupted file. clear the buffer and stop reading
                        reader.reset();
                        bufferPosn = bufferLength = 0;
                        throw new RuntimeDataException(ErrorCode.RECORD_READER_MALFORMED_INPUT_STREAM);
                    }
                }
            }
            if (hasStarted) {
                for (; bufferPosn < bufferLength; ++bufferPosn) {
                    char c = inputBuffer[bufferPosn];
                    if (c == LF || isLastCharCR) {
                        lineNumber++;
                    }
                    if (inString) {
                        // we are in a string, we only care about the string end
                        if (c == ExternalDataConstants.QUOTE && !prevCharEscape) {
                            inString = false;
                        }
                        prevCharEscape = c == ExternalDataConstants.ESCAPE && !prevCharEscape;
                    } else {
                        if (c == ExternalDataConstants.QUOTE) {
                            inString = true;
                        } else if (c == recordStart) {
                            depth += 1;
                        } else if (c == recordEnd) {
                            depth -= 1;
                            if (depth == 0) {
                                hasFinished = true;
                                ++bufferPosn; // at next invocation proceed from following byte
                                break;
                            }
                        }
                    }
                    isLastCharCR = c == CR;
                }

                int appendLength = bufferPosn - startPosn;
                if (appendLength > 0) {
                    try {
                        record.append(inputBuffer, startPosn, appendLength);
                    } catch (RuntimeDataException e) {
                        reader.reset();
                        bufferPosn = bufferLength = 0;
                        throw e;
                    }
                }
            }
        } while (!hasFinished);
        record.endRecord();
        return true;
    }

    @Override
    public List<String> getRecordReaderFormats() {
        return recordReaderFormats;
    }

    @Override
    public String getRequiredConfigs() {
        return REQUIRED_CONFIGS;
    }

    private void resetForNewRecord() {
        record.reset();
        hasStarted = false;
        hasFinished = false;
        prevCharEscape = false;
        isLastCharCR = false;
        inString = false;
        depth = 0;
    }
}
