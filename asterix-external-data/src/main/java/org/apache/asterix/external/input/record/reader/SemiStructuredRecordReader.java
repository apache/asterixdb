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
package org.apache.asterix.external.input.record.reader;

import java.io.IOException;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataExceptionUtils;

public class SemiStructuredRecordReader extends AbstractStreamRecordReader {

    private int depth;
    private boolean prevCharEscape;
    private boolean inString;
    private char recordStart;
    private char recordEnd;
    private int recordNumber = 0;

    public int getRecordNumber() {
        return recordNumber;
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        super.configure(configuration);
        String recStartString = configuration.get(ExternalDataConstants.KEY_RECORD_START);
        String recEndString = configuration.get(ExternalDataConstants.KEY_RECORD_END);
        if (recStartString != null) {
            if (recStartString.length() != 1) {
                throw new AsterixException(
                        ExternalDataExceptionUtils.incorrectParameterMessage(ExternalDataConstants.KEY_RECORD_START,
                                ExternalDataConstants.PARAMETER_OF_SIZE_ONE, recStartString));
            }
            recordStart = recStartString.charAt(0);
        } else {
            recordStart = ExternalDataConstants.DEFAULT_RECORD_START;
        }
        if (recEndString != null) {
            if (recEndString.length() != 1) {
                throw new AsterixException(
                        ExternalDataExceptionUtils.incorrectParameterMessage(ExternalDataConstants.KEY_RECORD_END,
                                ExternalDataConstants.PARAMETER_OF_SIZE_ONE, recEndString));
            }
            recordEnd = recEndString.charAt(0);
        } else {
            recordEnd = ExternalDataConstants.DEFAULT_RECORD_END;
        }
    }

    @Override
    public boolean hasNext() throws Exception {
        record.reset();
        boolean hasStarted = false;
        boolean hasFinished = false;
        prevCharEscape = false;
        inString = false;
        depth = 0;
        do {
            int startPosn = bufferPosn; //starting from where we left off the last time
            if (bufferPosn >= bufferLength) {
                startPosn = bufferPosn = 0;
                bufferLength = reader.read(inputBuffer);
                if (bufferLength <= 0) {
                    return false; // EOF
                }
            }
            if (!hasStarted) {
                for (; bufferPosn < bufferLength; ++bufferPosn) { //search for record begin
                    if (inputBuffer[bufferPosn] == recordStart) {
                        startPosn = bufferPosn;
                        hasStarted = true;
                        depth = 1;
                        ++bufferPosn; // at next invocation proceed from following byte
                        break;
                    } else if (inputBuffer[bufferPosn] != ExternalDataConstants.SPACE
                            && inputBuffer[bufferPosn] != ExternalDataConstants.TAB
                            && inputBuffer[bufferPosn] != ExternalDataConstants.LF
                            && inputBuffer[bufferPosn] != ExternalDataConstants.CR) {
                        // corrupted file. clear the buffer and stop reading
                        reader.skipError();
                        bufferPosn = bufferLength = 0;
                        throw new IOException("Malformed input stream");
                    }
                }
            }
            if (hasStarted) {
                for (; bufferPosn < bufferLength; ++bufferPosn) { //search for record begin
                    if (inString) {
                        // we are in a string, we only care about the string end
                        if (inputBuffer[bufferPosn] == ExternalDataConstants.QUOTE && !prevCharEscape) {
                            inString = false;
                        }
                        if (prevCharEscape) {
                            prevCharEscape = false;
                        } else {
                            prevCharEscape = inputBuffer[bufferPosn] == ExternalDataConstants.ESCAPE;
                        }
                    } else {
                        if (inputBuffer[bufferPosn] == ExternalDataConstants.QUOTE) {
                            inString = true;
                        } else if (inputBuffer[bufferPosn] == recordStart) {
                            depth += 1;
                        } else if (inputBuffer[bufferPosn] == recordEnd) {
                            depth -= 1;
                            if (depth == 0) {
                                hasFinished = true;
                                ++bufferPosn; // at next invocation proceed from following byte
                                break;
                            }
                        }
                    }
                }
            }

            int appendLength = bufferPosn - startPosn;
            if (appendLength > 0) {
                record.append(inputBuffer, startPosn, appendLength);
            }
        } while (!hasFinished);
        record.endRecord();
        recordNumber++;
        return true;
    }

    @Override
    public boolean stop() {
        return false;
    }
}
