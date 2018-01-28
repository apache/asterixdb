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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;

public class EmptyLineSeparatedRecordReader extends StreamRecordReader {

    private static final List<String> recordReaderFormats =
            Collections.unmodifiableList(Arrays.asList(ExternalDataConstants.FORMAT_LINE_SEPARATED));
    private static final String REQUIRED_CONFIGS = "";
    protected Map<String, String> config;

    private boolean prevCharCR;
    private boolean prevCharLF;
    private int newlineLength;
    private int readLength;

    @Override
    public boolean hasNext() throws IOException {
        if (done) {
            return false;
        }
        if (!skipWhiteSpace()) {
            done = true;
            close();
            return false;
        }
        newlineLength = 0;
        prevCharCR = false;
        prevCharLF = false;
        record.reset();
        readLength = 0;
        do {
            int startPosn = bufferPosn; //starting from where we left off the last time
            if (bufferPosn >= bufferLength) {
                startPosn = bufferPosn = 0;
                bufferLength = reader.read(inputBuffer);
                if (bufferLength <= 0) {
                    if (readLength > 0) {
                        record.endRecord();
                        return true;
                    }
                    close();
                    return false; //EOF
                }
            }
            for (; bufferPosn < bufferLength; ++bufferPosn) { //search for two consecutive newlines
                if (inputBuffer[bufferPosn] == ExternalDataConstants.LF) {
                    if (prevCharLF) {
                        // \n\n
                        ++bufferPosn; // at next invocation proceed from following byte
                        newlineLength = 2;
                        break;
                    } else if (prevCharCR) {
                        newlineLength += 1;
                    }
                    prevCharLF = true;
                } else {
                    prevCharLF = false;
                }
                if (inputBuffer[bufferPosn] == ExternalDataConstants.CR) { //CR + notLF, we are at notLF
                    if (prevCharCR) {
                        // \cr\cr
                        newlineLength = 2;
                        break;
                    }
                    prevCharCR = true;
                } else {
                    prevCharCR = false;
                }
                if (!(prevCharCR || prevCharLF)) {
                    newlineLength = 0;
                }
            }
            readLength = bufferPosn - startPosn;
            if (readLength > 0) {
                record.append(inputBuffer, startPosn, readLength);
            }
        } while (newlineLength < 2);
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

    private boolean skipWhiteSpace() throws IOException {
        // start by skipping white spaces
        while (true) {
            if (bufferPosn < bufferLength) {
                if (!Character.isWhitespace(inputBuffer[bufferPosn])) {
                    return true;
                }
                bufferPosn++;
            } else {
                // fill buffer
                bufferPosn = 0;
                bufferLength = reader.read(inputBuffer);
                if (bufferLength < 0) {
                    return false;
                }
            }
        }
    }

    @Override
    public void configure(AsterixInputStream inputStream, Map<String, String> config) {
        super.configure(inputStream);
        this.config = config;
    }
}
