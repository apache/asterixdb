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
import java.util.function.LongSupplier;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class LineRecordReader extends StreamRecordReader {

    protected boolean hasHeader;
    protected boolean prevCharCR;
    protected int newlineLength;
    protected long beginLineNumber = 1;
    protected long lineNumber = 1;
    protected boolean newSource = false;
    private static final List<String> recordReaderFormats =
            Collections.unmodifiableList(Arrays.asList(ExternalDataConstants.FORMAT_DELIMITED_TEXT,
                    ExternalDataConstants.FORMAT_CSV, ExternalDataConstants.FORMAT_TSV));
    private static final String REQUIRED_CONFIGS = "";

    @Override
    public void configure(IHyracksTaskContext ctx, AsterixInputStream inputStream, Map<String, String> config)
            throws HyracksDataException {
        super.configure(inputStream, config);
        this.hasHeader = ExternalDataUtils.hasHeader(config);
        this.newSource = true;
        inputStream.setNotificationHandler(this);
    }

    @Override
    public void notifyNewSource() {
        resetForNewSource();
    }

    @Override
    public void resetForNewSource() {
        super.resetForNewSource();
        newSource = true;
        beginLineNumber = 1;
        lineNumber = 1;
        prevCharCR = false;
        newlineLength = 0;
    }

    @Override
    public List<String> getRecordReaderFormats() {
        return recordReaderFormats;
    }

    @Override
    public String getRequiredConfigs() {
        return REQUIRED_CONFIGS;
    }

    @Override
    public boolean hasNext() throws IOException {
        while (true) {
            if (done) {
                return false;
            }
            /*
             * We're reading data from in, but the head of the stream may be
             * already buffered in buffer, so we have several cases:
             * 1. No newline characters are in the buffer, so we need to copy
             *   everything and read another buffer from the stream.
             * 2. An unambiguously terminated line is in buffer, so we just
             *    copy to record.
             * 3. Ambiguously terminated line is in buffer, i.e. buffer ends
             *    in CR. In this case we copy everything up to CR to record, but
             * we also need to see what follows CR: if it's LF, then we
             * need consume LF as well, so next call to readLine will read
             * from after that.
             * We use a flag prevCharCR to signal if previous character was CR
             * and, if it happens to be at the end of the buffer, delay
             * consuming it until we have a chance to look at the char that
             * follows.
             */
            beginLineNumber = lineNumber;
            newlineLength = 0; //length of terminating newline
            prevCharCR = false; //true of prev char was CR
            record.reset();
            int readLength = 0;
            do {
                int startPosn = bufferPosn; //starting from where we left off the last time
                if (bufferPosn >= bufferLength) {
                    startPosn = bufferPosn = 0;
                    bufferLength = reader.read(inputBuffer);
                    if (bufferLength <= 0) {
                        if (readLength <= 0) {
                            close();
                            return false; //EOF
                        }
                        record.endRecord();
                        break;
                    }
                }
                for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
                    if (inputBuffer[bufferPosn] == ExternalDataConstants.LF) {
                        newlineLength = (prevCharCR) ? 2 : 1;
                        ++bufferPosn; // at next invocation proceed from following byte
                        ++lineNumber;
                        break;
                    }
                    if (prevCharCR) { //CR + notLF, we are at notLF
                        ++lineNumber;
                        newlineLength = 1;
                        break;
                    }
                    prevCharCR = (inputBuffer[bufferPosn] == ExternalDataConstants.CR);
                }
                readLength = bufferPosn - startPosn;
                if (readLength > 0) {
                    record.append(inputBuffer, startPosn, readLength);
                }
            } while (newlineLength == 0);
            if (record.isEmptyRecord()) {
                continue;
            }
            if (newSource && hasHeader) {
                newSource = false;
                continue;
            }
            return true;
        }
    }

    @Override
    public LongSupplier getLineNumber() {
        return this::getBeginLineNumber;
    }

    private long getBeginLineNumber() {
        return beginLineNumber;
    }
}
