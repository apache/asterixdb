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

import static org.apache.asterix.external.util.ExternalDataConstants.REC_ENDED_IN_Q;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.ParseUtil;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public class QuotedLineRecordReader extends LineRecordReader {

    private char quote;
    private char quoteEscape;
    private boolean prevCharEscape;
    private int readLength;
    private boolean inQuote;
    private IWarningCollector warnings;
    private static final List<String> recordReaderFormats = Collections.unmodifiableList(
            Arrays.asList(ExternalDataConstants.FORMAT_DELIMITED_TEXT, ExternalDataConstants.FORMAT_CSV));
    private static final String REQUIRED_CONFIGS = ExternalDataConstants.KEY_QUOTE;

    @Override
    public void configure(IHyracksTaskContext ctx, AsterixInputStream inputStream, Map<String, String> config)
            throws HyracksDataException {
        super.configure(ctx, inputStream, config);
        this.warnings = ctx.getWarningCollector();
        String quoteString = config.get(ExternalDataConstants.KEY_QUOTE);
        ExternalDataUtils.validateQuote(quoteString);
        this.quote = quoteString.charAt(0);
        this.quoteEscape = ExternalDataUtils.validateGetQuoteEscape(config);
    }

    @Override
    public void notifyNewSource() {
        if (!record.isEmptyRecord() && warnings.shouldWarn()) {
            ParseUtil.warn(warnings, getDataSourceName().get(), recordNumber, 0, REC_ENDED_IN_Q);
        }
        // restart for a new record from a new source
        resetForNewSource();
    }

    @Override
    public void resetForNewSource() {
        super.resetForNewSource();
        prevCharEscape = false;
        readLength = 0;
        inQuote = false;
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
            newlineLength = 0;
            prevCharCR = false;
            prevCharEscape = false;
            record.reset();
            readLength = 0;
            inQuote = false;
            do {
                int startPosn = bufferPosn;
                if (bufferPosn >= bufferLength) {
                    startPosn = bufferPosn = 0;
                    bufferLength = reader.read(inputBuffer);
                    if (bufferLength <= 0) {
                        // reached end of stream
                        if (readLength <= 0 || inQuote) {
                            // haven't read anything previously OR have read and in the middle and hit the end
                            if (inQuote && warnings.shouldWarn()) {
                                ParseUtil.warn(warnings, getDataSourceName().get(), recordNumber, 0, REC_ENDED_IN_Q);
                            }
                            close();
                            return false;
                        }
                        record.endRecord();
                        break;
                    }
                }
                boolean maybeInQuote = false;
                for (; bufferPosn < bufferLength; ++bufferPosn) {
                    if (inputBuffer[bufferPosn] == quote && quoteEscape == quote) {
                        inQuote |= maybeInQuote;
                        prevCharEscape |= maybeInQuote;
                    }
                    maybeInQuote = false;
                    if (!inQuote) {
                        if (inputBuffer[bufferPosn] == ExternalDataConstants.LF) {
                            newlineLength = (prevCharCR) ? 2 : 1;
                            ++bufferPosn;
                            break;
                        }
                        if (prevCharCR) {
                            newlineLength = 1;
                            break;
                        }
                        prevCharCR = (inputBuffer[bufferPosn] == ExternalDataConstants.CR);
                        // if this is an opening quote, mark it
                        inQuote = inputBuffer[bufferPosn] == quote && !prevCharEscape;
                        // the quoteEscape != quote is for making an opening quote not an escape
                        prevCharEscape =
                                inputBuffer[bufferPosn] == quoteEscape && !prevCharEscape && quoteEscape != quote;
                    } else {
                        // if quote == quoteEscape and current char is quote, then it could be closing or escaping
                        if (inputBuffer[bufferPosn] == quote && !prevCharEscape) {
                            // this is most likely a closing quote. the outcome depends on the next char
                            inQuote = false;
                            maybeInQuote = true;
                        }
                        prevCharEscape =
                                inputBuffer[bufferPosn] == quoteEscape && !prevCharEscape && quoteEscape != quote;
                    }
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
            recordNumber++;
            return true;
        }
    }
}
