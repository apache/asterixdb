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

import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;

public class LineRecordReader extends AbstractStreamRecordReader {

    protected boolean prevCharCR;
    protected int newlineLength;
    protected int recordNumber = 0;

    @Override
    public boolean hasNext() throws IOException {
        /* We're reading data from in, but the head of the stream may be
         * already buffered in buffer, so we have several cases:
         * 1. No newline characters are in the buffer, so we need to copy
         *    everything and read another buffer from the stream.
         * 2. An unambiguously terminated line is in buffer, so we just
         *    copy to record.
         * 3. Ambiguously terminated line is in buffer, i.e. buffer ends
         *    in CR.  In this case we copy everything up to CR to record, but
         *    we also need to see what follows CR: if it's LF, then we
         *    need consume LF as well, so next call to readLine will read
         *    from after that.
         * We use a flag prevCharCR to signal if previous character was CR
         * and, if it happens to be at the end of the buffer, delay
         * consuming it until we have a chance to look at the char that
         * follows.
         */
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
                    if (readLength > 0) {
                        record.endRecord();
                        recordNumber++;
                        return true;
                    }
                    reader.close();
                    return false; //EOF
                }
            }
            for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
                if (inputBuffer[bufferPosn] == ExternalDataConstants.LF) {
                    newlineLength = (prevCharCR) ? 2 : 1;
                    ++bufferPosn; // at next invocation proceed from following byte
                    break;
                }
                if (prevCharCR) { //CR + notLF, we are at notLF
                    newlineLength = 1;
                    break;
                }
                prevCharCR = (inputBuffer[bufferPosn] == ExternalDataConstants.CR);
            }
            readLength = bufferPosn - startPosn;
            if (prevCharCR && newlineLength == 0) {
                --readLength; //CR at the end of the buffer
            }
            if (readLength > 0) {
                record.append(inputBuffer, startPosn, readLength);
            }
        } while (newlineLength == 0);
        recordNumber++;
        return true;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        super.configure(configuration);
        if (ExternalDataUtils.hasHeader(configuration)) {
            if (hasNext()) {
                next();
            }
        }
    }
}