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
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.nio.ByteBuffer;

public class LogPageReader {

    private final ByteBuffer buffer;
    private final LogRecord logRecord;
    private int endOffset;

    public LogPageReader(ByteBuffer buffer) {
        this.buffer = buffer;
        logRecord = new LogRecord();
    }

    public void initializeScan(int beginOffset, int endOffset) {
        this.endOffset = endOffset;
        buffer.position(beginOffset);
    }

    public LogRecord next() {
        if (buffer.position() == endOffset) {
            return null;
        }
        if (!logRecord.readLogRecord(buffer)) {
            throw new IllegalStateException();
        }
        return logRecord;
    }
}
