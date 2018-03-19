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
package org.apache.asterix.transaction.management.service.logging;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.asterix.common.transactions.ILogRecord.RecordReadStatus;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LogBufferTailReader {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ByteBuffer buffer;
    private final LogRecord logRecord;
    private int endOffset;

    public LogBufferTailReader(ByteBuffer buffer) {
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
        RecordReadStatus status = logRecord.readLogRecord(buffer);
        //underflow is not expected because we are at the very tail of the current log buffer
        if (status != RecordReadStatus.OK) {
            logReadFailure(status);
            throw new IllegalStateException("Unexpected log read status: " + status);
        }
        return logRecord;
    }

    private void logReadFailure(RecordReadStatus status) {
        final int bufferRemaining = endOffset - buffer.position();
        final byte[] remainingData = new byte[bufferRemaining];
        buffer.get(remainingData);
        final char[] hexData = Hex.encodeHex(remainingData);
        LOGGER.error(
                "Unexpected read status {}, read Log: {},  buffer remaining at read: {}, buffer remaining content: {}",
                status, bufferRemaining, logRecord.getLogRecordForDisplay(), Arrays.toString(hexData));
    }
}
