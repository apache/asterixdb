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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ILogReader;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.MutableLong;

public class LogReader implements ILogReader {

    public static final boolean IS_DEBUG_MODE = false;//true
    private static final Logger LOGGER = Logger.getLogger(LogReader.class.getName());
    private final LogManager logMgr;
    private final long logFileSize;
    private final int logPageSize;
    private final MutableLong flushLSN;
    private final boolean isRecoveryMode;
    private final ByteBuffer readBuffer;
    private final ILogRecord logRecord;
    private long readLSN;
    private long bufferBeginLSN;
    private long fileBeginLSN;
    private FileChannel fileChannel;

    private enum ReturnState {
        FLUSH,
        EOF
    };

    public LogReader(LogManager logMgr, long logFileSize, int logPageSize, MutableLong flushLSN, boolean isRecoveryMode) {
        this.logMgr = logMgr;
        this.logFileSize = logFileSize;
        this.logPageSize = logPageSize;
        this.flushLSN = flushLSN;
        this.isRecoveryMode = isRecoveryMode;
        this.readBuffer = ByteBuffer.allocate(logPageSize);
        this.logRecord = new LogRecord();
    }

    @Override
    public void initializeScan(long beginLSN) throws ACIDException {
        readLSN = beginLSN;
        if (waitForFlushOrReturnIfEOF() == ReturnState.EOF) {
            return;
        }
        getFileChannel();
        readPage();
    }

    //for scanning
    @Override
    public ILogRecord next() throws ACIDException {
        if (waitForFlushOrReturnIfEOF() == ReturnState.EOF) {
            return null;
        }
        if (readBuffer.position() == readBuffer.limit() || !logRecord.readLogRecord(readBuffer)) {
            readNextPage();
            if (!logRecord.readLogRecord(readBuffer)) {
                throw new IllegalStateException();
            }
        }
        logRecord.setLSN(readLSN);
        readLSN += logRecord.getLogSize();
        return logRecord;
    }

    private ReturnState waitForFlushOrReturnIfEOF() {
        synchronized (flushLSN) {
            while (readLSN >= flushLSN.get()) {
                if (isRecoveryMode) {
                    return ReturnState.EOF;
                }
                try {
                    if (IS_DEBUG_MODE) {
                        LOGGER.info("waitForFlushOrReturnIfEOF()| flushLSN: " + flushLSN.get() + ", readLSN: "
                                + readLSN);
                    }
                    flushLSN.wait();
                } catch (InterruptedException e) {
                    //ignore
                }
            }
            return ReturnState.FLUSH;
        }
    }

    private void readNextPage() throws ACIDException {
        try {
            if (readLSN % logFileSize == fileChannel.size()) {
                fileChannel.close();
                readLSN += logFileSize - (readLSN % logFileSize);
                getFileChannel();
            }
            readPage();
        } catch (IOException e) {
            throw new ACIDException(e);
        }
    }

    private void readPage() throws ACIDException {
        int size;
        readBuffer.position(0);
        readBuffer.limit(logPageSize);
        try {
            fileChannel.position(readLSN % logFileSize);
            size = fileChannel.read(readBuffer);
        } catch (IOException e) {
            throw new ACIDException(e);
        }
        readBuffer.position(0);
        readBuffer.limit(size);
        bufferBeginLSN = readLSN;
    }

    //for random reading
    @Override
    public ILogRecord read(long LSN) throws ACIDException {
        readLSN = LSN;
        synchronized (flushLSN) {
            while (readLSN >= flushLSN.get()) {
                try {
                    flushLSN.wait();
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        }
        try {
            if (fileChannel == null) {
                getFileChannel();
                readPage();
            } else if (readLSN < fileBeginLSN || readLSN >= fileBeginLSN + fileChannel.size()) {
                fileChannel.close();
                getFileChannel();
                readPage();
            } else if (readLSN < bufferBeginLSN || readLSN >= bufferBeginLSN + readBuffer.limit()) {
                readPage();
            } else {
                readBuffer.position((int) (readLSN - bufferBeginLSN));
            }
        } catch (IOException e) {
            throw new ACIDException(e);
        }
        if (!logRecord.readLogRecord(readBuffer)) {
            readNextPage();
            if (!logRecord.readLogRecord(readBuffer)) {
                throw new IllegalStateException();
            }
        }
        logRecord.setLSN(readLSN);
        readLSN += logRecord.getLogSize();
        return logRecord;
    }

    private void getFileChannel() throws ACIDException {
        fileChannel = logMgr.getFileChannel(readLSN, false);
        fileBeginLSN = readLSN;
    }

    @Override
    public void close() throws ACIDException {
        try {
            if (fileChannel != null) {
                fileChannel.close();
            }
        } catch (IOException e) {
            throw new ACIDException(e);
        }
    }
}
