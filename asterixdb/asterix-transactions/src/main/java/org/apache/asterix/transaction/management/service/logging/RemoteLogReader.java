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

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ILogReader;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.ILogRecord.RecordReadStatus;
import org.apache.asterix.common.transactions.LogRecord;

public class RemoteLogReader implements ILogReader {

    private final FileChannel fileChannel;
    private final ILogRecord logRecord;
    private final ByteBuffer readBuffer;
    private long readLSN;
    private final int logPageSize;

    public RemoteLogReader(FileChannel fileChannel, long logFileSize, int logPageSize) {
        this.fileChannel = fileChannel;
        this.logPageSize = logPageSize;
        logRecord = new LogRecord();
        readBuffer = ByteBuffer.allocate(logPageSize);
    }

    @Override
    public void initializeScan(long beginLSN) throws ACIDException {
        readLSN = beginLSN;
        fillLogReadBuffer();
    }

    private boolean fillLogReadBuffer() throws ACIDException {
        return fillLogReadBuffer(logPageSize, readBuffer);
    }

    private boolean fillLogReadBuffer(int pageSize, ByteBuffer readBuffer) throws ACIDException {
        int size = 0;
        int read = 0;
        readBuffer.position(0);
        readBuffer.limit(logPageSize);
        try {
            fileChannel.position(readLSN);
            //We loop here because read() may return 0, but this simply means we are waiting on IO.
            //Therefore we want to break out only when either the buffer is full, or we reach EOF.
            while (size < pageSize && read != -1) {
                read = fileChannel.read(readBuffer);
                if (read > 0) {
                    size += read;
                }
            }
        } catch (IOException e) {
            throw new ACIDException(e);
        }
        readBuffer.position(0);
        readBuffer.limit(size);
        if (size == 0 && read == -1) {
            return false; //EOF
        }
        return true;
    }

    @Override
    public ILogRecord read(long LSN) throws ACIDException {
        throw new UnsupportedOperationException("Random read is not supported.");
    }

    @Override
    public ILogRecord next() throws ACIDException {
        if (readBuffer.position() == readBuffer.limit()) {
            boolean hasRemaining = fillLogReadBuffer();
            if (!hasRemaining) {
                return null;
            }
        }
        ByteBuffer readBuffer = this.readBuffer;
        boolean refilled = false;

        while (true) {
            RecordReadStatus status = logRecord.readRemoteLog(readBuffer, true);
            switch (status) {
                case TRUNCATED: {
                    if (!refilled) {
                        //we may have just read off the end of the buffer, so try refiling it
                        if (!fillLogReadBuffer()) {
                            return null;
                        }
                        refilled = true;
                        //now see what we have in the refilled buffer
                        continue;
                    }
                    return null;
                }
                case LARGE_RECORD: {
                    readBuffer = ByteBuffer.allocate(logRecord.getLogSize());
                    fillLogReadBuffer(logRecord.getLogSize(), readBuffer);
                    //now see what we have in the expanded buffer
                    continue;
                }
                case BAD_CHKSUM: {
                    return null;
                }
                case OK:
                    break;
            }
            break;
        }

        readLSN += logRecord.getSerializedLogSize();
        return logRecord;
    }

    @Override
    public void close() throws ACIDException {
        try {
            if (fileChannel != null) {
                if (fileChannel.isOpen()) {
                    fileChannel.close();
                }
            }
        } catch (IOException e) {
            throw new ACIDException(e);
        }
    }

}
