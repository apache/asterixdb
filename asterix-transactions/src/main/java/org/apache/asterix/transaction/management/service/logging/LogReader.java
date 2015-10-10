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

import static org.apache.asterix.common.transactions.LogRecord.*;

/**
 * NOTE: Many method calls of this class are not thread safe.
 * Be very cautious using it in a multithreaded context.
 */
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
        fillLogReadBuffer();
    }

    /**
     * Get the next log record from the log file.
     * @return A deserialized log record, or null if we have reached the end of the file.
     * @throws ACIDException
     */
    @Override
    public ILogRecord next() throws ACIDException {
        if (waitForFlushOrReturnIfEOF() == ReturnState.EOF) {
            return null;
        }
        if (readBuffer.position() == readBuffer.limit()) {
            boolean eof = refillLogReadBuffer();
            if (eof && isRecoveryMode && readLSN < flushLSN.get()) {
                LOGGER.severe("Transaction log ends before expected. Log files may be missing.");
                return null;
            }
        }

        RECORD_STATUS status = logRecord.readLogRecord(readBuffer);
        switch(status) {
            case TRUNCATED: {
                //we may have just read off the end of the buffer, so try refiling it
                if(!refillLogReadBuffer()) {
                    return null;
                }
                //now see what we have in the refilled buffer
                status = logRecord.readLogRecord(readBuffer);
                switch(status){
                    case TRUNCATED: {
                        LOGGER.info("Log file has truncated log records.");
                        return null;
                    }
                    case BAD_CHKSUM:{
                        LOGGER.severe("Transaction log contains corrupt log records (perhaps due to medium error). Stopping recovery early.");
                        return null;
                    }
                    case OK: break;
                }
                //if we have exited the inner switch,
                // this means status is really "OK" after buffer refill
                break;
            }
            case BAD_CHKSUM:{
                LOGGER.severe("Transaction log contains corrupt log records (perhaps due to medium error). Stopping recovery early.");
                return null;
            }
            case OK: break;
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

    /**
     * Continues log analysis between log file splits.
     * @return true if log continues, false if EOF
     * @throws ACIDException
     */
    private boolean refillLogReadBuffer() throws ACIDException {
        try {
            if (readLSN % logFileSize == fileChannel.size()) {
                fileChannel.close();
                readLSN += logFileSize - (readLSN % logFileSize);
                getFileChannel();
            }
            return fillLogReadBuffer();
        } catch (IOException e) {
            throw new ACIDException(e);
        }
    }

    /**
     * Fills the log buffer with data from the log file at the current position
     * @return false if EOF, true otherwise
     * @throws ACIDException
     */

    private boolean fillLogReadBuffer() throws ACIDException {
        int size=0;
        int read=0;
        readBuffer.position(0);
        readBuffer.limit(logPageSize);
        try {
            fileChannel.position(readLSN % logFileSize);
            //We loop here because read() may return 0, but this simply means we are waiting on IO.
            //Therefore we want to break out only when either the buffer is full, or we reach EOF.
            while( size < logPageSize && read != -1) {
                read = fileChannel.read(readBuffer);
                if(read>0) {
                    size += read;
                }
            }
        } catch (IOException e) {
            throw new ACIDException(e);
        }
        readBuffer.position(0);
        readBuffer.limit(size);
        if(size == 0 && read == -1){
            return false; //EOF
        }
        bufferBeginLSN = readLSN;
        return true;
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
                fillLogReadBuffer();
            } else if (readLSN < fileBeginLSN || readLSN >= fileBeginLSN + fileChannel.size()) {
                fileChannel.close();
                getFileChannel();
                fillLogReadBuffer();
            } else if (readLSN < bufferBeginLSN || readLSN >= bufferBeginLSN + readBuffer.limit()) {
                fillLogReadBuffer();
            } else {
                readBuffer.position((int) (readLSN - bufferBeginLSN));
            }
        } catch (IOException e) {
            throw new ACIDException(e);
        }
        boolean hasRemaining;
        if(readBuffer.position() == readBuffer.limit()){
            hasRemaining = refillLogReadBuffer();
            if(!hasRemaining){
                throw new ACIDException("LSN is out of bounds");
            }
        }
        RECORD_STATUS status = logRecord.readLogRecord(readBuffer);
        switch(status){
            case TRUNCATED:{
                throw new ACIDException("LSN is out of bounds");
            }
            case BAD_CHKSUM:{
                throw new ACIDException("Log record has incorrect checksum");
            }
            case OK: break;

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
