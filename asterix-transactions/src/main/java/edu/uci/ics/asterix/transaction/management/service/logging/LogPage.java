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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.ILogPage;
import edu.uci.ics.asterix.common.transactions.ILogRecord;
import edu.uci.ics.asterix.common.transactions.MutableLong;
import edu.uci.ics.asterix.transaction.management.service.locking.LockManager;

public class LogPage implements ILogPage {

    public static final boolean IS_DEBUG_MODE = false;//true
    private static final Logger LOGGER = Logger.getLogger(LogPage.class.getName());
    private final LockManager lockMgr;
    private final LogPageReader logPageReader;
    private final int logPageSize;
    private final MutableLong flushLSN;
    private final AtomicBoolean full;
    private int appendOffset;
    private int flushOffset;
    private final ByteBuffer appendBuffer;
    private final ByteBuffer flushBuffer;
    private final ByteBuffer unlockBuffer;
    private boolean isLastPage;
    private final LinkedBlockingQueue<ILogRecord> syncCommitQ;
    private FileChannel fileChannel;
    private boolean stop;

    public LogPage(LockManager lockMgr, int logPageSize, MutableLong flushLSN) {
        this.lockMgr = lockMgr;
        this.logPageSize = logPageSize;
        this.flushLSN = flushLSN;
        appendBuffer = ByteBuffer.allocate(logPageSize);
        flushBuffer = appendBuffer.duplicate();
        unlockBuffer = appendBuffer.duplicate();
        logPageReader = getLogPageReader();
        full = new AtomicBoolean(false);
        appendOffset = 0;
        flushOffset = 0;
        isLastPage = false;
        syncCommitQ = new LinkedBlockingQueue<ILogRecord>(logPageSize / ILogRecord.JOB_TERMINATE_LOG_SIZE);
    }

    ////////////////////////////////////
    // LogAppender Methods
    ////////////////////////////////////

    @Override
    public void append(ILogRecord logRecord, long appendLSN) {
        logRecord.writeLogRecord(appendBuffer);
        logRecord.getTxnCtx().setLastLSN(logRecord.getLogType() == LogType.UPDATE ? logRecord.getResourceId() : -1,
                appendLSN);
        synchronized (this) {
            appendOffset += logRecord.getLogSize();
            if (IS_DEBUG_MODE) {
                LOGGER.info("append()| appendOffset: " + appendOffset);
            }
            if (logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT) {
                logRecord.isFlushed(false);
                syncCommitQ.offer(logRecord);
            }
            this.notify();
        }
    }

    public void setFileChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
    }

    public void setInitialFlushOffset(long offset) {
        try {
            fileChannel.position(offset);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public synchronized void isFull(boolean full) {
        this.full.set(full);
        this.notify();
    }

    public void isLastPage(boolean isLastPage) {
        this.isLastPage = isLastPage;
    }

    public boolean hasSpace(int logSize) {
        return appendOffset + logSize <= logPageSize;
    }

    public void reset() {
        appendBuffer.position(0);
        appendBuffer.limit(logPageSize);
        flushBuffer.position(0);
        flushBuffer.limit(logPageSize);
        unlockBuffer.position(0);
        unlockBuffer.limit(logPageSize);
        full.set(false);
        appendOffset = 0;
        flushOffset = 0;
        isLastPage = false;
    }

    ////////////////////////////////////
    // LogFlusher Methods
    ////////////////////////////////////

    @Override
    public void flush() {
        try {
            int endOffset;
            while (!full.get()) {
                synchronized (this) {
                    if (appendOffset - flushOffset == 0 && !full.get()) {
                        try {
                            if (IS_DEBUG_MODE) {
                                LOGGER.info("flush()| appendOffset: " + appendOffset + ", flushOffset: " + flushOffset
                                        + ", full: " + full.get());
                            }
                            if (stop) {
                                fileChannel.close();
                                break;
                            }
                            this.wait();
                        } catch (InterruptedException e) {
                            continue;
                        }
                    }
                    endOffset = appendOffset;
                }
                internalFlush(flushOffset, endOffset);
            }
            internalFlush(flushOffset, appendOffset);
            if (isLastPage) {
                fileChannel.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void internalFlush(int beginOffset, int endOffset) {
        try {
            if (endOffset > beginOffset) {
                flushBuffer.limit(endOffset);
                fileChannel.write(flushBuffer);
                fileChannel.force(false);
                flushOffset = endOffset;
                synchronized (flushLSN) {
                    flushLSN.set(flushLSN.get() + (endOffset - beginOffset));
                    flushLSN.notifyAll(); //notify to LogReaders if any
                }
                if (IS_DEBUG_MODE) {
                    LOGGER.info("internalFlush()| flushOffset: " + flushOffset + ", flushLSN: " + flushLSN.get());
                }
                batchUnlock(beginOffset, endOffset);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private LogPageReader getLogPageReader() {
        return new LogPageReader(unlockBuffer);
    }

    private void batchUnlock(int beginOffset, int endOffset) throws ACIDException {
        if (endOffset > beginOffset) {
            logPageReader.initializeScan(beginOffset, endOffset);
            lockMgr.batchUnlock(this, logPageReader);
        }
    }

    public void notifyJobTerminator() {
        ILogRecord logRecord = null;
        while (logRecord == null) {
            try {
                logRecord = syncCommitQ.take();
            } catch (InterruptedException e) {
                //ignore
            }
        }
        synchronized (logRecord) {
            logRecord.isFlushed(true);
            logRecord.notifyAll();
        }
    }

    public boolean isStop() {
        return stop;
    }

    public void isStop(boolean stop) {
        this.stop = stop;
    }
}
