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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.replication.IReplicationThread;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ILogBuffer;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.transactions.MutableLong;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import org.apache.asterix.transaction.management.service.transaction.TransactionSubsystem;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class LogBuffer implements ILogBuffer {

    public static final boolean IS_DEBUG_MODE = false;//true
    private static final Logger LOGGER = Logger.getLogger(LogBuffer.class.getName());
    private final TransactionSubsystem txnSubsystem;
    private final LogBufferTailReader logBufferTailReader;
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
    private final LinkedBlockingQueue<ILogRecord> flushQ;
    private final LinkedBlockingQueue<ILogRecord> remoteJobsQ;
    private FileChannel fileChannel;
    private boolean stop;
    private final DatasetId reusableDsId;
    private final JobId reusableJobId;

    public LogBuffer(TransactionSubsystem txnSubsystem, int logPageSize, MutableLong flushLSN) {
        this.txnSubsystem = txnSubsystem;
        this.logPageSize = logPageSize;
        this.flushLSN = flushLSN;
        appendBuffer = ByteBuffer.allocate(logPageSize);
        flushBuffer = appendBuffer.duplicate();
        unlockBuffer = appendBuffer.duplicate();
        logBufferTailReader = getLogBufferTailReader();
        full = new AtomicBoolean(false);
        appendOffset = 0;
        flushOffset = 0;
        isLastPage = false;
        syncCommitQ = new LinkedBlockingQueue<ILogRecord>(logPageSize / ILogRecord.JOB_TERMINATE_LOG_SIZE);
        flushQ = new LinkedBlockingQueue<ILogRecord>();
        remoteJobsQ = new LinkedBlockingQueue<ILogRecord>();
        reusableDsId = new DatasetId(-1);
        reusableJobId = new JobId(-1);
    }

    ////////////////////////////////////
    // LogAppender Methods
    ////////////////////////////////////

    @Override
    public void append(ILogRecord logRecord, long appendLSN) {
        logRecord.writeLogRecord(appendBuffer);
        if (logRecord.getLogType() != LogType.FLUSH) {
            logRecord.getTxnCtx().setLastLSN(appendLSN);
        }
        synchronized (this) {
            appendOffset += logRecord.getLogSize();
            if (IS_DEBUG_MODE) {
                LOGGER.info("append()| appendOffset: " + appendOffset);
            }
            if (logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT) {
                logRecord.isFlushed(false);
                syncCommitQ.offer(logRecord);
            }
            if (logRecord.getLogType() == LogType.FLUSH) {
                logRecord.isFlushed(false);
                flushQ.offer(logRecord);
            }
            this.notify();
        }
    }

    @Override
    public void appendWithReplication(ILogRecord logRecord, long appendLSN) {
        logRecord.writeLogRecord(appendBuffer, appendLSN);

        if (logRecord.getLogSource() == LogSource.LOCAL && logRecord.getLogType() != LogType.FLUSH) {
            if (logRecord.getLogType() != LogType.FLUSH) {
                logRecord.getTxnCtx().setLastLSN(appendLSN);
            }
        }
        synchronized (this) {
            appendOffset += logRecord.getLogSize();
            if (IS_DEBUG_MODE) {
                LOGGER.info("append()| appendOffset: " + appendOffset);
            }
            if (logRecord.getLogSource() == LogSource.LOCAL) {
                if (logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT) {
                    logRecord.isFlushed(false);
                    syncCommitQ.offer(logRecord);
                }
                if (logRecord.getLogType() == LogType.FLUSH) {
                    logRecord.isFlushed(false);
                    flushQ.offer(logRecord);
                }
            } else if (logRecord.getLogSource() == LogSource.REMOTE) {

                if (logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.FLUSH) {
                    remoteJobsQ.offer(logRecord);
                }
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
        stop = false;
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

    private LogBufferTailReader getLogBufferTailReader() {
        return new LogBufferTailReader(unlockBuffer);
    }

    private void batchUnlock(int beginOffset, int endOffset) throws ACIDException {
        if (endOffset > beginOffset) {
            logBufferTailReader.initializeScan(beginOffset, endOffset);

            ITransactionContext txnCtx = null;

            LogRecord logRecord = logBufferTailReader.next();
            while (logRecord != null) {
                if (logRecord.getLogSource() == LogSource.LOCAL) {
                    if (logRecord.getLogType() == LogType.ENTITY_COMMIT) {
                        reusableJobId.setId(logRecord.getJobId());
                        txnCtx = txnSubsystem.getTransactionManager().getTransactionContext(reusableJobId, false);
                        reusableDsId.setId(logRecord.getDatasetId());
                        txnSubsystem.getLockManager().unlock(reusableDsId, logRecord.getPKHashValue(), LockMode.ANY,
                                txnCtx);
                        txnCtx.notifyOptracker(false);
                    } else if (logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT) {
                        reusableJobId.setId(logRecord.getJobId());
                        txnCtx = txnSubsystem.getTransactionManager().getTransactionContext(reusableJobId, false);
                        txnCtx.notifyOptracker(true);
                        notifyJobTerminator();
                    } else if (logRecord.getLogType() == LogType.FLUSH) {
                        notifyFlushTerminator();
                    }
                } else if (logRecord.getLogSource() == LogSource.REMOTE) {
                    if (logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.FLUSH) {
                        notifyReplicationTerminator();
                    }
                }

                logRecord = logBufferTailReader.next();
            }
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

    public void notifyFlushTerminator() throws ACIDException {
        LogRecord logRecord = null;
        try {
            logRecord = (LogRecord) flushQ.take();
        } catch (InterruptedException e) {
            //ignore
        }
        synchronized (logRecord) {
            logRecord.isFlushed(true);
            logRecord.notifyAll();
        }
        PrimaryIndexOperationTracker opTracker = logRecord.getOpTracker();
        if (opTracker != null) {
            try {
                opTracker.triggerScheduleFlush(logRecord);
            } catch (HyracksDataException e) {
                throw new ACIDException(e);
            }
        }
    }

    public void notifyReplicationTerminator() {
        LogRecord logRecord = null;
        try {
            logRecord = (LogRecord) remoteJobsQ.take();
        } catch (InterruptedException e) {
            //ignore
        }
        logRecord.isFlushed(true);
        IReplicationThread replicationThread = logRecord.getReplicationThread();

        if (replicationThread != null) {
            try {
                replicationThread.notifyLogReplicationRequester(logRecord);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isStop() {
        return stop;
    }

    public void isStop(boolean stop) {
        this.stop = stop;
    }
}
