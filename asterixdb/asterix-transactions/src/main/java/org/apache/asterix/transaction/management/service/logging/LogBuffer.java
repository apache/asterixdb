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
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.transactions.MutableLong;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class LogBuffer implements ILogBuffer {

    public static final boolean IS_DEBUG_MODE = false;//true
    private static final Logger LOGGER = Logger.getLogger(LogBuffer.class.getName());
    private final ITransactionSubsystem txnSubsystem;
    private final LogBufferTailReader logBufferTailReader;
    private final int logPageSize;
    private final MutableLong flushLSN;
    private final AtomicBoolean full;
    protected int appendOffset;
    private int flushOffset;
    protected final ByteBuffer appendBuffer;
    private final ByteBuffer flushBuffer;
    private final ByteBuffer unlockBuffer;
    private boolean isLastPage;
    protected final LinkedBlockingQueue<ILogRecord> syncCommitQ;
    protected final LinkedBlockingQueue<ILogRecord> flushQ;
    protected final LinkedBlockingQueue<ILogRecord> remoteJobsQ;
    private FileChannel fileChannel;
    private boolean stop;
    private final MutableTxnId reusableTxnId;
    private final DatasetId reusableDatasetId;

    public LogBuffer(ITransactionSubsystem txnSubsystem, int logPageSize, MutableLong flushLSN) {
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
        syncCommitQ = new LinkedBlockingQueue<>(logPageSize / ILogRecord.JOB_TERMINATE_LOG_SIZE);
        flushQ = new LinkedBlockingQueue<>();
        remoteJobsQ = new LinkedBlockingQueue<>();
        reusableTxnId = new MutableTxnId(-1);
        reusableDatasetId = new DatasetId(-1);
    }

    ////////////////////////////////////
    // LogAppender Methods
    ////////////////////////////////////

    @Override
    public void append(ILogRecord logRecord, long appendLsn) {
        logRecord.writeLogRecord(appendBuffer);

        if (logRecord.getLogSource() == LogSource.LOCAL && logRecord.getLogType() != LogType.FLUSH
                && logRecord.getLogType() != LogType.WAIT) {
            logRecord.getTxnCtx().setLastLSN(appendLsn);
        }

        synchronized (this) {
            appendOffset += logRecord.getLogSize();
            if (IS_DEBUG_MODE) {
                LOGGER.info("append()| appendOffset: " + appendOffset);
            }
            if (logRecord.getLogSource() == LogSource.LOCAL) {
                if (logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT
                        || logRecord.getLogType() == LogType.WAIT) {
                    logRecord.isFlushed(false);
                    syncCommitQ.add(logRecord);
                }
                if (logRecord.getLogType() == LogType.FLUSH) {
                    logRecord.isFlushed(false);
                    flushQ.add(logRecord);
                }
            } else if (logRecord.getLogSource() == LogSource.REMOTE && (logRecord.getLogType() == LogType.JOB_COMMIT
                    || logRecord.getLogType() == LogType.ABORT || logRecord.getLogType() == LogType.FLUSH)) {
                remoteJobsQ.add(logRecord);
            }
            this.notify();
        }
    }

    @Override
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

    @Override
    public synchronized void setFull() {
        this.full.set(true);
        this.notify();
    }

    @Override
    public void setLastPage() {
        this.isLastPage = true;
    }

    @Override
    public boolean hasSpace(int logSize) {
        return appendOffset + logSize <= logPageSize;
    }

    @Override
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
    public void flush(boolean stopping) {
        boolean interrupted = false;
        try {
            int endOffset;
            while (!full.get()) {
                try {
                    synchronized (this) {
                        if (appendOffset - flushOffset == 0 && !full.get()) {
                            if (IS_DEBUG_MODE) {
                                LOGGER.info("flush()| appendOffset: " + appendOffset + ", flushOffset: " + flushOffset
                                        + ", full: " + full.get());
                            }
                            if (stopping || stop) {
                                fileChannel.close();
                                return;
                            }
                            wait();
                        }
                        endOffset = appendOffset;
                    }
                internalFlush(flushOffset, endOffset);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            internalFlush(flushOffset, appendOffset);
            if (isLastPage) {
                fileChannel.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
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
            ITransactionContext txnCtx;
            LogRecord logRecord = logBufferTailReader.next();
            while (logRecord != null) {
                if (logRecord.getLogSource() == LogSource.LOCAL) {
                    if (logRecord.getLogType() == LogType.ENTITY_COMMIT) {
                        reusableTxnId.setId(logRecord.getTxnId());
                        reusableDatasetId.setId(logRecord.getDatasetId());
                        txnCtx = txnSubsystem.getTransactionManager().getTransactionContext(reusableTxnId);
                        txnSubsystem.getLockManager().unlock(reusableDatasetId, logRecord.getPKHashValue(),
                                LockMode.ANY, txnCtx);
                        txnCtx.notifyEntityCommitted();
                        if (txnSubsystem.getTransactionProperties().isCommitProfilerEnabled()) {
                            txnSubsystem.incrementEntityCommitCount();
                        }
                    } else if (logRecord.getLogType() == LogType.UPDATE) {
                        reusableTxnId.setId(logRecord.getTxnId());
                        txnCtx = txnSubsystem.getTransactionManager().getTransactionContext(reusableTxnId);
                        txnCtx.notifyUpdateCommitted(logRecord.getResourceId());
                    } else if (logRecord.getLogType() == LogType.JOB_COMMIT
                            || logRecord.getLogType() == LogType.ABORT) {
                        notifyJobTermination();
                    } else if (logRecord.getLogType() == LogType.FLUSH) {
                        notifyFlushTermination();
                    } else if (logRecord.getLogType() == LogType.WAIT) {
                        notifyWaitTermination();
                    }
                } else if (logRecord.getLogSource() == LogSource.REMOTE && (logRecord.getLogType() == LogType.JOB_COMMIT
                        || logRecord.getLogType() == LogType.ABORT || logRecord.getLogType() == LogType.FLUSH)) {
                    notifyReplicationTermination();
                }
                logRecord = logBufferTailReader.next();
            }
        }
    }

    public void notifyJobTermination() {
        notifyToSyncCommitQWaiter();
    }

    public void notifyWaitTermination() {
        notifyToSyncCommitQWaiter();
    }

    public void notifyToSyncCommitQWaiter() {
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

    public void notifyFlushTermination() throws ACIDException {
        LogRecord logRecord = null;
        while (logRecord == null) {
            try {
                logRecord = (LogRecord) flushQ.take();
            } catch (InterruptedException e) { //NOSONAR LogFlusher should survive interrupts
                //ignore
            }
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

    public void notifyReplicationTermination() {
        LogRecord logRecord = null;
        while (logRecord == null) {
            try {
                logRecord = (LogRecord) remoteJobsQ.take();
            } catch (InterruptedException e) { //NOSONAR LogFlusher should survive interrupts
                //ignore
            }
        }
        logRecord.isFlushed(true);
        IReplicationThread replicationThread = logRecord.getReplicationThread();

        if (replicationThread != null) {
            replicationThread.notifyLogReplicationRequester(logRecord);
        }
    }

    @Override
    public synchronized void stop() {
        stop = true;
        notifyAll();
    }

    @Override
    public int getLogPageSize() {
        return logPageSize;
    }

    private class MutableTxnId extends TxnId {

        public MutableTxnId(long id) {
            super(id);
        }

        public void setId(long id) {
            this.id = id;
        }
    }
}
