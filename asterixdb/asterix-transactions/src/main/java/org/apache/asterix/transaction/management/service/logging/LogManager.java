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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.ILogBuffer;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ILogReader;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.LogConstants;
import org.apache.asterix.common.transactions.LogManagerProperties;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.transactions.MutableLong;
import org.apache.asterix.common.transactions.TxnLogFile;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

public class LogManager implements ILogManager, ILifeCycleComponent {

    private static final Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger();
    private static final long SMALLEST_LOG_FILE_ID = 0;
    private static final int INITIAL_LOG_SIZE = 0;
    private static final boolean IS_DEBUG_MODE = false;

    private final ITransactionSubsystem txnSubsystem;
    private final LogManagerProperties logManagerProperties;
    private final int numLogPages;
    private final String logDir;
    private final String logFilePrefix;
    private final MutableLong flushLSN;
    private final String nodeId;
    private final long logFileSize;
    private final int logPageSize;
    private final AtomicLong appendLSN;
    private final long maxLogRecordSize;

    private LinkedBlockingQueue<ILogBuffer> emptyQ;
    private LinkedBlockingQueue<ILogBuffer> flushQ;
    private LinkedBlockingQueue<ILogBuffer> stashQ;
    private FileChannel appendChannel;
    private ILogBuffer appendPage;
    private LogFlusher logFlusher;
    private Future<?> futureLogFlusher;
    private LinkedBlockingQueue<ILogRecord> flushLogsQ;
    private long currentLogFileId;

    public LogManager(ITransactionSubsystem txnSubsystem) {
        this.txnSubsystem = txnSubsystem;
        logManagerProperties =
                new LogManagerProperties(this.txnSubsystem.getTransactionProperties(), this.txnSubsystem.getId());
        logFileSize = logManagerProperties.getLogPartitionSize();
        maxLogRecordSize = logFileSize - 1;
        logPageSize = logManagerProperties.getLogPageSize();
        numLogPages = logManagerProperties.getNumLogPages();
        logDir = logManagerProperties.getLogDir();
        logFilePrefix = logManagerProperties.getLogFilePrefix();
        flushLSN = new MutableLong();
        appendLSN = new AtomicLong();
        nodeId = txnSubsystem.getId();
        flushLogsQ = new LinkedBlockingQueue<>();
        txnSubsystem.getApplicationContext().getThreadExecutor().execute(new FlushLogsLogger());
        final long onDiskMaxLogFileId = getOnDiskMaxLogFileId();
        initializeLogManager(onDiskMaxLogFileId);
    }

    private void initializeLogManager(long nextLogFileId) {
        emptyQ = new LinkedBlockingQueue<>(numLogPages);
        flushQ = new LinkedBlockingQueue<>(numLogPages);
        stashQ = new LinkedBlockingQueue<>(numLogPages);
        for (int i = 0; i < numLogPages; i++) {
            emptyQ.add(new LogBuffer(txnSubsystem, logPageSize, flushLSN));
        }
        appendLSN.set(initializeLogAnchor(nextLogFileId));
        flushLSN.set(appendLSN.get());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("LogManager starts logging in LSN: " + appendLSN);
        }
        try {
            setLogPosition(appendLSN.get());
        } catch (IOException e) {
            throw new ACIDException(e);
        }
        initNewPage(INITIAL_LOG_SIZE);
        logFlusher = new LogFlusher(this, emptyQ, flushQ, stashQ);
        futureLogFlusher =
                ((ExecutorService) txnSubsystem.getApplicationContext().getThreadExecutor()).submit(logFlusher);
    }

    @Override
    public void log(ILogRecord logRecord) {
        if (!logToFlushQueue(logRecord)) {
            appendToLogTail(logRecord);
        }
    }

    @SuppressWarnings("squid:S2445")
    protected boolean logToFlushQueue(ILogRecord logRecord) {
        //Remote flush logs do not need to be flushed separately since they may not trigger local flush
        if ((logRecord.getLogType() == LogType.FLUSH && logRecord.getLogSource() == LogSource.LOCAL)
                || logRecord.getLogType() == LogType.WAIT_FOR_FLUSHES) {
            logRecord.isFlushed(false);
            flushLogsQ.add(logRecord);
            if (logRecord.getLogType() == LogType.WAIT_FOR_FLUSHES) {
                InvokeUtil.doUninterruptibly(() -> {
                    synchronized (logRecord) {
                        while (!logRecord.isFlushed()) {
                            logRecord.wait();
                        }
                    }
                });
            }
            return true;
        }
        return false;
    }

    @SuppressWarnings("squid:S2445")
    protected void appendToLogTail(ILogRecord logRecord) {
        syncAppendToLogTail(logRecord);
        if (waitForFlush(logRecord) && !logRecord.isFlushed()) {
            InvokeUtil.doUninterruptibly(() -> {
                synchronized (logRecord) {
                    while (!logRecord.isFlushed()) {
                        logRecord.wait();
                    }
                }
            });
        }
    }

    protected static boolean waitForFlush(ILogRecord logRecord) {
        final byte logType = logRecord.getLogType();
        return logType == LogType.JOB_COMMIT || logType == LogType.ABORT || logType == LogType.WAIT;
    }

    synchronized void syncAppendToLogTail(ILogRecord logRecord) {
        if (logRecord.getLogSource() == LogSource.LOCAL && logRecord.getLogType() != LogType.FLUSH
                && logRecord.getLogType() != LogType.WAIT && logRecord.getLogType() != LogType.WAIT_FOR_FLUSHES) {
            ITransactionContext txnCtx = logRecord.getTxnCtx();
            if (txnCtx.getTxnState() == ITransactionManager.ABORTED && logRecord.getLogType() != LogType.ABORT) {
                throw new ACIDException(
                        "Aborted txn(" + txnCtx.getTxnId() + ") tried to write non-abort type log record.");
            }
        }
        final int logSize = logRecord.getLogSize();
        ensureSpace(logSize);
        if (logRecord.getLogType() == LogType.FLUSH) {
            logRecord.setLSN(appendLSN.get());
        }
        appendPage.append(logRecord, appendLSN.get());
        if (logRecord.isMarker()) {
            logRecord.logAppended(appendLSN.get());
        }
        appendLSN.addAndGet(logSize);
    }

    private void ensureSpace(int logSize) {
        if (!fileHasSpace(logSize)) {
            ensureLastPageFlushed();
            prepareNextLogFile();
        }
        if (!appendPage.hasSpace(logSize)) {
            prepareNextPage(logSize);
        }
    }

    private boolean fileHasSpace(int logSize) {
        if (logSize > maxLogRecordSize) {
            throw new ACIDException("Maximum log record size of (" + maxLogRecordSize + ") exceeded");
        }
        /*
         * To eliminate the case where the modulo of the next appendLSN = 0 (the next
         * appendLSN = the first LSN of the next log file), we do not allow a log to be
         * written at the last offset of the current file.
         */
        return getLogFileOffset(appendLSN.get()) + logSize < logFileSize;
    }

    private void prepareNextPage(int logSize) {
        appendPage.setFull();
        initNewPage(logSize);
    }

    private void initNewPage(int logSize) {
        boolean largePage = logSize > logPageSize;
        // if a new large page will be allocated, we need to stash a normal sized page
        // since our queues have fixed capacity
        ensureAvailablePage(largePage);
        if (largePage) {
            // for now, alloc a new buffer for each large page
            // TODO: pool large pages??
            appendPage = new LogBuffer(txnSubsystem, logSize, flushLSN);
        } else {
            appendPage.reset();
        }
        appendPage.setFileChannel(appendChannel);
        flushQ.add(appendPage);
    }

    private void ensureAvailablePage(boolean stash) {
        final ILogBuffer currentPage = appendPage;
        appendPage = null;
        try {
            appendPage = emptyQ.take();
            if (stash) {
                stashQ.add(appendPage);
            }
        } catch (InterruptedException e) {
            appendPage = currentPage;
            Thread.currentThread().interrupt();
            throw new ACIDException(e);
        }
    }

    private void prepareNextLogFile() {
        final long nextFileBeginLsn = getNextFileFirstLsn();
        try {
            closeCurrentLogFile();
            createNextLogFile();
            InvokeUtil.doIoUninterruptibly(() -> setLogPosition(nextFileBeginLsn));
            // move appendLSN and flushLSN to the first LSN of the next log file
            // only after the file was created and the channel was positioned successfully
            appendLSN.set(nextFileBeginLsn);
            flushLSN.set(nextFileBeginLsn);
            LOGGER.info("Created new txn log file with id({}) starting with LSN = {}", currentLogFileId,
                    nextFileBeginLsn);
        } catch (IOException e) {
            throw new ACIDException(e);
        }
    }

    private long getNextFileFirstLsn() {
        // add the remaining space in the current file
        return appendLSN.get() + (logFileSize - getLogFileOffset(appendLSN.get()));
    }

    private void ensureLastPageFlushed() {
        // Make sure to flush whatever left in the log tail.
        appendPage.setFull();
        synchronized (flushLSN) {
            while (flushLSN.get() != appendLSN.get()) {
                // notification will come from LogBuffer.internalFlush(.)
                try {
                    flushLSN.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ACIDException(e);
                }
            }
        }
    }

    @Override
    public ILogReader getLogReader(boolean isRecoveryMode) {
        return new LogReader(this, logFileSize, logPageSize, flushLSN, isRecoveryMode);
    }

    public LogManagerProperties getLogManagerProperties() {
        return logManagerProperties;
    }

    public ITransactionSubsystem getTransactionSubsystem() {
        return txnSubsystem;
    }

    @Override
    public long getAppendLSN() {
        return appendLSN.get();
    }

    @Override
    public void start() {
        // no op
    }

    @Override
    public void stop(boolean dumpState, OutputStream os) {
        terminateLogFlusher();
        closeCurrentLogFile();
        if (dumpState) {
            dumpState(os);
        }
    }

    @Override
    public void dumpState(OutputStream os) {
        // #. dump Configurable Variables
        dumpConfVars(os);

        // #. dump LSNInfo
        dumpLSNInfo(os);
    }

    private void dumpConfVars(OutputStream os) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("\n>>dump_begin\t>>----- [ConfVars] -----");
            sb.append(logManagerProperties.toString());
            sb.append("\n>>dump_end\t>>----- [ConfVars] -----\n");
            os.write(sb.toString().getBytes());
        } catch (Exception e) {
            // ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
    }

    private void dumpLSNInfo(OutputStream os) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("\n>>dump_begin\t>>----- [LSNInfo] -----");
            sb.append("\nappendLsn: " + appendLSN);
            sb.append("\nflushLsn: " + flushLSN.get());
            sb.append("\n>>dump_end\t>>----- [LSNInfo] -----\n");
            os.write(sb.toString().getBytes());
        } catch (Exception e) {
            // ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
    }

    private long initializeLogAnchor(long fileId) {
        final String logFilePath = getLogFilePath(fileId);
        createFileIfNotExists(logFilePath);
        final File logFile = new File(logFilePath);
        long offset = logFile.length();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("initializing log anchor with log file Id: {} at offset: {}", fileId, offset);
        }
        return getLogFileFirstLsn(fileId) + offset;
    }

    @Override
    public void renewLogFiles() {
        terminateLogFlusher();
        closeCurrentLogFile();
        long nextLogFileId = getNextLogFileId();
        createFileIfNotExists(getLogFilePath(nextLogFileId));
        final long logFileFirstLsn = getLogFileFirstLsn(nextLogFileId);
        deleteOldLogFiles(logFileFirstLsn);
        initializeLogManager(nextLogFileId);
    }

    @Override
    public void deleteOldLogFiles(long checkpointLSN) {
        Long checkpointLSNLogFileID = getLogFileId(checkpointLSN);
        List<Long> logFileIds = getOrderedLogFileIds();
        if (!logFileIds.isEmpty()) {
            //sort log files from oldest to newest
            Collections.sort(logFileIds);
            // remove the last one not to delete the current log file
            logFileIds.remove(logFileIds.size() - 1);
            /**
             * At this point, any future LogReader should read from LSN >= checkpointLSN
             */
            for (Long id : logFileIds) {
                /**
                 * Stop deletion if:
                 * The log file which contains the checkpointLSN has been reached.
                 * The oldest log file being accessed by a LogReader has been reached.
                 */
                if (id >= checkpointLSNLogFileID) {
                    break;
                }
                //delete old log file
                File file = new File(getLogFilePath(id));
                file.delete();
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Deleted log file " + file.getAbsolutePath());
                }
            }
        }
    }

    private void terminateLogFlusher() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Terminating LogFlusher thread ...");
        }
        logFlusher.terminate();
        try {
            futureLogFlusher.get();
        } catch (ExecutionException | InterruptedException e) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("---------- warning(begin): LogFlusher thread is terminated abnormally --------");
                e.printStackTrace();
                LOGGER.info("---------- warning(end)  : LogFlusher thread is terminated abnormally --------");
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("LogFlusher thread is terminated.");
        }
    }

    public List<Long> getOrderedLogFileIds() {
        File fileLogDir = new File(logDir);
        String[] logFileNames = null;
        List<Long> logFileIds = null;
        if (!fileLogDir.exists()) {
            LOGGER.log(Level.INFO, "log dir " + logDir + " doesn't exist.  returning empty list");
            return Collections.emptyList();
        }
        if (!fileLogDir.isDirectory()) {
            throw new IllegalStateException("log dir " + logDir + " exists but it is not a directory");
        }
        logFileNames = fileLogDir.list((dir, name) -> name.startsWith(logFilePrefix));
        if (logFileNames == null) {
            throw new IllegalStateException("listing of log dir (" + logDir + ") files returned null. "
                    + "Either an IO error occurred or the dir was just deleted by another process/thread");
        }
        if (logFileNames.length == 0) {
            LOGGER.log(Level.INFO, "the log dir (" + logDir + ") is empty. returning empty list");
            return Collections.emptyList();
        }
        logFileIds = new ArrayList<>();
        for (String fileName : logFileNames) {
            logFileIds.add(Long.parseLong(fileName.substring(logFilePrefix.length() + 1)));
        }
        logFileIds.sort(Long::compareTo);
        return logFileIds;
    }

    private String getLogFilePath(long fileId) {
        return logDir + File.separator + logFilePrefix + "_" + fileId;
    }

    private long getLogFileOffset(long lsn) {
        return lsn % logFileSize;
    }

    public long getLogFileId(long lsn) {
        return lsn / logFileSize;
    }

    private static void createFileIfNotExists(String path) {
        try {
            File file = new File(path);
            if (file.exists()) {
                return;
            }
            File parentFile = file.getParentFile();
            if (parentFile != null) {
                parentFile.mkdirs();
            }
            Files.createFile(file.toPath());
            LOGGER.info("Created log file {}", path);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create file in " + path, e);
        }
    }

    private void createNextLogFile() throws IOException {
        final long nextFileBeginLsn = getNextFileFirstLsn();
        final long fileId = getLogFileId(nextFileBeginLsn);
        final Path nextFilePath = Paths.get(getLogFilePath(fileId));
        if (nextFilePath.toFile().exists()) {
            LOGGER.warn("Ignored create log file {} since file already exists", nextFilePath.toString());
            return;
        }
        Files.createFile(nextFilePath);
    }

    private void setLogPosition(long lsn) throws IOException {
        final long fileId = getLogFileId(lsn);
        final Path targetFilePath = Paths.get(getLogFilePath(fileId));
        final long targetPosition = getLogFileOffset(lsn);
        final RandomAccessFile raf = new RandomAccessFile(targetFilePath.toFile(), "rw"); // NOSONAR closed when full
        appendChannel = raf.getChannel();
        appendChannel.position(targetPosition);
        currentLogFileId = fileId;
    }

    private void closeCurrentLogFile() {
        if (appendChannel != null && appendChannel.isOpen()) {
            try {
                LOGGER.info("closing current log file with id({})", currentLogFileId);
                appendChannel.close();
            } catch (IOException e) {
                LOGGER.error(() -> "failed to close log file with id(" + currentLogFileId + ")", e);
                throw new ACIDException(e);
            }
        }
    }

    @Override
    public long getReadableSmallestLSN() {
        List<Long> logFileIds = getOrderedLogFileIds();
        if (!logFileIds.isEmpty()) {
            return logFileIds.get(0) * logFileSize;
        } else {
            throw new IllegalStateException("Couldn't find any log files.");
        }
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public int getLogPageSize() {
        return logPageSize;
    }

    @Override
    public void setReplicationManager(IReplicationManager replicationManager) {
        throw new IllegalStateException("This log manager does not support replication");
    }

    @Override
    public int getNumLogPages() {
        return numLogPages;
    }

    @Override
    public TxnLogFile getLogFile(long LSN) throws IOException {
        long fileId = getLogFileId(LSN);
        String logFilePath = getLogFilePath(fileId);
        File file = new File(logFilePath);
        if (!file.exists()) {
            throw new IOException("Log file with id(" + fileId + ") was not found. Requested LSN: " + LSN);
        }
        RandomAccessFile raf = new RandomAccessFile(new File(logFilePath), "r");
        FileChannel newFileChannel = raf.getChannel();
        TxnLogFile logFile = new TxnLogFile(this, newFileChannel, fileId, fileId * logFileSize);
        return logFile;
    }

    @Override
    public void closeLogFile(TxnLogFile logFileRef, FileChannel fileChannel) throws IOException {
        if (!fileChannel.isOpen()) {
            LOGGER.warn(() -> "Closing log file with id(" + logFileRef.getLogFileId() + ") with a closed channel.");
        }
        fileChannel.close();
    }

    private long getNextLogFileId() {
        return getOnDiskMaxLogFileId() + 1;
    }

    private long getLogFileFirstLsn(long logFileId) {
        return logFileId * logFileSize;
    }

    private long getOnDiskMaxLogFileId() {
        final List<Long> logFileIds = getOrderedLogFileIds();
        if (logFileIds.isEmpty()) {
            return SMALLEST_LOG_FILE_ID;
        }
        return logFileIds.get(logFileIds.size() - 1);
    }

    /**
     * This class is used to log FLUSH logs.
     * FLUSH logs are flushed on a different thread to avoid a possible deadlock in {@link LogBuffer} batchUnlock
     * which calls {@link org.apache.asterix.common.context.PrimaryIndexOperationTracker} completeOperation. The
     * deadlock happens when completeOperation generates a FLUSH log and there are no empty log buffers available
     * to log it.
     */
    private class FlushLogsLogger implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final ILogRecord logRecord = flushLogsQ.take();
                    appendToLogTail(logRecord);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

class LogFlusher implements Callable<Boolean> {
    private static final Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger();
    private static final ILogBuffer POISON_PILL = new LogBuffer(null, LogConstants.JOB_TERMINATE_LOG_SIZE, null);
    private final LogManager logMgr;//for debugging
    private final LinkedBlockingQueue<ILogBuffer> emptyQ;
    private final LinkedBlockingQueue<ILogBuffer> flushQ;
    private final LinkedBlockingQueue<ILogBuffer> stashQ;
    private volatile ILogBuffer flushPage;
    private volatile boolean stopping;
    private final Semaphore started;

    LogFlusher(LogManager logMgr, LinkedBlockingQueue<ILogBuffer> emptyQ, LinkedBlockingQueue<ILogBuffer> flushQ,
            LinkedBlockingQueue<ILogBuffer> stashQ) {
        this.logMgr = logMgr;
        this.emptyQ = emptyQ;
        this.flushQ = flushQ;
        this.stashQ = stashQ;
        this.started = new Semaphore(0);
    }

    public void terminate() {
        // make sure the LogFlusher thread started before terminating it.
        InvokeUtil.doUninterruptibly(started::acquire);
        stopping = true;

        // we must tell any active flush, if any, to stop
        final ILogBuffer currentFlushPage = this.flushPage;
        if (currentFlushPage != null) {
            currentFlushPage.stop();
        }
        // finally we put a POISON_PILL onto the flushQ to indicate to the flusher it is time to exit
        InvokeUtil.doUninterruptibly(() -> flushQ.put(POISON_PILL));
    }

    @Override
    public Boolean call() {
        started.release();
        boolean interrupted = false;
        try {
            while (true) {
                flushPage = null;
                interrupted = InvokeUtil.doUninterruptiblyGet(() -> flushPage = flushQ.take()) || interrupted;
                if (flushPage == POISON_PILL) {
                    return true;
                }
                flushPage.flush(stopping);

                // TODO(mblow): recycle large pages
                emptyQ.add(flushPage.getLogPageSize() == logMgr.getLogPageSize() ? flushPage : stashQ.remove());
            }
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "LogFlusher is terminating abnormally. System is in unusable state; halting", e);
            ExitUtil.halt(ExitUtil.EC_TXN_LOG_FLUSHER_FAILURE);
            throw new AssertionError("not reachable");
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
