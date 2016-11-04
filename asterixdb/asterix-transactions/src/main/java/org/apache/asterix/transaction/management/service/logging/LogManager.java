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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ILogReader;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.LogManagerProperties;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.transactions.MutableLong;
import org.apache.asterix.common.transactions.TxnLogFile;
import org.apache.asterix.transaction.management.service.transaction.TransactionSubsystem;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;

public class LogManager implements ILogManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;// true
    private static final Logger LOGGER = Logger.getLogger(LogManager.class.getName());
    private final TransactionSubsystem txnSubsystem;

    private final LogManagerProperties logManagerProperties;
    protected final long logFileSize;
    protected final int logPageSize;
    private final int numLogPages;
    private final String logDir;
    private final String logFilePrefix;
    private final MutableLong flushLSN;
    private LinkedBlockingQueue<LogBuffer> emptyQ;
    private LinkedBlockingQueue<LogBuffer> flushQ;
    private LinkedBlockingQueue<LogBuffer> stashQ;
    protected final AtomicLong appendLSN;
    private FileChannel appendChannel;
    protected LogBuffer appendPage;
    private LogFlusher logFlusher;
    private Future<? extends Object> futureLogFlusher;
    private static final long SMALLEST_LOG_FILE_ID = 0;
    private static final int INITIAL_LOG_SIZE = 0;
    private final String nodeId;
    protected LinkedBlockingQueue<ILogRecord> flushLogsQ;
    private final FlushLogsLogger flushLogsLogger;
    private final HashMap<Long, Integer> txnLogFileId2ReaderCount = new HashMap<>();

    public LogManager(TransactionSubsystem txnSubsystem) {
        this.txnSubsystem = txnSubsystem;
        logManagerProperties = new LogManagerProperties(this.txnSubsystem.getTransactionProperties(),
                this.txnSubsystem.getId());
        logFileSize = logManagerProperties.getLogPartitionSize();
        logPageSize = logManagerProperties.getLogPageSize();
        numLogPages = logManagerProperties.getNumLogPages();
        logDir = logManagerProperties.getLogDir();
        logFilePrefix = logManagerProperties.getLogFilePrefix();
        flushLSN = new MutableLong();
        appendLSN = new AtomicLong();
        nodeId = txnSubsystem.getId();
        flushLogsQ = new LinkedBlockingQueue<>();
        flushLogsLogger = new FlushLogsLogger();
        initializeLogManager(SMALLEST_LOG_FILE_ID);
    }

    private void initializeLogManager(long nextLogFileId) {
        emptyQ = new LinkedBlockingQueue<>(numLogPages);
        flushQ = new LinkedBlockingQueue<>(numLogPages);
        stashQ = new LinkedBlockingQueue<>(numLogPages);
        for (int i = 0; i < numLogPages; i++) {
            emptyQ.offer(new LogBuffer(txnSubsystem, logPageSize, flushLSN));
        }
        appendLSN.set(initializeLogAnchor(nextLogFileId));
        flushLSN.set(appendLSN.get());
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("LogManager starts logging in LSN: " + appendLSN);
        }
        appendChannel = getFileChannel(appendLSN.get(), false);
        getAndInitNewPage(INITIAL_LOG_SIZE);
        logFlusher = new LogFlusher(this, emptyQ, flushQ, stashQ);
        futureLogFlusher = txnSubsystem.getAsterixAppRuntimeContextProvider().getThreadExecutor().submit(logFlusher);
        if (!flushLogsLogger.isAlive()) {
            txnSubsystem.getAsterixAppRuntimeContextProvider().getThreadExecutor().execute(flushLogsLogger);
        }
    }

    @Override
    public void log(ILogRecord logRecord) throws ACIDException {
        if (logRecord.getLogType() == LogType.FLUSH) {
            flushLogsQ.offer(logRecord);
            return;
        }
        appendToLogTail(logRecord);
    }

    protected void appendToLogTail(ILogRecord logRecord) throws ACIDException {
        syncAppendToLogTail(logRecord);

        if ((logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT
                || logRecord.getLogType() == LogType.WAIT) && !logRecord.isFlushed()) {
            synchronized (logRecord) {
                while (!logRecord.isFlushed()) {
                    try {
                        logRecord.wait();
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
            }
        }
    }

    protected synchronized void syncAppendToLogTail(ILogRecord logRecord) throws ACIDException {
        if (logRecord.getLogType() != LogType.FLUSH) {
            ITransactionContext txnCtx = logRecord.getTxnCtx();
            if (txnCtx.getTxnState() == ITransactionManager.ABORTED && logRecord.getLogType() != LogType.ABORT) {
                throw new ACIDException(
                        "Aborted job(" + txnCtx.getJobId() + ") tried to write non-abort type log record.");
            }
        }

        /**
         * To eliminate the case where the modulo of the next appendLSN = 0 (the next
         * appendLSN = the first LSN of the next log file), we do not allow a log to be
         * written at the last offset of the current file.
         */
        final int logSize = logRecord.getLogSize();
        // Make sure the log will not exceed the log file size
        if (getLogFileOffset(appendLSN.get()) + logSize >= logFileSize) {
            prepareNextLogFile();
            prepareNextPage(logSize);
        } else if (!appendPage.hasSpace(logSize)) {
            prepareNextPage(logSize);
        }
        appendPage.append(logRecord, appendLSN.get());

        if (logRecord.getLogType() == LogType.FLUSH) {
            logRecord.setLSN(appendLSN.get());
        }
        if (logRecord.isMarker()) {
            logRecord.logAppended(appendLSN.get());
        }
        appendLSN.addAndGet(logSize);
    }

    protected void prepareNextPage(int logSize) {
        appendPage.isFull(true);
        getAndInitNewPage(logSize);
    }

    protected void getAndInitNewPage(int logSize) {
        if (logSize > logPageSize) {
            // before creating a new page, we need to stash a normal sized page since our queues have fixed capacity
            appendPage = null;
            while (appendPage == null) {
                try {
                    appendPage = emptyQ.take();
                    stashQ.add(appendPage);
                } catch (InterruptedException e) {
                    //ignore
                }
            }
            // for now, alloc a new buffer for each large page
            // TODO: pool large pages??
            appendPage = new LogBuffer(txnSubsystem, logSize, flushLSN);
            appendPage.setFileChannel(appendChannel);
            flushQ.offer(appendPage);
        } else {
            appendPage = null;
            while (appendPage == null) {
                try {
                    appendPage = emptyQ.take();
                } catch (InterruptedException e) {
                    //ignore
                }
            }
            appendPage.reset();
            appendPage.setFileChannel(appendChannel);
            flushQ.offer(appendPage);
        }
    }

    protected void prepareNextLogFile() {
        // Make sure to flush whatever left in the log tail.
        appendPage.isFull(true);
        //wait until all log records have been flushed in the current file
        synchronized (flushLSN) {
            try {
                while (flushLSN.get() != appendLSN.get()) {
                    //notification will come from LogBuffer.internalFlush(.)
                    flushLSN.wait();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        //move appendLSN and flushLSN to the first LSN of the next log file
        appendLSN.addAndGet(logFileSize - getLogFileOffset(appendLSN.get()));
        flushLSN.set(appendLSN.get());
        appendChannel = getFileChannel(appendLSN.get(), true);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Created new txn log file with id(" + getLogFileId(appendLSN.get()) + ") starting with LSN = "
                    + appendLSN.get());
        }
        appendPage.isLastPage(true);
        //[Notice]
        //the current log file channel is closed if
        //LogBuffer.flush() completely flush the last page of the file.
    }

    @Override
    public ILogReader getLogReader(boolean isRecoveryMode) {
        return new LogReader(this, logFileSize, logPageSize, flushLSN, isRecoveryMode);
    }

    public LogManagerProperties getLogManagerProperties() {
        return logManagerProperties;
    }

    public TransactionSubsystem getTransactionSubsystem() {
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

    private long initializeLogAnchor(long nextLogFileId) {
        long fileId = 0;
        long offset = 0;
        File fileLogDir = new File(logDir);
        try {
            if (fileLogDir.exists()) {
                List<Long> logFileIds = getLogFileIds();
                if (logFileIds == null) {
                    fileId = nextLogFileId;
                    createFileIfNotExists(getLogFilePath(fileId));
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("created a log file: " + getLogFilePath(fileId));
                    }
                } else {
                    fileId = logFileIds.get(logFileIds.size() - 1);
                    File logFile = new File(getLogFilePath(fileId));
                    offset = logFile.length();
                }
            } else {
                fileId = nextLogFileId;
                createNewDirectory(logDir);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("created the log directory: " + logManagerProperties.getLogDir());
                }
                createFileIfNotExists(getLogFilePath(fileId));
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("created a log file: " + getLogFilePath(fileId));
                }
            }
        } catch (IOException ioe) {
            throw new IllegalStateException("Failed to initialize the log anchor", ioe);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("log file Id: " + fileId + ", offset: " + offset);
        }
        return logFileSize * fileId + offset;
    }

    public void renewLogFiles() throws IOException {
        terminateLogFlusher();
        long lastMaxLogFileId = deleteAllLogFiles();
        initializeLogManager(lastMaxLogFileId + 1);
    }

    @Override
    public void deleteOldLogFiles(long checkpointLSN) {
        Long checkpointLSNLogFileID = getLogFileId(checkpointLSN);
        List<Long> logFileIds = getLogFileIds();
        if (logFileIds != null) {
            //sort log files from oldest to newest
            Collections.sort(logFileIds);
            /**
             * At this point, any future LogReader should read from LSN >= checkpointLSN
             */
            synchronized (txnLogFileId2ReaderCount) {
                for (Long id : logFileIds) {
                    /**
                     * Stop deletion if:
                     * The log file which contains the checkpointLSN has been reached.
                     * The oldest log file being accessed by a LogReader has been reached.
                     */
                    if (id >= checkpointLSNLogFileID
                            || (txnLogFileId2ReaderCount.containsKey(id) && txnLogFileId2ReaderCount.get(id) > 0)) {
                        break;
                    }

                    //delete old log file
                    File file = new File(getLogFilePath(id));
                    file.delete();
                    txnLogFileId2ReaderCount.remove(id);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Deleted log file " + file.getAbsolutePath());
                    }
                }
            }
        }
    }

    private void terminateLogFlusher() {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Terminating LogFlusher thread ...");
        }
        logFlusher.terminate();
        try {
            futureLogFlusher.get();
        } catch (ExecutionException | InterruptedException e) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("---------- warning(begin): LogFlusher thread is terminated abnormally --------");
                e.printStackTrace();
                LOGGER.info("---------- warning(end)  : LogFlusher thread is terminated abnormally --------");
            }
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("LogFlusher thread is terminated.");
        }
    }

    private long deleteAllLogFiles() {
        if (appendChannel != null) {
            try {
                appendChannel.close();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to close a fileChannel of a log file");
            }
        }
        txnLogFileId2ReaderCount.clear();
        List<Long> logFileIds = getLogFileIds();
        if (logFileIds != null) {
            for (Long id : logFileIds) {
                File file = new File(getLogFilePath(id));
                if (!file.delete()) {
                    throw new IllegalStateException("Failed to delete a file: " + file.getAbsolutePath());
                }
            }
            return logFileIds.get(logFileIds.size() - 1);
        } else {
            throw new IllegalStateException("Couldn't find any log files.");
        }
    }

    public List<Long> getLogFileIds() {
        File fileLogDir = new File(logDir);
        String[] logFileNames = null;
        List<Long> logFileIds = null;
        if (fileLogDir.exists()) {
            logFileNames = fileLogDir.list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.startsWith(logFilePrefix)) {
                        return true;
                    }
                    return false;
                }
            });
            if (logFileNames != null && logFileNames.length != 0) {
                logFileIds = new ArrayList<>();
                for (String fileName : logFileNames) {
                    logFileIds.add(Long.parseLong(fileName.substring(logFilePrefix.length() + 1)));
                }
                Collections.sort(logFileIds, new Comparator<Long>() {
                    @Override
                    public int compare(Long arg0, Long arg1) {
                        return arg0.compareTo(arg1);
                    }
                });
            }
        }
        return logFileIds;
    }

    public String getLogFilePath(long fileId) {
        return logDir + File.separator + logFilePrefix + "_" + fileId;
    }

    public long getLogFileOffset(long lsn) {
        return lsn % logFileSize;
    }

    public long getLogFileId(long lsn) {
        return lsn / logFileSize;
    }

    private static boolean createFileIfNotExists(String path) throws IOException {
        File file = new File(path);
        File parentFile = file.getParentFile();
        if (parentFile != null) {
            parentFile.mkdirs();
        }
        return file.createNewFile();
    }

    private static boolean createNewDirectory(String path) {
        return (new File(path)).mkdir();
    }

    private FileChannel getFileChannel(long lsn, boolean create) {
        FileChannel newFileChannel = null;
        try {
            long fileId = getLogFileId(lsn);
            String logFilePath = getLogFilePath(fileId);
            File file = new File(logFilePath);
            if (create) {
                if (!file.createNewFile()) {
                    throw new IllegalStateException();
                }
            } else {
                if (!file.exists()) {
                    throw new IllegalStateException();
                }
            }
            RandomAccessFile raf = new RandomAccessFile(new File(logFilePath), "rw");
            newFileChannel = raf.getChannel();
            newFileChannel.position(getLogFileOffset(lsn));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return newFileChannel;
    }

    @Override
    public long getReadableSmallestLSN() {
        List<Long> logFileIds = getLogFileIds();
        if (logFileIds != null) {
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
    public void renewLogFilesAndStartFromLSN(long LSNtoStartFrom) throws IOException {
        terminateLogFlusher();
        deleteAllLogFiles();
        long newLogFile = getLogFileId(LSNtoStartFrom);
        initializeLogManager(newLogFile + 1);
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
        touchLogFile(fileId);
        return logFile;
    }

    @Override
    public void closeLogFile(TxnLogFile logFileRef, FileChannel fileChannel) throws IOException {
        if (!fileChannel.isOpen()) {
            throw new IllegalStateException("File channel is not open");
        }
        fileChannel.close();
        untouchLogFile(logFileRef.getLogFileId());
    }

    private void touchLogFile(long fileId) {
        synchronized (txnLogFileId2ReaderCount) {
            if (txnLogFileId2ReaderCount.containsKey(fileId)) {
                txnLogFileId2ReaderCount.put(fileId, txnLogFileId2ReaderCount.get(fileId) + 1);
            } else {
                txnLogFileId2ReaderCount.put(fileId, 1);
            }
        }
    }

    private void untouchLogFile(long fileId) {
        synchronized (txnLogFileId2ReaderCount) {
            if (txnLogFileId2ReaderCount.containsKey(fileId)) {
                int newReaderCount = txnLogFileId2ReaderCount.get(fileId) - 1;
                if (newReaderCount < 0) {
                    throw new IllegalStateException(
                            "Invalid log file reader count (ID=" + fileId + ", count: " + newReaderCount + ")");
                }
                txnLogFileId2ReaderCount.put(fileId, newReaderCount);
            } else {
                throw new IllegalStateException("Trying to close log file id(" + fileId + ") which was not opened.");
            }
        }
    }

    /**
     * This class is used to log FLUSH logs.
     * FLUSH logs are flushed on a different thread to avoid a possible deadlock in LogBuffer batchUnlock which calls PrimaryIndexOpeartionTracker.completeOperation
     * The deadlock happens when PrimaryIndexOpeartionTracker.completeOperation results in generating a FLUSH log and there are no empty log buffers available to log it.
     */
    private class FlushLogsLogger extends Thread {
        private ILogRecord logRecord;

        @Override
        public void run() {
            while (true) {
                try {
                    logRecord = flushLogsQ.take();
                    appendToLogTail(logRecord);
                } catch (ACIDException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        }
    }
}

class LogFlusher implements Callable<Boolean> {
    private static final Logger LOGGER = Logger.getLogger(LogFlusher.class.getName());
    private final static LogBuffer POISON_PILL = new LogBuffer(null, ILogRecord.JOB_TERMINATE_LOG_SIZE, null);
    private final LogManager logMgr;//for debugging
    private final LinkedBlockingQueue<LogBuffer> emptyQ;
    private final LinkedBlockingQueue<LogBuffer> flushQ;
    private final LinkedBlockingQueue<LogBuffer> stashQ;
    private LogBuffer flushPage;
    private final AtomicBoolean isStarted;
    private final AtomicBoolean terminateFlag;

    public LogFlusher(LogManager logMgr, LinkedBlockingQueue<LogBuffer> emptyQ, LinkedBlockingQueue<LogBuffer> flushQ,
            LinkedBlockingQueue<LogBuffer> stashQ) {
        this.logMgr = logMgr;
        this.emptyQ = emptyQ;
        this.flushQ = flushQ;
        this.stashQ = stashQ;
        flushPage = null;
        isStarted = new AtomicBoolean(false);
        terminateFlag = new AtomicBoolean(false);

    }

    public void terminate() {
        //make sure the LogFlusher thread started before terminating it.
        synchronized (isStarted) {
            while (!isStarted.get()) {
                try {
                    isStarted.wait();
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        }

        terminateFlag.set(true);
        if (flushPage != null) {
            synchronized (flushPage) {
                flushPage.isStop(true);
                flushPage.notify();
            }
        }
        //[Notice]
        //The return value doesn't need to be checked
        //since terminateFlag will trigger termination if the flushQ is full.
        flushQ.offer(POISON_PILL);
    }

    @Override
    public Boolean call() {
        synchronized (isStarted) {
            isStarted.set(true);
            isStarted.notify();
        }
        try {
            while (true) {
                flushPage = null;
                try {
                    flushPage = flushQ.take();
                    if (flushPage == POISON_PILL || terminateFlag.get()) {
                        return true;
                    }
                } catch (InterruptedException e) {
                    if (flushPage == null) {
                        continue;
                    }
                }
                flushPage.flush();
                emptyQ.offer(flushPage.getLogPageSize() == logMgr.getLogPageSize() ? flushPage : stashQ.remove());
            }
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("-------------------------------------------------------------------------");
                LOGGER.info("LogFlusher is terminating abnormally. System is in unusalbe state.");
                LOGGER.info("-------------------------------------------------------------------------");
            }
            e.printStackTrace();
            throw e;
        }
    }
}
