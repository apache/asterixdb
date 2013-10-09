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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.AsterixThreadExecutor;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.ILogManager;
import edu.uci.ics.asterix.common.transactions.ILogReader;
import edu.uci.ics.asterix.common.transactions.ILogRecord;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager;
import edu.uci.ics.asterix.common.transactions.LogManagerProperties;
import edu.uci.ics.asterix.common.transactions.MutableLong;
import edu.uci.ics.asterix.transaction.management.service.locking.LockManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;

public class LogManager implements ILogManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;// true
    private static final Logger LOGGER = Logger.getLogger(LogManager.class.getName());
    private final TransactionSubsystem txnSubsystem;
    private final LogManagerProperties logManagerProperties;
    private final long logFileSize;
    private final int logPageSize;
    private final int numLogPages;
    private final String logDir;
    private final String logFilePrefix;
    private final MutableLong flushLSN;
    private LinkedBlockingQueue<LogPage> emptyQ;
    private LinkedBlockingQueue<LogPage> flushQ;
    private long appendLSN;
    private FileChannel appendChannel;
    private LogPage appendPage;
    private LogFlusher logFlusher;
    private Future<Object> futureLogFlusher;

    public LogManager(TransactionSubsystem txnSubsystem) throws ACIDException {
        this.txnSubsystem = txnSubsystem;
        logManagerProperties = new LogManagerProperties(this.txnSubsystem.getTransactionProperties(),
                this.txnSubsystem.getId());
        logFileSize = logManagerProperties.getLogPartitionSize();
        logPageSize = logManagerProperties.getLogPageSize();
        numLogPages = logManagerProperties.getNumLogPages();
        logDir = logManagerProperties.getLogDir();
        logFilePrefix = logManagerProperties.getLogFilePrefix();
        flushLSN = new MutableLong();
        initializeLogManager(0);
    }

    private void initializeLogManager(long nextLogFileId) {
        emptyQ = new LinkedBlockingQueue<LogPage>(numLogPages);
        flushQ = new LinkedBlockingQueue<LogPage>(numLogPages);
        for (int i = 0; i < numLogPages; i++) {
            emptyQ.offer(new LogPage((LockManager) txnSubsystem.getLockManager(), logPageSize, flushLSN));
        }
        appendLSN = initializeLogAnchor(nextLogFileId);
        flushLSN.set(appendLSN);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("LogManager starts logging in LSN: " + appendLSN);
        }
        appendChannel = getFileChannel(appendLSN, false);
        getAndInitNewPage();
        logFlusher = new LogFlusher(this, emptyQ, flushQ);
        futureLogFlusher = AsterixThreadExecutor.INSTANCE.submit(logFlusher);
    }

    @Override
    public void log(ILogRecord logRecord) throws ACIDException {
        if (logRecord.getLogSize() > logPageSize) {
            throw new IllegalStateException();
        }
        syncLog(logRecord);
        if ((logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT)
                && !logRecord.isFlushed()) {
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

    private synchronized void syncLog(ILogRecord logRecord) throws ACIDException {
        ITransactionContext txnCtx = logRecord.getTxnCtx();
        if (txnCtx.getTxnState() == ITransactionManager.ABORTED && logRecord.getLogType() != LogType.ABORT) {
            throw new ACIDException("Aborted job(" + txnCtx.getJobId() + ") tried to write non-abort type log record.");
        }
        if (getLogFileOffset(appendLSN) + logRecord.getLogSize() > logFileSize) {
            prepareNextLogFile();
            appendPage.isFull(true);
            getAndInitNewPage();
        } else if (!appendPage.hasSpace(logRecord.getLogSize())) {
            appendPage.isFull(true);
            getAndInitNewPage();
        }
        if (logRecord.getLogType() == LogType.UPDATE) {
            logRecord.setPrevLSN(txnCtx.getLastLSN());
        }
        appendPage.append(logRecord, appendLSN);
        appendLSN += logRecord.getLogSize();
    }

    private void getAndInitNewPage() {
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

    private void prepareNextLogFile() {
        appendLSN += logFileSize - getLogFileOffset(appendLSN);
        appendChannel = getFileChannel(appendLSN, true);
        appendPage.isLastPage(true);
        //[Notice]
        //the current log file channel is closed if 
        //LogPage.flush() completely flush the last page of the file.
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

    public long getAppendLSN() {
        return appendLSN;
    }

    @Override
    public void start() {
        // no op
    }

    @Override
    public void stop(boolean dumpState, OutputStream os) {
        terminateLogFlusher();
        if (dumpState) {
            // #. dump Configurable Variables
            dumpConfVars(os);

            // #. dump LSNInfo
            dumpLSNInfo(os);

            try {
                os.flush();
            } catch (IOException e) {
                // ignore
            }
        }
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

    public MutableLong getFlushLSN() {
        return flushLSN;
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

    public void renewLogFiles() {
        terminateLogFlusher();
        long lastMaxLogFileId = deleteAllLogFiles();
        initializeLogManager(lastMaxLogFileId + 1);
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
        List<Long> logFileIds = getLogFileIds();
        for (Long id : logFileIds) {
            File file = new File(getLogFilePath(id));
            if (!file.delete()) {
                throw new IllegalStateException("Failed to delete a file: " + file.getAbsolutePath());
            }
        }
        return logFileIds.get(logFileIds.size() - 1);
    }

    private List<Long> getLogFileIds() {
        File fileLogDir = new File(logDir);
        String[] logFileNames = null;
        List<Long> logFileIds = null;
        if (fileLogDir.exists()) {
            logFileNames = fileLogDir.list(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    if (name.startsWith(logFilePrefix)) {
                        return true;
                    }
                    return false;
                }
            });
            if (logFileNames != null && logFileNames.length != 0) {
                logFileIds = new ArrayList<Long>();
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

    private boolean createFileIfNotExists(String path) throws IOException {
        File file = new File(path);
        File parentFile = file.getParentFile();
        if (parentFile != null) {
            parentFile.mkdirs();
        }
        return file.createNewFile();
    }

    private boolean createNewDirectory(String path) throws IOException {
        return (new File(path)).mkdir();
    }

    public FileChannel getFileChannel(long lsn, boolean create) {
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

    public long getReadableSmallestLSN() {
        List<Long> logFileIds = getLogFileIds();
        return logFileIds.get(0) * logFileSize;
    }
}

class LogFlusher implements Callable<Boolean> {
    private static final Logger LOGGER = Logger.getLogger(LogFlusher.class.getName());
    private final static LogPage POISON_PILL = new LogPage(null, ILogRecord.JOB_TERMINATE_LOG_SIZE, null);
    private final LogManager logMgr;//for debugging
    private final LinkedBlockingQueue<LogPage> emptyQ;
    private final LinkedBlockingQueue<LogPage> flushQ;
    private LogPage flushPage;
    private final AtomicBoolean isStarted;
    private final AtomicBoolean terminateFlag;

    public LogFlusher(LogManager logMgr, LinkedBlockingQueue<LogPage> emptyQ, LinkedBlockingQueue<LogPage> flushQ) {
        this.logMgr = logMgr;
        this.emptyQ = emptyQ;
        this.flushQ = flushQ;
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
                emptyQ.offer(flushPage);
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