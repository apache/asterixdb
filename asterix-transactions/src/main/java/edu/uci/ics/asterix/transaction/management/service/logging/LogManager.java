/*
 * Copyright 2009-2012 by The Regents of the University of California
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.AsterixThreadExecutor;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.FileBasedBuffer;
import edu.uci.ics.asterix.common.transactions.FileUtil;
import edu.uci.ics.asterix.common.transactions.IFileBasedBuffer;
import edu.uci.ics.asterix.common.transactions.ILogCursor;
import edu.uci.ics.asterix.common.transactions.ILogFilter;
import edu.uci.ics.asterix.common.transactions.ILogManager;
import edu.uci.ics.asterix.common.transactions.ILogRecordHelper;
import edu.uci.ics.asterix.common.transactions.ILogger;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.LogManagerProperties;
import edu.uci.ics.asterix.common.transactions.LogicalLogLocator;
import edu.uci.ics.asterix.common.transactions.PhysicalLogLocator;
import edu.uci.ics.asterix.common.transactions.ReusableLogContentObject;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;

public class LogManager implements ILogManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;//true
    private static final Logger LOGGER = Logger.getLogger(LogManager.class.getName());
    private final TransactionSubsystem provider;
    private LogManagerProperties logManagerProperties;
    private LogPageFlushThread logPageFlusher;
    private final int logPageSize;
    private long statLogSize;
    private long statLogCount;

    /*
     * the array of log pages. The number of log pages is configurable. Pages
     * taken together form an in-memory log buffer.
     */
    private IFileBasedBuffer[] logPages;

    private ILogRecordHelper logRecordHelper;

    /*
     * Number of log pages that constitute the in-memory log buffer.
     */
    private int numLogPages;

    private AtomicLong lastFlushedLSN = new AtomicLong(-1);

    /*
     * When the transaction eco-system comes to life, the log manager positions
     * itself to the end of the last written log. the startingLsn represent the
     * lsn value of the next log record to be written after a system (re)start.
     * The value is zero when the system is starting for the first time.
     */
    private long startingLSN = 0;

    /*
     * lsn represents the monotonically increasing long value that can be broken
     * down into a file id and an offset within a log file.
     */
    private AtomicLong lsn = new AtomicLong(0);

    private List<HashMap<ITransactionContext, Integer>> activeTxnCountMaps;

    public void addFlushRequest(int pageIndex, long lsn, boolean isSynchronous) {
        logPageFlusher.requestFlush(pageIndex, lsn, isSynchronous);
    }

    public AtomicLong getLastFlushedLsn() {
        return lastFlushedLSN;
    }

    public AtomicLong getCurrentLsn() {
        return lsn;
    }

    public long incrementLastFlushedLsn(long delta) {
        return lastFlushedLSN.addAndGet(delta);
    }

    public LogManager(TransactionSubsystem provider) throws ACIDException {
        this.provider = provider;
        initLogManagerProperties(this.provider.getId());
        logPageSize = logManagerProperties.getLogPageSize();
        initLogManager();
        statLogSize = 0;
        statLogCount = 0;
    }

    public LogManager(TransactionSubsystem provider, String nodeId) throws ACIDException {
        this.provider = provider;
        initLogManagerProperties(nodeId);
        logPageSize = logManagerProperties.getLogPageSize();
        initLogManager();
        statLogSize = 0;
        statLogCount = 0;
    }

    /*
     * initialize the log manager properties either from the configuration file
     * on disk or with default values
     */
    private void initLogManagerProperties(String nodeId) throws ACIDException {
        LogManagerProperties logProperties = null;
        InputStream is = null;
        try {
            is = this.getClass().getClassLoader()
                    .getResourceAsStream(TransactionManagementConstants.LogManagerConstants.LOG_CONF_FILE);

            Properties p = new Properties();

            if (is != null) {
                p.load(is);
            }
            logProperties = new LogManagerProperties(p, nodeId);

        } catch (IOException ioe) {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    throw new ACIDException("unable to close input stream ", e);
                }
            }
        }
        logManagerProperties = logProperties;
    }

    private void initLogManager() throws ACIDException {
        logRecordHelper = new LogRecordHelper(this);
        numLogPages = logManagerProperties.getNumLogPages();
        activeTxnCountMaps = new ArrayList<HashMap<ITransactionContext, Integer>>(numLogPages);

        for (int i = 0; i < numLogPages; i++) {
            activeTxnCountMaps.add(new HashMap<ITransactionContext, Integer>());
        }

        logPages = new FileBasedBuffer[numLogPages];

        /*
         * place the log anchor at the end of the last log record written.
         */
        initLSN();

        /*
         * initialize the log pages.
         */
        initializeLogPages(startingLSN);

        /*
         * Instantiate and begin the LogFlusher thread. The Log Flusher thread
         * is responsible for putting log pages to disk. It is configured as a
         * daemon thread so that it does not stop the JVM from exiting when all
         * other threads are done with their work.
         */
        logPageFlusher = new LogPageFlushThread(this);
        logPageFlusher.setDaemon(true);
        AsterixThreadExecutor.INSTANCE.execute(logPageFlusher);
    }

    public int getLogPageIndex(long lsnValue) {
        return (int) (((lsnValue - startingLSN) / logPageSize) % numLogPages);
    }

    /*
     * given a lsn, get the file id where the corresponding log record is
     * located.
     */
    public int getLogFileId(long lsnValue) {
        return (int) ((lsnValue) / logManagerProperties.getLogPartitionSize());
    }

    /*
     * given a lsn, get the offset within a log page where the corresponding log
     * record is (to be) placed.
     */
    public int getLogPageOffset(long lsnValue) {
        return (int) (lsnValue % logPageSize);
    }

    /*
     * The method that reserves the space for a transaction to write log record
     * in the log buffer. Note that the method is not synchronized for
     * performance reasons as we do not want transactions to be blocked by each
     * other when writing log records.
     * 
     * @param entrySize: the requested space.
     * 
     * @param logType: the type of log record.
     */
    private long getLsn(int entrySize, byte logType) throws ACIDException {

        while (true) {
            boolean forwardPage = false;
            long old = lsn.get();

            // get the log page corresponding to the current lsn value
            int pageIndex = getLogPageIndex(old);
            long retVal = old;

            // the lsn value for the next request if the current request is
            // served.
            long next = old + entrySize;
            int prevPage = -1;

            // check if the log record will cross page boundaries, a case that
            // is not allowed.
            if ((next - 1) / logPageSize != old / logPageSize || (next % logPageSize == 0)) {

                if ((old != 0 && old % logPageSize == 0)) {
                    // On second thought, this shall never be the case as it
                    // means that the lsn is
                    // currently at the beginning of a page and we still need to
                    // forward the page which
                    // means that the entrySize exceeds a log page size. If this
                    // is the case, an
                    // exception is thrown before calling this API. would remove
                    // this case.
                    retVal = old;

                } else {
                    // set the lsn to point to the beginning of the next page.
                    retVal = ((old / logPageSize) + 1) * logPageSize;
                }

                next = retVal;

                // as the log record shall cross log page boundary, we must
                // re-assign the lsn so
                // that the log record begins on a different location.
                forwardPage = true;

                prevPage = pageIndex;
                pageIndex = getNextPageInSequence(pageIndex);
            }

            if (!lsn.compareAndSet(old, next)) {
                // Atomic call -> returns true only when the value represented
                // by lsn is same as
                // "old". The value is updated to "next".
                continue;
            }

            if (forwardPage) {
                logPages[prevPage].acquireReadLatch();
                // increment the counter as the transaction thread now holds a
                // space in the log page and hence is an owner.
                logPages[prevPage].incRefCnt();
                logPages[prevPage].releaseReadLatch();

                // forward the nextWriteOffset in the log page
                logPages[prevPage].setBufferNextWriteOffset(logPageSize);

                logPages[prevPage].decRefCnt();

                addFlushRequest(prevPage, old, false);

                // The transaction thread that discovers the need to forward a
                // page is made to re-acquire a lsn.
                continue;

            } else {
                logPages[pageIndex].acquireReadLatch();
                // increment the counter as the transaction thread now holds a
                // space in the log page and hence is an owner.
                logPages[pageIndex].incRefCnt();
                logPages[pageIndex].releaseReadLatch();

                // Before the count is incremented, if the flusher flushed the
                // allocated page,
                // then retry to get new LSN. Otherwise, the log with allocated
                // lsn will be lost.
                if (lastFlushedLSN.get() >= retVal) {
                    logPages[pageIndex].decRefCnt();
                    continue;
                }
            }

            return retVal;
        }
    }

    @Override
    public void log(byte logType, ITransactionContext txnCtx, int datasetId, int PKHashValue, long resourceId,
            byte resourceMgrId, int logContentSize, ReusableLogContentObject reusableLogContentObject, ILogger logger,
            LogicalLogLocator logicalLogLocator) throws ACIDException {

        HashMap<ITransactionContext, Integer> map = null;
        int activeTxnCount;

        // logLocator is a re-usable object that is appropriately set in each
        // invocation.
        // If the reference is null, the log manager must throw an exception.
        if (logicalLogLocator == null) {
            throw new ACIDException(
                    " you need to pass in a non-null logLocator, if you dont have it, then pass in a dummy so that the +"
                            + "log manager can set it approporiately for you");
        }

        // compute the total log size including the header and the checksum.
        int totalLogSize = logRecordHelper.getLogRecordSize(logType, logContentSize);

        // check for the total space requirement to be less than a log page.
        if (totalLogSize > logPageSize) {
            throw new ACIDException(
                    " Maximum Log Content Size is "
                            + (logPageSize - logRecordHelper.getLogHeaderSize(LogType.UPDATE) - logRecordHelper
                                    .getLogChecksumSize()));
        }

        // all constraints checked and we are good to go and acquire a lsn.
        long previousLSN = -1;

        // the will be set to the location (a long value) where the log record
        // needs to be placed.
        long currentLSN;

        // The logs written by a transaction need to be linked to each other for
        // a successful rollback/recovery. However there could be multiple
        // threads operating concurrently that are part of a common transaction.
        // These threads need to synchronize and record the lsn corresponding to
        // the last log record written by (any thread of) the transaction.
        synchronized (txnCtx) {
            previousLSN = txnCtx.getLastLogLocator().getLsn();
            currentLSN = getLsn(totalLogSize, logType);
            txnCtx.setLastLSN(currentLSN);
            if (IS_DEBUG_MODE) {
                System.out.println("--------------> LSN(" + currentLSN + ") is allocated");
            }
            logicalLogLocator.setLsn(currentLSN);
        }

        /*
         * At this point, the transaction thread has obtained reserved space for
         * writing the log record. In doing so, it has acquired (shared)
         * ownership of the log page. All subsequent actions are under a try
         * catch block so that if any exception is encountered, a clean can be
         * performed correctly that is ownership is released.
         */

        // indicates if the transaction thread has release ownership of the
        // page.
        boolean decremented = false;

        int pageIndex = (int) getLogPageIndex(currentLSN);

        // the lsn has been obtained for the log record. need to set the
        // LogLocator instance accordingly.
        try {
            logicalLogLocator.setBuffer(logPages[pageIndex]);
            int pageOffset = getLogPageOffset(currentLSN);
            logicalLogLocator.setMemoryOffset(pageOffset);

            // write the log header.
            logRecordHelper.writeLogHeader(logicalLogLocator, logType, txnCtx, datasetId, PKHashValue, previousLSN,
                    resourceId, resourceMgrId, logContentSize);

            // increment the offset so that the transaction can fill up the
            // content in the correct region of the allocated space.
            logicalLogLocator.increaseMemoryOffset(logRecordHelper.getLogHeaderSize(logType));

            // a COMMIT log record does not have any content and hence
            // the logger (responsible for putting the log content) is not
            // invoked.
            if (logContentSize != 0) {
                logger.preLog(txnCtx, reusableLogContentObject);
            }

            if (logContentSize != 0) {
                // call the logger implementation and ask to fill in the log
                // record content at the allocated space.
                logger.log(txnCtx, logicalLogLocator, logContentSize, reusableLogContentObject);
                logger.postLog(txnCtx, reusableLogContentObject);
                if (IS_DEBUG_MODE) {
                    logicalLogLocator.setMemoryOffset(logicalLogLocator.getMemoryOffset()
                            - logRecordHelper.getLogHeaderSize(logType));
                    System.out.println(logRecordHelper.getLogRecordForDisplay(logicalLogLocator));
                    logicalLogLocator.increaseMemoryOffset(logRecordHelper.getLogHeaderSize(logType));
                }
            }

            // The log record has been written. For integrity checks, compute
            // the checksum and put it at the end of the log record.
            int startPosChecksum = logicalLogLocator.getMemoryOffset() - logRecordHelper.getLogHeaderSize(logType);
            int length = totalLogSize - logRecordHelper.getLogChecksumSize();
            long checksum = DataUtil.getChecksum(logPages[pageIndex], startPosChecksum, length);
            logPages[pageIndex].writeLong(pageOffset + logRecordHelper.getLogHeaderSize(logType) + logContentSize,
                    checksum);

            // forward the nextWriteOffset in the log page
            int bufferNextWriteOffset = (int) ((currentLSN + totalLogSize) % logPageSize);
            if (bufferNextWriteOffset == 0) {
                bufferNextWriteOffset = logPageSize;
            }
            logPages[pageIndex].setBufferNextWriteOffset(bufferNextWriteOffset);

            if (logType != LogType.ENTITY_COMMIT) {
                if (logType == LogType.COMMIT) {
                    txnCtx.setExclusiveJobLevelCommit();
                    map = activeTxnCountMaps.get(pageIndex);
                    map.put(txnCtx, 1);
                }
                // release the ownership as the log record has been placed in
                // created space.
                logPages[pageIndex].decRefCnt();

                // indicating that the transaction thread has released ownership
                decremented = true;
            }

            if (logType == LogType.ENTITY_COMMIT) {
                map = activeTxnCountMaps.get(pageIndex);
                if (map.containsKey(txnCtx)) {
                    activeTxnCount = (Integer) map.get(txnCtx);
                    activeTxnCount++;
                    map.put(txnCtx, activeTxnCount);
                } else {
                    map.put(txnCtx, 1);
                }
                //------------------------------------------------------------------------------
                // [Notice]
                // reference count should be decremented 
                // after activeTxnCount is incremented, but before addFlushRequest() is called. 
                //------------------------------------------------------------------------------
                // release the ownership as the log record has been placed in
                // created space.
                logPages[pageIndex].decRefCnt();

                // indicating that the transaction thread has released ownership
                decremented = true;

                addFlushRequest(pageIndex, currentLSN, false);
            } else if (logType == LogType.COMMIT) {

                addFlushRequest(pageIndex, currentLSN, true);
                if (IS_DEBUG_MODE) {
                    System.out.println("Running sum of log size: " + statLogSize + ", log count: " + statLogCount);
                }
            }

            if (IS_DEBUG_MODE) {
                System.out.println("--------------> LSN(" + currentLSN + ") is written");
            }

            //collect statistics
            statLogSize += totalLogSize;
            statLogCount++;

        } catch (Exception e) {
            e.printStackTrace();
            throw new ACIDException(txnCtx, "Thread: " + Thread.currentThread().getName()
                    + " logger encountered exception", e);
        } finally {
            if (!decremented) {
                logPages[pageIndex].decRefCnt();
            }
        }
    }

    /*
     * This method resets the log page and is called by the log flusher thread
     * after a page has been flushed to disk.
     */
    public void resetLogPage(long lsn, long nextWritePosition, int pageIndex) throws IOException {

        String filePath = LogUtil.getLogFilePath(logManagerProperties, getLogFileId(lsn));

        logPages[pageIndex].reset(filePath, LogUtil.getFileOffset(this, nextWritePosition), logPageSize);
    }

    @Override
    public ILogCursor readLog(PhysicalLogLocator physicalLogLocator, ILogFilter logFilter) throws IOException,
            ACIDException {
        LogCursor cursor = new LogCursor(this, physicalLogLocator, logFilter, logPageSize);
        return cursor;
    }

    /*
     * Read a log that is residing on the disk.
     */
    private void readDiskLog(long lsnValue, LogicalLogLocator logicalLogLocator) throws ACIDException {
        String filePath = LogUtil.getLogFilePath(logManagerProperties, LogUtil.getFileId(this, lsnValue));
        long fileOffset = LogUtil.getFileOffset(this, lsnValue);

        ByteBuffer buffer = ByteBuffer.allocate(logPageSize);
        RandomAccessFile raf = null;
        FileChannel fileChannel = null;
        try {
            raf = new RandomAccessFile(filePath, "r");
            fileChannel = raf.getChannel();
            fileChannel.position(fileOffset);
            fileChannel.read(buffer);
            buffer.position(0);

            byte logType = buffer.get(4);
            int logHeaderSize = logRecordHelper.getLogHeaderSize(logType);
            int logBodySize = buffer.getInt(logHeaderSize - 4);
            int logRecordSize = logHeaderSize + logBodySize + logRecordHelper.getLogChecksumSize();
            buffer.limit(logRecordSize);
            MemBasedBuffer memBuffer = new MemBasedBuffer(buffer.slice());
            if (logicalLogLocator == null) {
                logicalLogLocator = new LogicalLogLocator(lsnValue, memBuffer, 0, this);
            } else {
                logicalLogLocator.setLsn(lsnValue);
                logicalLogLocator.setBuffer(memBuffer);
                logicalLogLocator.setMemoryOffset(0);
            }
            if (!logRecordHelper.validateLogRecord(logicalLogLocator)) {
                throw new ACIDException(" invalid log record at lsn " + lsnValue);
            }
        } catch (Exception fnfe) {
            fnfe.printStackTrace();
            throw new ACIDException(" unable to retrieve log record with lsn " + lsnValue + " from the file system",
                    fnfe);
        } finally {
            try {
                if (fileChannel != null) {
                    fileChannel.close();
                } else if (raf != null) {
                    raf.close();
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw new ACIDException(" exception in closing a file: " + filePath, ioe);
            }
        }
    }

    @Override
    public void readLog(long lsnValue, LogicalLogLocator logicalLogLocator) throws ACIDException {
        byte[] logRecord = null;

        if (lsnValue >= lsn.get()) {
            throw new ACIDException(" invalid lsn " + lsnValue);
        }

        /* check if the log record in the log buffer or has reached the disk. */
        if (isMemoryRead(lsnValue)) {
            int pageIndex = getLogPageIndex(lsnValue);
            int pageOffset = getLogPageOffset(lsnValue);

            // TODO
            // minimize memory allocation overhead. current code allocates the
            // log page size per reading a log record.

            byte[] pageContent = new byte[logPageSize];

            // take a lock on the log page so that the page is not flushed to
            // disk interim
            synchronized (logPages[pageIndex]) {

                // need to check again (this thread may have got de-scheduled
                // and must refresh!)

                if (isMemoryRead(lsnValue)) {
                    // get the log record length
                    logPages[pageIndex].getBytes(pageContent, 0, pageContent.length);
                    byte logType = pageContent[pageOffset + 4];
                    int logHeaderSize = logRecordHelper.getLogHeaderSize(logType);
                    int logBodySize = DataUtil.byteArrayToInt(pageContent, pageOffset + logHeaderSize - 4);
                    int logRecordSize = logHeaderSize + logBodySize + logRecordHelper.getLogChecksumSize();
                    logRecord = new byte[logRecordSize];

                    // copy the log record content
                    System.arraycopy(pageContent, pageOffset, logRecord, 0, logRecordSize);
                    MemBasedBuffer memBuffer = new MemBasedBuffer(logRecord);
                    if (logicalLogLocator == null) {
                        logicalLogLocator = new LogicalLogLocator(lsnValue, memBuffer, 0, this);
                    } else {
                        logicalLogLocator.setLsn(lsnValue);
                        logicalLogLocator.setBuffer(memBuffer);
                        logicalLogLocator.setMemoryOffset(0);
                    }
                    try {
                        // validate the log record by comparing checksums
                        if (!logRecordHelper.validateLogRecord(logicalLogLocator)) {
                            throw new ACIDException(" invalid log record at lsn " + lsnValue);
                        }
                    } catch (Exception e) {
                        throw new ACIDException("exception encoutered in validating log record at lsn " + lsnValue, e);
                    }
                    return;
                }
            }
        }

        // the log record is residing on the disk, read it from there.
        readDiskLog(lsnValue, logicalLogLocator);
    }

    public boolean isMemoryRead(long currentLSN) {
        long flushLSN = lastFlushedLSN.get();
        if ((flushLSN + 1) % logPageSize == 0) {
            return false;
        }
        long logPageBeginOffset = flushLSN - (flushLSN % logPageSize);
        long logPageEndOffset = logPageBeginOffset + logPageSize;
        if (currentLSN > flushLSN || (currentLSN >= logPageBeginOffset && currentLSN < logPageEndOffset)) {
            return true;
        } else {
            return false;
        }
    }

    public void renewLogFiles() throws ACIDException {
        List<String> logFileNames = LogUtil.getLogFiles(logManagerProperties);
        for (String name : logFileNames) {
            File file = new File(LogUtil.getLogFilePath(logManagerProperties, Long.parseLong(name)));
            if (!file.delete()) {
                throw new ACIDException("Failed to delete a file: " + name);
            }
        }
        closeLogPages();
        initLSN();
        openLogPages();
        logPageFlusher.renew();
    }

    private void initLSN() throws ACIDException {
        PhysicalLogLocator nextPhysicalLsn = LogUtil.initializeLogAnchor(this);
        startingLSN = nextPhysicalLsn.getLsn();
        lastFlushedLSN.set(startingLSN - 1);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" Starting lsn is : " + startingLSN);
        }
        lsn.set(startingLSN);
    }

    private void closeLogPages() throws ACIDException {
        for (int i = 0; i < numLogPages; i++) {
            try {
                logPages[i].close();
            } catch (IOException e) {
                throw new ACIDException(e);
            }
        }
    }

    private void openLogPages() throws ACIDException {
        try {
            String filePath = LogUtil.getLogFilePath(logManagerProperties, LogUtil.getFileId(this, startingLSN));
            for (int i = 0; i < numLogPages; i++) {
                logPages[i].open(filePath, LogUtil.getFileOffset(this, startingLSN) + i * logPageSize, logPageSize);
            }
        } catch (Exception e) {
            throw new ACIDException(Thread.currentThread().getName() + " unable to create log buffer", e);
        }
    }

    @Override
    public ILogRecordHelper getLogRecordHelper() {
        return logRecordHelper;
    }

    /*
     * Map each log page to cover a physical byte range over a log file. When a
     * page is flushed, the page contents are put to disk in the corresponding
     * byte range.
     */
    private void initializeLogPages(long beginLsn) throws ACIDException {
        try {
            String filePath = LogUtil.getLogFilePath(logManagerProperties, LogUtil.getFileId(this, beginLsn));
            long nextDiskWriteOffset = LogUtil.getFileOffset(this, beginLsn);
            long nextBufferWriteOffset = nextDiskWriteOffset % logPageSize;
            long bufferBeginOffset = nextDiskWriteOffset - nextBufferWriteOffset;

            for (int i = 0; i < numLogPages; i++) {
                logPages[i] = FileUtil.getFileBasedBuffer(filePath, bufferBeginOffset + i * logPageSize, logPageSize,
                        logManagerProperties.getDiskSectorSize());
                if (i == 0) {
                    logPages[i].setBufferLastFlushOffset((int) nextBufferWriteOffset);
                    logPages[i].setBufferNextWriteOffset((int) nextBufferWriteOffset);
                    logPages[i].setDiskNextWriteOffset(nextDiskWriteOffset);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new ACIDException(Thread.currentThread().getName() + " unable to create log buffer", e);
        }
    }

    /*
     * Pages are sequenced starting with 0 going upto numLogPages-1.
     */
    public int getNextPageInSequence(int pageNo) {
        return (pageNo + 1) % numLogPages;
    }

    public int getPreviousPageInSequence(int pageNo) {
        return pageNo == 0 ? numLogPages - 1 : pageNo - 1;
    }

    @Override
    public LogManagerProperties getLogManagerProperties() {
        return logManagerProperties;
    }

    public IFileBasedBuffer getLogPage(int pageIndex) {
        return logPages[pageIndex];
    }

    public IFileBasedBuffer[] getLogPages() {
        return logPages;
    }

    @Override
    public TransactionSubsystem getTransactionSubsystem() {
        return provider;
    }

    static AtomicInteger t = new AtomicInteger();

    public void decrementActiveTxnCountOnIndexes(int pageIndex) throws HyracksDataException {
        ITransactionContext ctx = null;
        int count = 0;
        int i = 0;

        HashMap<ITransactionContext, Integer> map = activeTxnCountMaps.get(pageIndex);
        Set<Map.Entry<ITransactionContext, Integer>> entrySet = map.entrySet();
        if (entrySet != null) {
            for (Map.Entry<ITransactionContext, Integer> entry : entrySet) {
                if (entry != null) {
                    if (entry.getValue() != null) {
                        count = entry.getValue();
                    }
                    if (count > 0) {
                        ctx = entry.getKey();
                        for (i = 0; i < count; i++) {
                            ctx.decreaseActiveTransactionCountOnIndexes();
                        }
                    }
                }
            }
        }

        map.clear();
    }

    @Override
    public void start() {
        //no op
    }

    @Override
    public void stop(boolean dumpState, OutputStream os) {
        if (dumpState) {
            //#. dump Configurable Variables
            dumpConfVars(os);

            //#. dump LSNInfo
            dumpLSNInfo(os);

            try {
                os.flush();
            } catch (IOException e) {
                //ignore
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
            //ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
    }

    private void dumpLSNInfo(OutputStream os) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("\n>>dump_begin\t>>----- [LSNInfo] -----");
            sb.append("\nstartingLSN: " + startingLSN);
            sb.append("\ncurrentLSN: " + lsn.get());
            sb.append("\nlastFlushedLSN: " + lastFlushedLSN.get());
            sb.append("\n>>dump_end\t>>----- [LSNInfo] -----\n");
            os.write(sb.toString().getBytes());
        } catch (Exception e) {
            //ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
    }
}

/*
 * The thread responsible for putting log pages to disk in an ordered manner.
 * The Log Flusher updates the bookkeeping data internal to the log manager and
 * acquires appropriate locks. It also acquires finer level locks on the log
 * page when it is in process of flushing the content to disk.
 */
class LogPageFlushThread extends Thread {

    private LogManager logManager;
    /*
     * pendingFlushRequests is a map with key as Integer denoting the page
     * index. When a (transaction) thread discovers the need to flush a page, it
     * puts its Thread object into the corresponding value that is a
     * LinkedBlockingQueue. The LogManager has a LogFlusher thread that scans
     * this map in order of page index (and circling around). The flusher thread
     * needs to flush pages in order and waits for a thread to deposit an object
     * in the blocking queue corresponding to the next page in order. A request
     * to flush a page is conveyed to the flush thread by simply depositing an
     * object in to corresponding blocking queue. It is blocking in the sense
     * that the flusher thread will continue to wait for an object to arrive in
     * the queue. The object itself is ignored by the fliusher and just acts as
     * a signal/event that a page needs to be flushed.
     */
    private final LinkedBlockingQueue<Object>[] flushRequestQueue;
    private final Object[] flushRequests;
    private int flushPageIndex;
    private final long groupCommitWaitPeriod;
    private boolean isRenewRequest;

    public LogPageFlushThread(LogManager logManager) {
        this.logManager = logManager;
        setName("Flusher");
        int numLogPages = logManager.getLogManagerProperties().getNumLogPages();
        this.flushRequestQueue = new LinkedBlockingQueue[numLogPages];
        this.flushRequests = new Object[numLogPages];
        for (int i = 0; i < numLogPages; i++) {
            flushRequestQueue[i] = new LinkedBlockingQueue<Object>(1);
            flushRequests[i] = new Object();
        }
        this.flushPageIndex = 0;
        groupCommitWaitPeriod = logManager.getLogManagerProperties().getGroupCommitWaitPeriod();
        isRenewRequest = false;
    }

    public void renew() {
        isRenewRequest = true;
        flushPageIndex = 0;
        this.interrupt();
        isRenewRequest = false;
    }

    public void requestFlush(int pageIndex, long lsn, boolean isSynchronous) {
        synchronized (logManager.getLogPage(pageIndex)) {
            // return if flushedLSN >= lsn
            if (logManager.getLastFlushedLsn().get() >= lsn) {
                return;
            }

            // put a new request to the queue only if the request on the page is
            // not in the queue.
            flushRequestQueue[pageIndex].offer(flushRequests[pageIndex]);

            // return if the request is asynchronous
            if (!isSynchronous) {
                return;
            }

            // wait until there is flush.
            boolean isNotified = false;
            while (!isNotified) {
                try {
                    logManager.getLogPage(pageIndex).wait();
                    isNotified = true;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void run() {
        int logPageSize = logManager.getLogManagerProperties().getLogPageSize();
        int logBufferSize = logManager.getLogManagerProperties().getLogBufferSize();
        int beforeFlushOffset = 0;
        int afterFlushOffset = 0;
        boolean resetFlushPageIndex = false;

        while (true) {
            try {
                // A wait call on the linkedBLockingQueue. The flusher thread is
                // notified when an object is added to the queue. Please note
                // that each page has an associated blocking queue.
                try {
                    flushRequestQueue[flushPageIndex].take();
                } catch (InterruptedException ie) {
                    while (isRenewRequest) {
                        sleep(1);
                    }
                    continue;
                }

                //if the log page is already full, don't wait. 
                if (logManager.getLogPage(flushPageIndex).getBufferNextWriteOffset() < logPageSize
                        - logManager.getLogRecordHelper().getCommitLogSize()) {
                    // #. sleep for the groupCommitWaitTime
                    sleep(groupCommitWaitPeriod);
                }

                synchronized (logManager.getLogPage(flushPageIndex)) {
                    logManager.getLogPage(flushPageIndex).acquireWriteLatch();
                    try {

                        // #. need to wait until the reference count reaches 0
                        while (logManager.getLogPage(flushPageIndex).getRefCnt() != 0) {
                            sleep(0);
                        }

                        beforeFlushOffset = logManager.getLogPage(flushPageIndex).getBufferLastFlushOffset();

                        // put the content to disk (the thread still has a lock on the log page)
                        logManager.getLogPage(flushPageIndex).flush();

                        afterFlushOffset = logManager.getLogPage(flushPageIndex).getBufferLastFlushOffset();

                        // increment the last flushed lsn
                        logManager.incrementLastFlushedLsn(afterFlushOffset - beforeFlushOffset);

                        // increment currentLSN if currentLSN is less than flushLSN.
                        if (logManager.getLastFlushedLsn().get() + 1 > logManager.getCurrentLsn().get()) {
                            logManager.getCurrentLsn().set(logManager.getLastFlushedLsn().get() + 1);
                        }

                        // Map the log page to a new region in the log file if the flushOffset reached the logPageSize
                        if (afterFlushOffset == logPageSize) {
                            long diskNextWriteOffset = logManager.getLogPages()[flushPageIndex]
                                    .getDiskNextWriteOffset() + logBufferSize;
                            logManager.resetLogPage(logManager.getLastFlushedLsn().get() + 1 + logBufferSize,
                                    diskNextWriteOffset, flushPageIndex);
                            resetFlushPageIndex = true;
                        }

                        // decrement activeTxnCountOnIndexes
                        logManager.decrementActiveTxnCountOnIndexes(flushPageIndex);

                    } finally {
                        logManager.getLogPage(flushPageIndex).releaseWriteLatch();
                    }

                    // #. checks the queue whether there is another flush
                    // request on the same log buffer
                    // If there is another request, then simply remove it.
                    if (flushRequestQueue[flushPageIndex].peek() != null) {
                        flushRequestQueue[flushPageIndex].take();
                    }

                    // notify all waiting (transaction) threads.
                    logManager.getLogPage(flushPageIndex).notifyAll();

                    if (resetFlushPageIndex) {
                        flushPageIndex = logManager.getNextPageInSequence(flushPageIndex);
                        resetFlushPageIndex = false;
                    }
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw new Error(" exception in flushing log page", ioe);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
    }
}