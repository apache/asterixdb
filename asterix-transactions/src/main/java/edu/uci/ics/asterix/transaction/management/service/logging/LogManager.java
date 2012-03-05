/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;

public class LogManager implements ILogManager {

    /*
     * Log Record Structure HEADER
     * <(log_magic_number,4)(log_length,8)(log_type,1
     * )(log_action_type,1)(log_timestamp
     * ,8)(log_transaction_id,8)(resource_manager_id
     * ,1)(page_id,8)(previous_lsn,8) <CONTENT> TAIL <(checksum,8)>
     */

    private static final Logger LOGGER = Logger.getLogger(LogManager.class.getName());
    private TransactionProvider provider;
    private LogManagerProperties logManagerProperties;

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
    /*
     * Initially all pages have an owner count of 1 that is the LogManager. When
     * a transaction requests to write in a log page, the owner count is
     * incremented. The log manager reserves space in the log page and puts in
     * the log header but leaves the space for the content and the checksum
     * (covering the whole log record). When the content has been put, the log
     * manager computes the checksum and puts it after the content. At this
     * point, the ownership count is decremented as the transaction is done with
     * using the page. When a page is full, the log manager decrements the count
     * by one indicating that it has released its ownership of the log page.
     * There could be other transaction(s) still owning the page (that is they
     * could still be mid-way putting the log content). When the ownership count
     * eventually reaches zero, the thread responsible for flushing the log page
     * is notified and the page is flushed to disk.
     */
    private AtomicInteger[] logPageOwnerCount;

    static class PageOwnershipStatus {
        public static final int LOG_WRITER = 1;
        public static final int LOG_FLUSHER = 0;
    }

    /*
     * LogPageStatus: A page is either ACTIVE or INACTIVE. The status for each
     * page is maintained in a map called logPageStatus. A page is ACTIVE when
     * the LogManager can allocate space in the page for writing a log record.
     * Initially all pages are ACTIVE. As transactions fill up space by writing
     * log records, a page may not have sufficient space left for serving a
     * request by a transaction. When this happens, the page is marked INACTIVE.
     * An INACTIVE page with no owners ( logPageOwnerCount.get(<pageIndex>) ==
     * 0) indicates that the page must be flushed to disk before any other log
     * record is written on the page.F
     */

    // private Map<Integer, Integer> logPageStatus = new
    // ConcurrentHashMap<Integer, Integer>();
    private AtomicInteger[] logPageStatus;

    static class PageState {
        public static final int INACTIVE = 0;
        public static final int ACTIVE = 1;
    }

    private AtomicLong lastFlushedLsn = new AtomicLong(-1);
    private AtomicInteger lastFlushedPage = new AtomicInteger(-1);

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

    private LinkedBlockingQueue[] pendingFlushRequests;

    /*
     * ICommitResolver is an interface that provides an API that can answer a
     * simple boolean - Given the commit requests so far, should a page be
     * flushed. The implementation of the interface contains the logic (or you
     * can say the policy) for commit. It could be group commit in which case
     * the commit resolver may not return a true indicating that it wishes to
     * delay flushing of the page.
     */
    private ICommitResolver commitResolver;

    /*
     * An object that keeps track of the submitted commit requests.
     */
    private CommitRequestStatistics commitRequestStatistics;

    /*
     * When the transaction eco-system comes to life, the log manager positions
     * itself to the end of the last written log. the startingLsn represent the
     * lsn value of the next log record to be written after a system (re)start.
     * The value is zero when the system is starting for the first time.
     */
    private long startingLsn = 0;

    /*
     * lsn represents the monotonically increasing long value that can be broken
     * down into a file id and an offset within a log file.
     */
    private AtomicLong lsn = new AtomicLong(0);

    /*
     * A map that tracks the flush requests submitted for each page. The
     * requests for a page are cleared when the page is flushed.
     */
    public LinkedBlockingQueue<Thread> getPendingFlushRequests(int pageIndex) {
        return pendingFlushRequests[pageIndex];
    }

    public void addFlushRequest(int pageIndex) {
        pendingFlushRequests[pageIndex].add(Thread.currentThread());
    }

    public AtomicLong getLastFlushedLsn() {
        return lastFlushedLsn;
    }

    public AtomicInteger getLogPageStatus(int pageIndex) {
        return logPageStatus[pageIndex];
    }

    public AtomicLong getCurrentLsn() {
        return lsn;
    }

    public long incrementLastFlushedLsn(long delta) {
        return lastFlushedLsn.addAndGet(delta);
    }

    public LogManager(TransactionProvider provider) throws ACIDException {
        this.provider = provider;
        initLogManagerProperties(null);
        initLogManager();
    }

    public LogManager(TransactionProvider provider, LogManagerProperties logConfiguration) throws ACIDException {
        this.provider = provider;
        initLogManagerProperties(logConfiguration);
        initLogManager();
    }

    /*
     * initialize the log manager properties either from the configuration file
     * on disk or with default values
     */
    private void initLogManagerProperties(LogManagerProperties logConfiguration) throws ACIDException {
        if (logConfiguration == null) {
            InputStream is = null;
            try {
                File file = new File(TransactionManagementConstants.LogManagerConstants.LOG_CONF_DIR
                        + File.pathSeparator + TransactionManagementConstants.LogManagerConstants.LOG_CONF_FILE);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Log Configuration file path is " + file.getAbsolutePath());
                }
                if (file.exists()) {
                    is = new FileInputStream(TransactionManagementConstants.LogManagerConstants.LOG_CONF_DIR
                            + File.pathSeparator + TransactionManagementConstants.LogManagerConstants.LOG_CONF_FILE);
                    Properties configuredProperties = new Properties();
                    configuredProperties.load(is);
                    logConfiguration = new LogManagerProperties(configuredProperties);
                } else {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Log configuration file not found, using defaults !");
                    }
                    Properties configuredProperties = new Properties();
                    configuredProperties.setProperty(LogManagerProperties.LOG_DIR_KEY,
                            TransactionManagementConstants.LogManagerConstants.DEFAULT_LOG_DIR + File.separator
                                    + provider.getId());
                    logConfiguration = new LogManagerProperties(configuredProperties);
                }
            } catch (IOException ioe) {
                if (is != null) {
                    try {
                        is.close();
                    } catch (IOException e) {
                        throw new ACIDException("unable to close input stream ", e);
                    }
                }
            }
        }
        logManagerProperties = logConfiguration;
    }

    private void initLogManager() throws ACIDException {
        logRecordHelper = new LogRecordHelper(this);
        numLogPages = logManagerProperties.getNumLogPages();
        logPageOwnerCount = new AtomicInteger[numLogPages];
        logPageStatus = new AtomicInteger[numLogPages];
        pendingFlushRequests = new LinkedBlockingQueue[numLogPages];
        if (logManagerProperties.getGroupCommitWaitPeriod() > 0) { // configure
            // the
            // Commit
            // Resolver
            commitResolver = new GroupCommitResolver(); // Group Commit is
            // enabled
            commitRequestStatistics = new CommitRequestStatistics(numLogPages);
        } else {
            commitResolver = new BasicCommitResolver(); // the basic commit
            // resolver
        }
        this.commitResolver.init(this); // initialize the commit resolver
        logPages = new FileBasedBuffer[numLogPages];

        /*
         * place the log anchor at the end of the last log record written.
         */
        PhysicalLogLocator nextPhysicalLsn = LogUtil.initializeLogAnchor(this);
        startingLsn = nextPhysicalLsn.getLsn();
        lastFlushedLsn.set(startingLsn - 1);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" Starting lsn is : " + startingLsn);
        }
        lsn.set(startingLsn);
        /*
         * initialize meta data for each log page.
         */
        for (int i = 0; i < numLogPages; i++) {
            logPageOwnerCount[i] = new AtomicInteger(PageOwnershipStatus.LOG_WRITER);
            logPageStatus[i] = new AtomicInteger(PageState.ACTIVE);
            pendingFlushRequests[i] = new LinkedBlockingQueue<Thread>();
        }

        /*
         * initialize the log pages.
         */
        initializeLogPages(nextPhysicalLsn);

        /*
         * Instantiate and begin the LogFlusher thread. The Log Flusher thread
         * is responsible for putting log pages to disk. It is configured as a
         * daemon thread so that it does not stop the JVM from exiting when all
         * other threads are done with their work.
         */
        LogPageFlushThread logFlusher = new LogPageFlushThread(this);
        logFlusher.setDaemon(true);
        logFlusher.start();
    }

    public int getLogPageIndex(long lsnValue) {
        return (int) ((lsnValue - startingLsn) / logManagerProperties.getLogPageSize()) % numLogPages;

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
        return (int) (lsnValue - startingLsn) % logManagerProperties.getLogPageSize();
    }

    /*
     * a transaction thread under certain scenarios is required to wait until
     * the page where it has to write a log record becomes available for writing
     * a log record.
     */
    private void waitUntillPageIsAvailableForWritingLog(int pageIndex) throws ACIDException {
        if (logPageStatus[pageIndex].get() == PageState.ACTIVE
                && getLogPageOwnershipCount(pageIndex).get() >= PageOwnershipStatus.LOG_WRITER) {
            return;
        }
        try {
            synchronized (logPages[pageIndex]) {
                while (!(logPageStatus[pageIndex].get() == PageState.ACTIVE && logPageOwnerCount[pageIndex].get() >= PageOwnershipStatus.LOG_WRITER)) {
                    logPages[pageIndex].wait();
                }
            }
        } catch (InterruptedException e) {
            throw new ACIDException(" thread interrupted while waiting for page " + pageIndex + " to be available ", e);
        }
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
        long pageSize = logManagerProperties.getLogPageSize();
        boolean requiresFlushing = logType == LogType.COMMIT;
        while (true) {
            boolean forwardPage = false;
            boolean shouldFlushPage = false;
            long old = lsn.get();
            int pageIndex = getLogPageIndex(old); // get the log page
            // corresponding to the
            // current lsn value
            long retVal = old;
            long next = old + entrySize; // the lsn value for the next request,
            // if the current request is served.
            int prevPage = -1;
            if ((next - 1) / pageSize != old / pageSize // check if the log
            // record will cross
            // page boundaries, a
            // case that is not
            // allowed.
                    || (next % pageSize == 0)) {
                if ((old != 0 && old % pageSize == 0)) {
                    retVal = old; // On second thought, this shall never be the
                    // case as it means that the lsn is
                    // currently at the beginning of a page and
                    // we still need to forward the page which
                    // means that the entrySize exceeds a log
                    // page size. If this is the case, an
                    // exception is thrown before calling this
                    // API.
                    // would remove this case.

                } else {
                    retVal = ((old / pageSize) + 1) * pageSize; // set the lsn
                    // to point to
                    // the beginning
                    // of the next
                    // page.
                }
                next = retVal;
                forwardPage = true; // as the log record shall cross log page
                // boundary, we must re-assign the lsn (so
                // that the log record begins on a different
                // location.
                prevPage = pageIndex;
                pageIndex = getNextPageInSequence(pageIndex);
            }

            /*
             * we do not want to keep allocating LSNs if the corresponding page
             * is unavailable. Consider a scenario when the log flusher thread
             * is incredibly slow in flushing pages. Transaction threads will
             * acquire an lsn each for writing their next log record. When a
             * page has been made available, mulltiple transaction threads that
             * were waiting can continue to write their log record at the
             * assigned LSNs. Two transaction threads may get LSNs that are on
             * the same log page but actually differ by the size of the log
             * buffer. This would be erroneous. Transaction threads are made to
             * wait upfront for avoiding this situation.
             */
            waitUntillPageIsAvailableForWritingLog(pageIndex);

            if (!forwardPage && requiresFlushing) {
                shouldFlushPage = commitResolver.shouldCommitPage(pageIndex, this, commitRequestStatistics);
                if (shouldFlushPage) {
                    next = ((next / pageSize) + 1) * pageSize; /*
                                                                * next
                                                                * represents the
                                                                * next value of
                                                                * lsn after this
                                                                * log record has
                                                                * been written.
                                                                * If the page
                                                                * needs to be
                                                                * flushed, then
                                                                * we do not give
                                                                * any more LSNs
                                                                * from this
                                                                * page.
                                                                */
                }
            }
            if (!lsn.compareAndSet(old, next)) { // Atomic call -> returns true
                // only when the value
                // represented by lsn is same as
                // "old". The value is updated
                // to "next".
                continue;
            }

            if (forwardPage) {
                logPageStatus[prevPage].set(PageState.INACTIVE); // mark
                // previous
                // page
                // inactive

                /*
                 * decrement on the behalf of the log manager. if there are no
                 * more owners (count == 0) the page must be marked as a
                 * candidate to be flushed.
                 */
                int pageDirtyCount = getLogPageOwnershipCount(prevPage).decrementAndGet();
                if (pageDirtyCount == 0) {
                    addFlushRequest(prevPage);
                }

                /*
                 * The transaction thread that discovers the need to forward a
                 * page is made to re-acquire a lsn.
                 */
                continue;
            } else {
                /*
                 * the transaction thread has been given a space in a log page,
                 * but is made to wait until the page is available.
                 */
                waitUntillPageIsAvailableForWritingLog(pageIndex);
                /*
                 * increment the counter as the transaction thread now holds a
                 * space in the log page and hence is an owner.
                 */
                logPageOwnerCount[pageIndex].incrementAndGet();
            }
            if (requiresFlushing) {
                if (!shouldFlushPage) {
                    /*
                     * the log record requires the page to be flushed but under
                     * the commit policy, the flush task has been deferred. The
                     * transaction thread submits its request to flush the page.
                     */
                    commitRequestStatistics.registerCommitRequest(pageIndex);
                } else {
                    /*
                     * the flush request was approved by the commit resolver.
                     * Thus the page is marked INACTIVE as no more logs will be
                     * written on this page. The log manager needs to release
                     * its ownership. Note that transaction threads may still
                     * continue to be owners of the log page till they fill up
                     * the space allocated to them.
                     */
                    logPageStatus[pageIndex].set(PageState.INACTIVE);
                    logPageOwnerCount[pageIndex].decrementAndGet(); // on
                    // the
                    // behalf
                    // of
                    // log
                    // manager
                }
            }
            return retVal;
        }
    }

    public void log(LogicalLogLocator logLocator, TransactionContext context, byte resourceMgrId, long pageId,
            byte logType, byte logActionType, int requestedSpaceForLog, ILogger logger,
            Map<Object, Object> loggerArguments) throws ACIDException {
        /*
         * logLocator is a re-usable object that is appropriately set in each
         * invocation. If the reference is null, the log manager must throw an
         * exception
         */
        if (logLocator == null) {
            throw new ACIDException(
                    " you need to pass in a non-null logLocator, if you dont have it, then pass in a dummy so that the +"
                            + "log manager can set it approporiately for you");
        }

        // compute the total log size including the header and the checksum.
        int totalLogSize = logManagerProperties.getLogHeaderSize() + requestedSpaceForLog
                + logManagerProperties.getLogChecksumSize();

        // check for the total space requirement to be less than a log page.
        if (totalLogSize > logManagerProperties.getLogPageSize()) {
            throw new ACIDException(
                    " Maximum Log Content Size is "
                            + (logManagerProperties.getLogPageSize() - logManagerProperties.getLogHeaderSize() - logManagerProperties
                                    .getLogChecksumSize()));
        }

        // all constraints checked and we are goot to go and acquire a lsn.
        long previousLogLocator = -1;
        long myLogLocator; // the will be set to the location (a long value)
        // where the log record needs to be placed.

        /*
         * The logs written by a transaction need to be linked to each other for
         * a successful rollback/recovery. However there could be multiple
         * threads operating concurrently that are part of a common transaction.
         * These threads need to synchronize and record the lsn corresponding to
         * the last log record written by (any thread of) the transaction.
         */
        synchronized (context) {
            previousLogLocator = context.getLastLogLocator().getLsn();
            myLogLocator = getLsn(totalLogSize, logType);
            context.getLastLogLocator().setLsn(myLogLocator);
            logLocator.setLsn(myLogLocator);
        }

        /*
         * At this point, the transaction thread has obtained reserved space for
         * writing the log record. In doing so, it has acquired (shared)
         * ownership of the log page. All subsequent actions are under a try
         * catch block so that if any exception is encountered, a clean can be
         * performed correctly that is ownership is released.
         */

        boolean decremented = false; // indicates if the transaction thread
        // has release ownership of the
        // page.
        boolean addedFlushRequest = false; // indicates if the transaction
        // thread has submitted a flush
        // request.

        int pageIndex = (int) getLogPageIndex(myLogLocator);

        /*
         * the lsn has been obtained for the log record. need to set the
         * LogLocator instance accordingly.
         */

        try {

            logLocator.setBuffer(logPages[pageIndex]);
            int pageOffset = getLogPageOffset(myLogLocator);
            logLocator.setMemoryOffset(pageOffset);

            /*
             * write the log header.
             */
            logRecordHelper.writeLogHeader(context, logLocator, resourceMgrId, pageId, logType, logActionType,
                    requestedSpaceForLog, previousLogLocator);

            // increment the offset so that the transaction can fill up the
            // content in the correct region of the allocated space.
            logLocator.increaseMemoryOffset(logManagerProperties.getLogHeaderSize());

            // a COMMIT log record does not have any content
            // and hence the logger (responsible for putting the log content) is
            // not invoked.
            if (requestedSpaceForLog != 0) {
                logger.preLog(context, loggerArguments);
            }

            if (requestedSpaceForLog != 0) {
                // call the logger implementation and ask to fill in the log
                // record content at the allocated space.
                logger.log(context, logLocator, requestedSpaceForLog, loggerArguments);
                logger.postLog(context, loggerArguments);
            }

            /*
             * The log record has been written. For integrity checks, compute
             * the checksum and put it at the end of the log record.
             */
            int startPosChecksum = logLocator.getMemoryOffset() - logManagerProperties.getLogHeaderSize();
            int length = totalLogSize - logManagerProperties.getLogChecksumSize();
            long checksum = DataUtil.getChecksum(logPages[pageIndex], startPosChecksum, length);
            logPages[pageIndex].writeLong(pageOffset + logManagerProperties.getLogHeaderSize() + requestedSpaceForLog,
                    checksum);

            /*
             * release the ownership as the log record has been placed in
             * created space.
             */
            int pageDirtyCount = logPageOwnerCount[pageIndex].decrementAndGet();

            // indicating that the transaction thread has released ownership
            decremented = true;

            /*
             * If the transaction thread happens to be the last owner of the log
             * page the page must by marked as a candidate to be flushed.
             */
            if (pageDirtyCount == 0) {
                logPageStatus[pageIndex].set(PageState.INACTIVE);
                addFlushRequest(pageIndex);
                addedFlushRequest = true;
            }

            /*
             * If the log type is commit, a flush request is registered, if the
             * log record has not reached the disk. It may be possible that this
             * thread does not get CPU cycles and in-between the log record has
             * been flushed to disk because the containing log page filled up.
             */
            if (logType == LogType.COMMIT) {
                if (getLastFlushedLsn().get() < myLogLocator) {
                    if (!addedFlushRequest) {
                        addFlushRequest(pageIndex);
                    }

                    /*
                     * the commit log record is still in log buffer. need to
                     * wait until the containing log page is flushed. When the
                     * log flusher thread does flush the page, it notifies all
                     * waiting threads of the flush event.
                     */
                    synchronized (logPages[pageIndex]) {
                        while (getLastFlushedLsn().get() < myLogLocator) {
                            logPages[pageIndex].wait();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new ACIDException(context, "Thread: " + Thread.currentThread().getName()
                    + " logger encountered exception", e);
        } finally {
            /*
             * If an exception was encountered and we did not release ownership
             */
            if (!decremented) {
                logPageOwnerCount[pageIndex].decrementAndGet();
            }
        }
    }

    /*
     * This method resets the log page and is called by the log flusher thread
     * after a page has been flushed to disk.
     */
    public void resetLogPage(long nextWritePosition, int pageIndex) throws IOException {

        String filePath = LogUtil.getLogFilePath(logManagerProperties, getLogFileId(nextWritePosition));

        logPages[pageIndex].reset(filePath, LogUtil.getFileOffset(this, nextWritePosition),
                logManagerProperties.getLogPageSize());
    }

    @Override
    public ILogCursor readLog(ILogFilter logFilter) throws ACIDException {
        LogCursor cursor = new LogCursor(this, logFilter);
        return cursor;
    }

    @Override
    public ILogCursor readLog(PhysicalLogLocator physicalLogLocator, ILogFilter logFilter) throws IOException,
            ACIDException {
        LogCursor cursor = new LogCursor(this, physicalLogLocator, logFilter);
        return cursor;
    }

    /*
     * Read a log that is residing on the disk.
     */
    private LogicalLogLocator readDiskLog(PhysicalLogLocator physicalLogLocator) throws ACIDException {
        LogicalLogLocator logicalLogLocator;
        String filePath = LogUtil.getLogFilePath(logManagerProperties,
                LogUtil.getFileId(this, physicalLogLocator.getLsn()));
        long fileOffset = LogUtil.getFileOffset(this, physicalLogLocator.getLsn());
        ByteBuffer buffer = ByteBuffer.allocate(logManagerProperties.getLogPageSize());
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(filePath, "r");
            raf.seek(fileOffset);
            FileChannel fileChannel = raf.getChannel();
            fileChannel.read(buffer);
            buffer.position(0);
            buffer.limit(buffer.getInt(4));
            MemBasedBuffer memBuffer = new MemBasedBuffer(buffer.slice());
            logicalLogLocator = new LogicalLogLocator(physicalLogLocator.getLsn(), memBuffer, 0, this);
            if (!logRecordHelper.validateLogRecord(logManagerProperties, logicalLogLocator)) {
                throw new ACIDException(" invalid log record at lsn " + physicalLogLocator.getLsn());
            }
        } catch (Exception fnfe) {
            throw new ACIDException(" unable to retrieve log record with lsn " + physicalLogLocator.getLsn()
                    + " from the file system", fnfe);
        } finally {
            try {
                if (raf != null) {
                    raf.close();
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw new ACIDException(" exception in closing " + raf, ioe);
            }
        }
        return logicalLogLocator;
    }

    @Override
    public LogicalLogLocator readLog(PhysicalLogLocator physicalLogLocator) throws ACIDException {
        byte[] logRecord = null;
        long lsnValue = physicalLogLocator.getLsn();
        if (lsnValue > lsn.get()) {
            throw new ACIDException(" invalid lsn " + physicalLogLocator);
        }

        LogicalLogLocator logLocator = null;

        /* check if the log record in the log buffer or has reached the disk. */
        if (lsnValue > getLastFlushedLsn().get()) {
            int pageIndex = getLogPageIndex(lsnValue);
            int pageOffset = getLogPageOffset(lsnValue);
            byte[] pageContent = new byte[logManagerProperties.getLogPageSize()];
            // take a lock on the log page so that the page is not flushed to
            // disk interim
            synchronized (logPages[pageIndex]) {
                if (lsnValue > getLastFlushedLsn().get()) { // need to check
                    // again
                    // (this
                    // thread may have got
                    // de-scheduled and must
                    // refresh!)

                    // get the log record length
                    logPages[pageIndex].getBytes(pageContent, 0, pageContent.length);
                    int logRecordLength = DataUtil.byteArrayToInt(pageContent, pageOffset + 4);
                    logRecord = new byte[logRecordLength];

                    /*
                     * copy the log record content
                     */
                    System.arraycopy(pageContent, pageOffset, logRecord, 0, logRecordLength);
                    MemBasedBuffer memBuffer = new MemBasedBuffer(logRecord);
                    logLocator = new LogicalLogLocator(lsnValue, memBuffer, 0, this);
                    try {
                        // validate the log record by comparing checksums
                        if (!logRecordHelper.validateLogRecord(logManagerProperties, logLocator)) {
                            throw new ACIDException(" invalid log record at lsn " + physicalLogLocator);
                        }
                    } catch (Exception e) {
                        throw new ACIDException("exception encoutered in validating log record at lsn "
                                + physicalLogLocator, e);
                    }
                    return logLocator;
                }
            }
        }

        /*
         * the log record is residing on the disk, read it from there.
         */
        return readDiskLog(physicalLogLocator);
    }

    public ILogRecordHelper getLogRecordHelper() {
        return logRecordHelper;
    }

    /*
     * This method shall be called by the Buffer manager when it needs to evict
     * a page from the cache. TODO: Change the implementation from a looping
     * logic to event based when log manager support is integrated with the
     * Buffer Manager.
     */
    public synchronized void flushLog(LogicalLogLocator logicalLogLocator) throws ACIDException {
        if (logicalLogLocator.getLsn() > lsn.get()) {
            throw new ACIDException(" invalid lsn " + logicalLogLocator.getLsn());
        }
        while (lastFlushedLsn.get() < logicalLogLocator.getLsn());
    }

    /*
     * Map each log page to cover a physical byte range over a log file. When a
     * page is flushed, the page contents are put to disk in the corresponding
     * byte range.
     */
    private void initializeLogPages(PhysicalLogLocator physicalLogLocator) throws ACIDException {
        try {
            String filePath = LogUtil.getLogFilePath(logManagerProperties,
                    LogUtil.getFileId(this, physicalLogLocator.getLsn()));
            for (int i = 0; i < numLogPages; i++) {
                logPages[i] = FileUtil.getFileBasedBuffer(
                        filePath,
                        LogUtil.getFileOffset(this, physicalLogLocator.getLsn()) + i
                                * logManagerProperties.getLogPageSize(), logManagerProperties.getLogPageSize());
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

    public LogManagerProperties getLogManagerProperties() {
        return logManagerProperties;
    }

    public IFileBasedBuffer getLogPage(int pageIndex) {
        return logPages[pageIndex];
    }

    public AtomicInteger getLogPageOwnershipCount(int pageIndex) {
        return logPageOwnerCount[pageIndex];
    }

    public ICommitResolver getCommitResolver() {
        return commitResolver;
    }

    public CommitRequestStatistics getCommitRequestStatistics() {
        return commitRequestStatistics;
    }

    public IFileBasedBuffer[] getLogPages() {
        return logPages;
    }

    public int getLastFlushedPage() {
        return lastFlushedPage.get();
    }

    public void setLastFlushedPage(int lastFlushedPage) {
        this.lastFlushedPage.set(lastFlushedPage);
    }

    @Override
    public TransactionProvider getTransactionProvider() {
        return provider;
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

    public LogPageFlushThread(LogManager logManager) {
        this.logManager = logManager;
        setName("Flusher");
    }

    @Override
    public void run() {
        while (true) {
            try {
                int pageToFlush = logManager.getNextPageInSequence(logManager.getLastFlushedPage());

                /*
                 * A wait call on the linkedBLockingQueue. The flusher thread is
                 * notified when an object is added to the queue. Please note
                 * that each page has an associated blocking queue.
                 */
                logManager.getPendingFlushRequests(pageToFlush).take();

                /*
                 * The LogFlusher was waiting for a page to be marked as a
                 * candidate for flushing. Now that has happened. The thread
                 * shall proceed to take a lock on the log page
                 */
                synchronized (logManager.getLogPages()[pageToFlush]) {

                    /*
                     * lock the internal state of the log manager and create a
                     * log file if necessary.
                     */
                    int prevLogFileId = logManager.getLogFileId(logManager.getLastFlushedLsn().get());
                    int nextLogFileId = logManager.getLogFileId(logManager.getLastFlushedLsn().get()
                            + logManager.getLogManagerProperties().getLogPageSize());
                    if (prevLogFileId != nextLogFileId) {
                        String filePath = LogUtil.getLogFilePath(logManager.getLogManagerProperties(), nextLogFileId);
                        FileUtil.createFileIfNotExists(filePath);
                        logManager.getLogPage(pageToFlush).reset(
                                LogUtil.getLogFilePath(logManager.getLogManagerProperties(), nextLogFileId), 0,
                                logManager.getLogManagerProperties().getLogPageSize());
                    }

                    logManager.getLogPage(pageToFlush).flush(); // put the
                    // content to
                    // disk, the
                    // thread still
                    // has a lock on
                    // the log page

                    /*
                     * acquire lock on the log manager as we need to update the
                     * internal bookkeeping data.
                     */

                    // increment the last flushed lsn.
                    long lastFlushedLsn = logManager.incrementLastFlushedLsn(logManager.getLogManagerProperties()
                            .getLogPageSize());

                    /*
                     * the log manager gains back ownership of the page. this is
                     * reflected by incrementing the owner count of the page.
                     * recall that when the page is begin flushed the owner
                     * count is actually 0 Value of zero implicitly indicates
                     * that the page is operated upon by the log flusher thread.
                     */
                    logManager.getLogPageOwnershipCount(pageToFlush).incrementAndGet();

                    /*
                     * get the number of log buffers that have been written so
                     * far. A log buffer = number of log pages * size of a log
                     * page
                     */
                    int numCycles = (int) lastFlushedLsn / logManager.getLogManagerProperties().getLogBufferSize();
                    if (lastFlushedLsn % logManager.getLogManagerProperties().getLogBufferSize() == 0) {
                        numCycles--;
                    }

                    /*
                     * Map the log page to a new region in the log file.
                     */

                    long nextWritePosition = logManager.getLogPages()[pageToFlush].getNextWritePosition()
                            + logManager.getLogManagerProperties().getLogBufferSize();

                    /*
                     * long nextPos = (numCycles + 1)
                     * logManager.getLogManagerProperties() .getLogBufferSize()
                     * + pageToFlush logManager.getLogManagerProperties()
                     * .getLogPageSize();
                     */
                    logManager.resetLogPage(nextWritePosition, pageToFlush);

                    // mark the page as ACTIVE
                    logManager.getLogPageStatus(pageToFlush).set(LogManager.PageState.ACTIVE);

                    // notify all waiting (transaction) threads.
                    // Transaction thread may be waiting for the page to be
                    // available or may have a commit log record on the page
                    // that got flushed.
                    logManager.getLogPages()[pageToFlush].notifyAll();
                    logManager.setLastFlushedPage(pageToFlush);
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw new Error(" exception in flushing log page", ioe);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break; // must break from the loop as the exception indicates
                // some thing horrendous has happened elsewhere
            }
        }
    }
}

/*
 * TODO: By default the commit policy is to commit at each request and not have
 * a group commit. The following code needs to change to support group commit.
 * The code for group commit has not been tested thoroughly and is under
 * development.
 */
class BasicCommitResolver implements ICommitResolver {

    public boolean shouldCommitPage(int pageIndex, LogManager logManager,
            CommitRequestStatistics commitRequestStatistics) {
        return true;
    }

    public void init(LogManager logManager) {
    }
}

class GroupCommitResolver implements ICommitResolver {

    public boolean shouldCommitPage(int pageIndex, LogManager logManager,
            CommitRequestStatistics commitRequestStatistics) {
        long maxCommitWait = logManager.getLogManagerProperties().getGroupCommitWaitPeriod();
        long timestamp = commitRequestStatistics.getPageLevelLastCommitRequestTimestamp(pageIndex);
        if (timestamp == -1) {
            if (maxCommitWait == 0) {
                return true;
            } else {
                timestamp = System.currentTimeMillis();
            }
        }
        long currenTime = System.currentTimeMillis();
        if (currenTime - timestamp > maxCommitWait) {
            return true;
        }
        return false;
    }

    public void init(LogManager logManager) {
        GroupCommitHandlerThread groupCommitHandler = new GroupCommitHandlerThread(logManager);
        groupCommitHandler.setDaemon(true);
        groupCommitHandler.start();
    }

    class GroupCommitHandlerThread extends Thread {

        private LogManager logManager;

        public GroupCommitHandlerThread(LogManager logManager) {
            this.logManager = logManager;
            setName("Group Commit Handler");
        }

        @Override
        public void run() {
            int pageIndex = -1;
            while (true) {
                pageIndex = logManager.getNextPageInSequence(pageIndex);
                long lastCommitRequeestTimestamp = logManager.getCommitRequestStatistics()
                        .getPageLevelLastCommitRequestTimestamp(pageIndex);
                if (lastCommitRequeestTimestamp != -1
                        && System.currentTimeMillis() - lastCommitRequeestTimestamp > logManager
                                .getLogManagerProperties().getGroupCommitWaitPeriod()) {
                    int dirtyCount = logManager.getLogPageOwnershipCount(pageIndex).decrementAndGet();
                    if (dirtyCount == 0) {
                        try {
                            logManager.getLogPageStatus(pageIndex).set(LogManager.PageState.INACTIVE);
                            logManager.getPendingFlushRequests(pageIndex).put(Thread.currentThread());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            break;
                        }
                        logManager.getCommitRequestStatistics().committedPage(pageIndex);
                    }
                }
            }
        }
    }

}

interface ICommitResolver {
    public boolean shouldCommitPage(int pageIndex, LogManager logManager,
            CommitRequestStatistics commitRequestStatistics);

    public void init(LogManager logManager);
}

/**
 * Represents a collection of all commit requests by transactions for each log
 * page. The requests are accumulated until the commit policy triggers a flush
 * of the corresponding log page. Upon a flush of a page, all commit requests
 * for the page are cleared.
 */
class CommitRequestStatistics {

    AtomicInteger[] pageLevelCommitRequestCount;
    AtomicLong[] pageLevelLastCommitRequestTimestamp;

    public CommitRequestStatistics(int numPages) {
        pageLevelCommitRequestCount = new AtomicInteger[numPages];
        pageLevelLastCommitRequestTimestamp = new AtomicLong[numPages];
        for (int i = 0; i < numPages; i++) {
            pageLevelCommitRequestCount[i] = new AtomicInteger(0);
            pageLevelLastCommitRequestTimestamp[i] = new AtomicLong(-1L);
        }
    }

    public void registerCommitRequest(int pageIndex) {
        pageLevelCommitRequestCount[pageIndex].incrementAndGet();
        pageLevelLastCommitRequestTimestamp[pageIndex].set(System.currentTimeMillis());
    }

    public long getPageLevelLastCommitRequestTimestamp(int pageIndex) {
        return pageLevelLastCommitRequestTimestamp[pageIndex].get();
    }

    public void committedPage(int pageIndex) {
        pageLevelCommitRequestCount[pageIndex].set(0);
        pageLevelLastCommitRequestTimestamp[pageIndex].set(-1L);
    }

}
