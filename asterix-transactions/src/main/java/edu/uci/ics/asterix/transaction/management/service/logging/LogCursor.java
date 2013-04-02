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
import java.io.IOException;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;

public class LogCursor implements ILogCursor {

    private final LogManager logManager;
    private final ILogFilter logFilter;
    private IBuffer readOnlyBuffer;
    private LogicalLogLocator logicalLogLocator = null;
    private long bufferIndex = 0;
    private boolean firstNext = true;
    private boolean readMemory = false;
    private long readLSN = 0;
    private boolean needReloadBuffer = true;

    /**
     * @param logFilter
     */
    public LogCursor(final LogManager logManager, ILogFilter logFilter) throws ACIDException {
        this.logFilter = logFilter;
        this.logManager = logManager;

    }

    public LogCursor(final LogManager logManager, PhysicalLogLocator startingPhysicalLogLocator, ILogFilter logFilter)
            throws IOException, ACIDException {
        this.logFilter = logFilter;
        this.logManager = logManager;
        initialize(startingPhysicalLogLocator);
    }

    private void initialize(final PhysicalLogLocator startingPhysicalLogLocator) throws IOException, ACIDException {
        logicalLogLocator = new LogicalLogLocator(startingPhysicalLogLocator.getLsn(), null, 0, logManager);
    }

    private IFileBasedBuffer getReadOnlyBuffer(long lsn, int size) throws IOException {
        int fileId = (int) (lsn / logManager.getLogManagerProperties().getLogPartitionSize());
        String filePath = LogUtil.getLogFilePath(logManager.getLogManagerProperties(), fileId);
        File file = new File(filePath);
        if (file.exists()) {
            return FileUtil.getFileBasedBuffer(filePath, lsn
                    % logManager.getLogManagerProperties().getLogPartitionSize(), size);
        } else {
            return null;
        }
    }

    /**
     * Moves the cursor to the next log record that satisfies the configured
     * filter. The parameter nextLogLocator is set to the point to the next log
     * record.
     * 
     * @param currentLogLocator
     * @return true if the cursor was successfully moved to the next log record
     *         false if there are no more log records that satisfy the
     *         configured filter.
     */
    @Override
    public boolean next(LogicalLogLocator currentLogLocator) throws IOException, ACIDException {

        //TODO
        //Test the correctness when multiple log files are created
        int integerRead = -1;
        boolean logRecordBeginPosFound = false;
        long bytesSkipped = 0;

        //if the lsn to read is greater than or equal to the most recent lsn, then return false
        if (logicalLogLocator.getLsn() >= logManager.getCurrentLsn().get()) {
            return false;
        }

        //if the lsn to read is greater than the last flushed lsn, then read from memory
        if (logicalLogLocator.getLsn() > logManager.getLastFlushedLsn().get()) {
            return readFromMemory(currentLogLocator);
        }

        //if the readOnlyBuffer should be reloaded, then load the log page from the log file.
        //needReloadBuffer is set to true if the log record is read from the memory log page.
        if (needReloadBuffer) {
            //log page size doesn't exceed integer boundary
            int offset = (int)(logicalLogLocator.getLsn() % logManager.getLogManagerProperties().getLogPageSize());
            long adjustedLSN = logicalLogLocator.getLsn() - offset;
            readOnlyBuffer = getReadOnlyBuffer(adjustedLSN, logManager.getLogManagerProperties()
                    .getLogPageSize());
            logicalLogLocator.setBuffer(readOnlyBuffer);
            logicalLogLocator.setMemoryOffset(offset);
            needReloadBuffer = false;
        }

        //check whether the currentOffset has enough space to have new log record by comparing
        //the smallest log record type(which is commit)'s log header.
        while (logicalLogLocator.getMemoryOffset() <= readOnlyBuffer.getSize()
                - logManager.getLogRecordHelper().getLogHeaderSize(LogType.COMMIT)) {
            integerRead = readOnlyBuffer.readInt(logicalLogLocator.getMemoryOffset());
            if (integerRead == logManager.getLogManagerProperties().LOG_MAGIC_NUMBER) {
                logRecordBeginPosFound = true;
                break;
            }
            logicalLogLocator.increaseMemoryOffset(1);
            logicalLogLocator.incrementLsn();
            bytesSkipped++;
            if (bytesSkipped > logManager.getLogManagerProperties().getLogPageSize()) {
                return false; // the maximum size of a log record is limited to
                // a log page size. If we have skipped as many
                // bytes without finding a log record, it
                // indicates an absence of logs any further.
            }

            if (logicalLogLocator.getLsn() > logManager.getLastFlushedLsn().get()) {
                return next(currentLogLocator); //should read from memory if there is any further log
            }
        }

        if (!logRecordBeginPosFound) {
            // need to reload the buffer
            // TODO
            // reduce IO by reading more pages(equal to logBufferSize) at a time.
            long lsnpos = ((logicalLogLocator.getLsn() / logManager.getLogManagerProperties().getLogPageSize()) + 1)
                    * logManager.getLogManagerProperties().getLogPageSize();

            readOnlyBuffer = getReadOnlyBuffer(lsnpos, logManager.getLogManagerProperties().getLogPageSize());
            if (readOnlyBuffer != null) {
                logicalLogLocator.setBuffer(readOnlyBuffer);
                logicalLogLocator.setLsn(lsnpos);
                logicalLogLocator.setMemoryOffset(0);
                return next(currentLogLocator);
            } else {
                return false;
            }
        }

        int logLength = logManager.getLogRecordHelper().getLogRecordSize(
                logManager.getLogRecordHelper().getLogType(logicalLogLocator),
                logManager.getLogRecordHelper().getLogContentSize(logicalLogLocator));
        if (logManager.getLogRecordHelper().validateLogRecord(logicalLogLocator)) {
            if (currentLogLocator == null) {
                currentLogLocator = new LogicalLogLocator(0, readOnlyBuffer, -1, logManager);
            }
            currentLogLocator.setLsn(logicalLogLocator.getLsn());
            currentLogLocator.setMemoryOffset(logicalLogLocator.getMemoryOffset());
            currentLogLocator.setBuffer(readOnlyBuffer);
            logicalLogLocator.incrementLsn(logLength);
            logicalLogLocator.setMemoryOffset(logicalLogLocator.getMemoryOffset() + logLength);
        } else {
            throw new ACIDException("Invalid Log Record found ! checksums do not match :( ");
        }
        return logFilter.accept(readOnlyBuffer, currentLogLocator.getMemoryOffset(), logLength);
    }

    /**
     * Returns the filter associated with the cursor.
     * 
     * @return ILogFilter
     */
    @Override
    public ILogFilter getLogFilter() {
        return logFilter;
    }

    private boolean readFromMemory(LogicalLogLocator currentLogLocator) throws ACIDException, IOException {
        byte[] logRecord = null;
        long lsn = logicalLogLocator.getLsn();

        //set the needReloadBuffer to true
        needReloadBuffer = true;

        int pageIndex = logManager.getLogPageIndex(lsn);
        logicalLogLocator.setMemoryOffset(logManager.getLogPageOffset(lsn));

        // take a lock on the log page so that the page is not flushed to
        // disk interim
        IFileBasedBuffer logPage = logManager.getLogPage(pageIndex);
        synchronized (logPage) {
            // need to check again if the log record in the log buffer or has reached the disk
            if (lsn > logManager.getLastFlushedLsn().get()) {

                //find the magic number to identify the start of the log record
                //----------------------------------------------------------------
                int readNumber = -1;
                int logPageSize = logManager.getLogManagerProperties().getLogPageSize();
                int logMagicNumber = logManager.getLogManagerProperties().LOG_MAGIC_NUMBER;
                int bytesSkipped = 0;
                boolean logRecordBeginPosFound = false;
                //check whether the currentOffset has enough space to have new log record by comparing
                //the smallest log record type(which is commit)'s log header.
                while (logicalLogLocator.getMemoryOffset() <= logPageSize
                        - logManager.getLogRecordHelper().getLogHeaderSize(LogType.COMMIT)) {
                    readNumber = logPage.readInt(logicalLogLocator.getMemoryOffset());
                    if (readNumber == logMagicNumber) {
                        logRecordBeginPosFound = true;
                        break;
                    }
                    logicalLogLocator.increaseMemoryOffset(1);
                    logicalLogLocator.incrementLsn();
                    bytesSkipped++;
                    if (bytesSkipped > logPageSize) {
                        return false; // the maximum size of a log record is limited to
                        // a log page size. If we have skipped as many
                        // bytes without finding a log record, it
                        // indicates an absence of logs any further.
                    }
                }

                if (!logRecordBeginPosFound) {
                    // need to read the next log page
                    readOnlyBuffer = null;
                    logicalLogLocator.setBuffer(null);
                    logicalLogLocator.setLsn(lsn / logPageSize + 1);
                    logicalLogLocator.setMemoryOffset(0);
                    return next(currentLogLocator);
                }
                //------------------------------------------------------

                logicalLogLocator.setBuffer(logPage);
                int logLength = logManager.getLogRecordHelper().getLogRecordSize(
                        logManager.getLogRecordHelper().getLogType(logicalLogLocator),
                        logManager.getLogRecordHelper().getLogContentSize(logicalLogLocator));
                logRecord = new byte[logLength];

                //copy the log record and set the buffer of logical log locator to the buffer of the copied log record.
                System.arraycopy(logPage.getArray(), logicalLogLocator.getMemoryOffset(), logRecord, 0, logLength);
                MemBasedBuffer memBuffer = new MemBasedBuffer(logRecord);
                readOnlyBuffer = memBuffer;
                logicalLogLocator.setBuffer(readOnlyBuffer);
                logicalLogLocator.setMemoryOffset(0);

                if (logManager.getLogRecordHelper().validateLogRecord(logicalLogLocator)) {
                    if (currentLogLocator == null) {
                        currentLogLocator = new LogicalLogLocator(0, readOnlyBuffer, -1, logManager);
                    }
                    currentLogLocator.setLsn(logicalLogLocator.getLsn());
                    currentLogLocator.setMemoryOffset(logicalLogLocator.getMemoryOffset());
                    currentLogLocator.setBuffer(readOnlyBuffer);
                    logicalLogLocator.incrementLsn(logLength);
                    logicalLogLocator.setMemoryOffset(logicalLogLocator.getMemoryOffset() + logLength);
                } else {
                    //if the checksum doesn't match, there is two possible scenario. 
                    //case1) the log file corrupted: there's nothing we can do for this case during abort. 
                    //case2) the log record is partially written by another thread. So, we may ignore this log record 
                    //       and continue to read the next log record
                    //[NOTICE]
                    //Only case2 is handled here. 
                    logicalLogLocator.incrementLsn(logLength);
                    logicalLogLocator.setMemoryOffset(logicalLogLocator.getMemoryOffset() + logLength);
                    return next(currentLogLocator);
                }
                return logFilter.accept(readOnlyBuffer, currentLogLocator.getMemoryOffset(), logLength);

            } else {
                return next(currentLogLocator);//read from disk
            }
        }
    }
}
