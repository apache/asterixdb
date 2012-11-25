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
    private IFileBasedBuffer readOnlyBuffer;
    private LogicalLogLocator logicalLogLocator = null;
    private int bufferIndex = 0;
    private boolean firstNext = true;
    private long readLSN = 0;

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
        if (startingPhysicalLogLocator.getLsn() > logManager.getLastFlushedLsn().get()) {
            readLSN = startingPhysicalLogLocator.getLsn();
        } else {
            //read from disk
            readOnlyBuffer = getReadOnlyBuffer(startingPhysicalLogLocator.getLsn(), logManager
                    .getLogManagerProperties().getLogBufferSize());
            logicalLogLocator = new LogicalLogLocator(startingPhysicalLogLocator.getLsn(), readOnlyBuffer, 0,
                    logManager);
            readLSN = logicalLogLocator.getLsn();
        }
        return;
    }

    private IFileBasedBuffer getReadOnlyBuffer(long lsn, int size) throws IOException {
        int fileId = (int) (lsn / logManager.getLogManagerProperties().getLogPartitionSize());
        String filePath = LogUtil.getLogFilePath(logManager.getLogManagerProperties(), fileId);
        File file = new File(filePath);
        if (file.exists()) {
            return FileUtil.getFileBasedBuffer(filePath, lsn, size);
        } else {
            return null;
        }
    }

    /**
     * Moves the cursor to the next log record that satisfies the configured
     * filter. The parameter nextLogLocator is set to the point to the next log
     * record.
     * 
     * @param nextLogicalLogLocator
     * @return true if the cursor was successfully moved to the next log record
     *         false if there are no more log records that satisfy the
     *         configured filter.
     */
    @Override
    public boolean next(LogicalLogLocator nextLogicalLogLocator) throws IOException, ACIDException {

        int integerRead = -1;
        boolean logRecordBeginPosFound = false;
        long bytesSkipped = 0;

        if (readLSN > logManager.getLastFlushedLsn().get()) {
            readFromMemory(readLSN, nextLogicalLogLocator);
            return true;
        }

        //check whether the currentOffset has enough space to have new log record by comparing
        //the smallest log record type(which is commit)'s log header.
        while (logicalLogLocator.getMemoryOffset() <= readOnlyBuffer.getSize()
                - logManager.getLogRecordHelper().getLogHeaderSize(LogType.COMMIT)) {
            integerRead = readOnlyBuffer.readInt(logicalLogLocator.getMemoryOffset());
            if (integerRead == logManager.getLogManagerProperties().logMagicNumber) {
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
        }

        if (!logRecordBeginPosFound) {
            // need to reload the buffer
            long lsnpos = (++bufferIndex * logManager.getLogManagerProperties().getLogBufferSize());
            readOnlyBuffer = getReadOnlyBuffer(lsnpos, logManager.getLogManagerProperties().getLogBufferSize());
            if (readOnlyBuffer != null) {
                logicalLogLocator.setBuffer(readOnlyBuffer);
                logicalLogLocator.setLsn(lsnpos);
                logicalLogLocator.setMemoryOffset(0);
                return next(nextLogicalLogLocator);
            } else {
                return false;
            }
        }

        int logLength = logManager.getLogRecordHelper().getLogRecordSize(
                logManager.getLogRecordHelper().getLogType(logicalLogLocator),
                logManager.getLogRecordHelper().getLogContentSize(logicalLogLocator));
        if (logManager.getLogRecordHelper().validateLogRecord(logicalLogLocator)) {
            if (nextLogicalLogLocator == null) {
                nextLogicalLogLocator = new LogicalLogLocator(0, readOnlyBuffer, -1, logManager);
            }
            nextLogicalLogLocator.setLsn(logicalLogLocator.getLsn());
            nextLogicalLogLocator.setMemoryOffset(logicalLogLocator.getMemoryOffset());
            nextLogicalLogLocator.setBuffer(readOnlyBuffer);
            logicalLogLocator.incrementLsn(logLength);
            logicalLogLocator.setMemoryOffset(logicalLogLocator.getMemoryOffset() + logLength);
        } else {
            throw new ACIDException("Invalid Log Record found ! checksums do not match :( ");
        }
        return logFilter.accept(readOnlyBuffer, nextLogicalLogLocator.getMemoryOffset(), logLength);
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

    private void readFromMemory(long lsn, LogicalLogLocator currentLogLocator) throws ACIDException {
        byte[] logRecord = null;
        if (lsn > logManager.getCurrentLsn().get()) {
            throw new ACIDException(" invalid lsn " + lsn);
        }

        /* check if the log record in the log buffer or has reached the disk. */
        int pageIndex = logManager.getLogPageIndex(lsn);
        int pageOffset = logManager.getLogPageOffset(lsn);

        byte[] pageContent = new byte[logManager.getLogManagerProperties().getLogPageSize()];
        // take a lock on the log page so that the page is not flushed to
        // disk interim
        IFileBasedBuffer logPage = logManager.getLogPage(pageIndex);
        int logRecordSize = 0;
        synchronized (logPage) {
            // need to check again
            if (lsn > logManager.getLastFlushedLsn().get()) {
                // get the log record length
                logPage.getBytes(pageContent, 0, pageContent.length);
                byte logType = pageContent[pageOffset + 4];
                int logHeaderSize = logManager.getLogRecordHelper().getLogHeaderSize(logType);
                int logBodySize = DataUtil.byteArrayToInt(pageContent, pageOffset + logHeaderSize - 4);
                logRecordSize = logHeaderSize + logBodySize + logManager.getLogRecordHelper().getLogChecksumSize();
                logRecord = new byte[logRecordSize];

                //copy the log content
                System.arraycopy(pageContent, pageOffset, logRecord, 0, logRecordSize);
                MemBasedBuffer memBuffer = new MemBasedBuffer(logRecord);
                if (logicalLogLocator == null) {
                    logicalLogLocator = new LogicalLogLocator(lsn, memBuffer, 0, logManager);
                } else {
                    logicalLogLocator.setLsn(lsn);
                    logicalLogLocator.setBuffer(memBuffer);
                    logicalLogLocator.setMemoryOffset(0);
                }
                
                currentLogLocator.setLsn(lsn);
                currentLogLocator.setBuffer(memBuffer);
                currentLogLocator.setMemoryOffset(0);
                
                try {
                    // validate the log record by comparing checksums
                    if (!logManager.getLogRecordHelper().validateLogRecord(logicalLogLocator)) {
                        throw new ACIDException(" invalid log record at lsn " + lsn);
                    }
                } catch (Exception e) {
                    throw new ACIDException("exception encoutered in validating log record at lsn " + lsn, e);
                }
            }
        }
        readLSN = readLSN + logRecordSize;
    }
}
