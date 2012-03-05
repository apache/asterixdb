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

    private final ILogManager logManager;
    private final ILogFilter logFilter;
    private IFileBasedBuffer readOnlyBuffer;
    private LogicalLogLocator logicalLogLocator = null;
    private int bufferIndex = 0;

    /**
     * @param logFilter
     */
    public LogCursor(final ILogManager logManager, ILogFilter logFilter) throws ACIDException {
        this.logFilter = logFilter;
        this.logManager = logManager;

    }

    public LogCursor(final ILogManager logManager, PhysicalLogLocator startingPhysicalLogLocator, ILogFilter logFilter)
            throws IOException {
        this.logFilter = logFilter;
        this.logManager = logManager;
        initialize(startingPhysicalLogLocator);
    }

    private void initialize(final PhysicalLogLocator startingPhysicalLogLocator) throws IOException {
        readOnlyBuffer = getReadOnlyBuffer(startingPhysicalLogLocator.getLsn(), logManager.getLogManagerProperties()
                .getLogBufferSize());
        logicalLogLocator = new LogicalLogLocator(startingPhysicalLogLocator.getLsn(), readOnlyBuffer, 0, logManager);

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
        while (logicalLogLocator.getMemoryOffset() <= readOnlyBuffer.getSize()
                - logManager.getLogManagerProperties().getLogHeaderSize()) {
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

        int logLength = logManager.getLogRecordHelper().getLogLength(logicalLogLocator);
        if (logManager.getLogRecordHelper().validateLogRecord(logManager.getLogManagerProperties(), logicalLogLocator)) {
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

}
