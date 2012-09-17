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

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;

/**
 * An implementation of the @see ILogRecordHelper interface that provides API
 * for writing/reading of log header and checksum as well as validating log
 * record by checksum comparison. Every ILogManager implementation has an
 * associated ILogRecordHelper implementation.
 */
public class LogRecordHelper implements ILogRecordHelper {

    private final int BEGIN_MAGIC_NO_POS = 0;
    private final int BEGING_LENGTH_POS = 4;
    private final int BEGIN_TYPE_POS = 8;
    private final int BEGIN_ACTION_TYPE_POS = 9;
    private final int BEGIN_TIMESTAMP_POS = 10;
    private final int BEGIN_TRANSACTION_ID_POS = 18;
    /*
    private final int BEGIN_RESOURCE_MGR_ID_POS = 26;
    private final int BEGIN_PAGE_ID_POS = 27;
    private final int BEGIN_PREV_LSN_POS = 35;
    */
    private final int BEGIN_RESOURCE_MGR_ID_POS = 22;
    private final int BEGIN_PAGE_ID_POS = 23;
    private final int BEGIN_PREV_LSN_POS = 31;

    
    private ILogManager logManager;

    public LogRecordHelper(ILogManager logManager) {
        this.logManager = logManager;
    }

    public byte getLogType(LogicalLogLocator logicalLogLocator) {
        return logicalLogLocator.getBuffer().getByte(logicalLogLocator.getMemoryOffset() + BEGIN_TYPE_POS);
    }

    public byte getLogActionType(LogicalLogLocator logicalLogLocator) {
        return logicalLogLocator.getBuffer().getByte(logicalLogLocator.getMemoryOffset() + BEGIN_ACTION_TYPE_POS);
    }

    public int getLogLength(LogicalLogLocator logicalLogLocator) {
        return (logicalLogLocator.getBuffer()).readInt(logicalLogLocator.getMemoryOffset() + BEGING_LENGTH_POS);
    }

    public long getLogTimestamp(LogicalLogLocator logicalLogLocator) {
        return (logicalLogLocator.getBuffer()).readLong(logicalLogLocator.getMemoryOffset() + BEGIN_TIMESTAMP_POS);
    }

    public long getLogChecksum(LogicalLogLocator logicalLogLocator) {
        return (logicalLogLocator.getBuffer()).readLong(logicalLogLocator.getMemoryOffset()
                + getLogLength(logicalLogLocator) - 8);
    }

    public long getLogTransactionId(LogicalLogLocator logicalLogLocator) {
        return (logicalLogLocator.getBuffer()).readInt(logicalLogLocator.getMemoryOffset() + BEGIN_TRANSACTION_ID_POS);
    }

    public byte getResourceMgrId(LogicalLogLocator logicalLogLocator) {
        return (logicalLogLocator.getBuffer()).getByte(logicalLogLocator.getMemoryOffset() + BEGIN_RESOURCE_MGR_ID_POS);
    }

    public long getPageId(LogicalLogLocator logicalLogLocator) {
        return (logicalLogLocator.getBuffer()).readLong(logicalLogLocator.getMemoryOffset() + BEGIN_PAGE_ID_POS);
    }

    public int getLogContentBeginPos(LogicalLogLocator logicalLogLocator) {
        return logicalLogLocator.getMemoryOffset() + logManager.getLogManagerProperties().getLogHeaderSize();
    }

    public int getLogContentEndPos(LogicalLogLocator logicalLogLocator) {
        return logicalLogLocator.getMemoryOffset() + getLogLength(logicalLogLocator)
                - logManager.getLogManagerProperties().getLogChecksumSize();
    }

    public PhysicalLogLocator getPreviousLsnByTransaction(LogicalLogLocator logicalLogLocator) {
        long prevLsnValue = (logicalLogLocator.getBuffer()).readLong(logicalLogLocator.getMemoryOffset()
                + BEGIN_PREV_LSN_POS);
        PhysicalLogLocator previousLogLocator = new PhysicalLogLocator(prevLsnValue, logManager);
        return previousLogLocator;
    }

    public boolean getPreviousLsnByTransaction(PhysicalLogLocator physicalLogLocator,
            LogicalLogLocator logicalLogLocator) {
        long prevLsnValue = (logicalLogLocator.getBuffer()).readLong(logicalLogLocator.getMemoryOffset()
                + BEGIN_PREV_LSN_POS);
        if (prevLsnValue == -1) {
            return false;
        }
        physicalLogLocator.setLsn(prevLsnValue);
        return true;
    }

    public String getLogRecordForDisplay(LogicalLogLocator logicalLogLocator) {
        StringBuilder builder = new StringBuilder();
        byte logType = new Byte(getLogType(logicalLogLocator));
        String logTypeDisplay = null;
        switch (logType) {
            case LogType.COMMIT:
                logTypeDisplay = "COMMIT";
                break;
            case LogType.UPDATE:
                logTypeDisplay = "UPDATE";
                break;
        }
        builder.append(" Log Type :" + logTypeDisplay);
        builder.append(" Log Length :" + getLogLength(logicalLogLocator));
        builder.append(" Log Timestamp:" + getLogTimestamp(logicalLogLocator));
        builder.append(" Log Transaction Id:" + getLogTransactionId(logicalLogLocator));
        builder.append(" Log Resource Mgr Id:" + getResourceMgrId(logicalLogLocator));
        builder.append(" Page Id:" + getPageId(logicalLogLocator));
        builder.append(" Log Checksum:" + getLogChecksum(logicalLogLocator));
        builder.append(" Log Previous lsn: " + getPreviousLsnByTransaction(logicalLogLocator));
        return new String(builder);
    }

    public void writeLogHeader(TransactionContext context, LogicalLogLocator logicalLogLocator, byte resourceMgrId,
            long pageId, byte logType, byte logActionType, int logContentSize, long prevLogicalLogLocator) {
        /* magic no */
        (logicalLogLocator.getBuffer()).writeInt(logicalLogLocator.getMemoryOffset() + BEGIN_MAGIC_NO_POS,
                logManager.getLogManagerProperties().logMagicNumber);

        /* length */
        int length = logManager.getLogManagerProperties().getTotalLogRecordLength(logContentSize);
        (logicalLogLocator.getBuffer()).writeInt(logicalLogLocator.getMemoryOffset() + BEGING_LENGTH_POS, length);

        /* log type */
        (logicalLogLocator.getBuffer()).put(logicalLogLocator.getMemoryOffset() + BEGIN_TYPE_POS, logType);

        /* log action type */
        (logicalLogLocator.getBuffer()).put(logicalLogLocator.getMemoryOffset() + BEGIN_ACTION_TYPE_POS, logActionType);

        /* timestamp */
        long timestamp = System.currentTimeMillis();
        (logicalLogLocator.getBuffer()).writeLong(logicalLogLocator.getMemoryOffset() + BEGIN_TIMESTAMP_POS, timestamp);

        /* transaction id */
        (logicalLogLocator.getBuffer()).writeInt(logicalLogLocator.getMemoryOffset() + BEGIN_TRANSACTION_ID_POS,
                context.getJobId().getId());

        /* resource Mgr id */
        (logicalLogLocator.getBuffer()).put(logicalLogLocator.getMemoryOffset() + BEGIN_RESOURCE_MGR_ID_POS,
                resourceMgrId);

        /* page id */
        (logicalLogLocator.getBuffer()).writeLong(logicalLogLocator.getMemoryOffset() + BEGIN_PAGE_ID_POS, pageId);

        /* previous LSN's File Id by the transaction */
        (logicalLogLocator.getBuffer()).writeLong(logicalLogLocator.getMemoryOffset() + BEGIN_PREV_LSN_POS,
                prevLogicalLogLocator);
    }

    public void writeLogTail(LogicalLogLocator logicalLogLocator, ILogManager logManager) {
        (logicalLogLocator.getBuffer()).writeInt(logicalLogLocator.getMemoryOffset(),
                logManager.getLogManagerProperties().logMagicNumber);
    }

    @Override
    public boolean validateLogRecord(LogManagerProperties logManagerProperties, LogicalLogLocator logicalLogLocator) {
        int logLength = this.getLogLength(logicalLogLocator);
        long expectedChecksum = DataUtil.getChecksum(logicalLogLocator.getBuffer(),
                logicalLogLocator.getMemoryOffset(), logLength - logManagerProperties.getLogChecksumSize());
        long actualChecksum = logicalLogLocator.getBuffer().readLong(
                logicalLogLocator.getMemoryOffset() + logLength - logManagerProperties.getLogChecksumSize());
        return expectedChecksum == actualChecksum;
    }

}
