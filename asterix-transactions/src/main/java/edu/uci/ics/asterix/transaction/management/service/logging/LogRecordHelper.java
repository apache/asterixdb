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
 * == LogRecordFormat ==
 * [Header]
 * --------------------------- Header part1(17) : Both COMMIT and UPDATE log type have part1 fields
 * LogMagicNumber(4)
 * LogType(1)
 * JobId(4)
 * DatasetId(4) //stored in dataset_dataset in Metadata Node
 * PKHashValue(4)
 * --------------------------- Header part2(21) : Only UPDATE log type has part2 fields
 * PrevLSN(8) //only for UPDATE
 * ResourceId(8) //stored in .metadata of the corresponding index in NC node
 * ResourceMgrId(1)
 * LogRecordSize(4)
 * --------------------------- COMMIT doesn't have Body fields.
 * [Body] The Body size is given through the parameter reusableLogContentObjectLength
 * TupleFieldCount(4)
 * NewOp(1)
 * NewValueLength(4)
 * NewValue(NewValueLength)
 * OldOp(1)
 * OldValueLength(4)
 * OldValue(OldValueLength)
 * --------------------------- Both COMMIT and UPDATE have tail fields.
 * [Tail]
 * Checksum(8)
 */
public class LogRecordHelper implements ILogRecordHelper {

    private final int LOG_CHECKSUM_SIZE = 8;

    private final int MAGIC_NO_POS = 0;
    private final int LOG_TYPE_POS = 4;
    private final int JOB_ID_POS = 5;
    private final int DATASET_ID_POS = 9;
    private final int PK_HASH_VALUE_POS = 13;
    private final int PREV_LSN_POS = 17;
    private final int RESOURCE_ID_POS = 25;
    private final int RESOURCE_MGR_ID_POS = 33;
    private final int LOG_RECORD_SIZE_POS = 34;

    private ILogManager logManager;

    public LogRecordHelper(ILogManager logManager) {
        this.logManager = logManager;
    }

    @Override
    public byte getLogType(LogicalLogLocator logicalLogLocator) {
        return logicalLogLocator.getBuffer().getByte(logicalLogLocator.getMemoryOffset() + LOG_TYPE_POS);
    }

    @Override
    public int getJobId(LogicalLogLocator logicalLogLocator) {
        return logicalLogLocator.getBuffer().readInt(logicalLogLocator.getMemoryOffset() + JOB_ID_POS);
    }

    @Override
    public int getDatasetId(LogicalLogLocator logicalLogLocator) {
        return logicalLogLocator.getBuffer().readInt(logicalLogLocator.getMemoryOffset() + DATASET_ID_POS);
    }

    @Override
    public int getPKHashValue(LogicalLogLocator logicalLogLocator) {
        return logicalLogLocator.getBuffer().readInt(logicalLogLocator.getMemoryOffset() + PK_HASH_VALUE_POS);
    }

    @Override
    public PhysicalLogLocator getPrevLSN(LogicalLogLocator logicalLogLocator) {
        long prevLsnValue = (logicalLogLocator.getBuffer())
                .readLong(logicalLogLocator.getMemoryOffset() + PREV_LSN_POS);
        PhysicalLogLocator previousLogLocator = new PhysicalLogLocator(prevLsnValue, logManager);
        return previousLogLocator;
    }

    @Override
    public boolean getPrevLSN(PhysicalLogLocator physicalLogLocator, LogicalLogLocator logicalLogLocator) {
        long prevLsnValue = (logicalLogLocator.getBuffer())
                .readLong(logicalLogLocator.getMemoryOffset() + PREV_LSN_POS);
        if (prevLsnValue == -1) {
            return false;
        }
        physicalLogLocator.setLsn(prevLsnValue);
        return true;
    }

    @Override
    public long getResourceId(LogicalLogLocator logicalLogLocator) {
        return logicalLogLocator.getBuffer().readLong(logicalLogLocator.getMemoryOffset() + RESOURCE_ID_POS);
    }

    @Override
    public byte getResourceMgrId(LogicalLogLocator logicalLogLocater) {
        return logicalLogLocater.getBuffer().getByte(logicalLogLocater.getMemoryOffset() + RESOURCE_MGR_ID_POS);
    }

    @Override
    public int getLogContentSize(LogicalLogLocator logicalLogLocater) {
        return logicalLogLocater.getBuffer().readInt(logicalLogLocater.getMemoryOffset() + LOG_RECORD_SIZE_POS);
    }

    @Override
    public long getLogChecksum(LogicalLogLocator logicalLogLocator) {
        return (logicalLogLocator.getBuffer()).readLong(logicalLogLocator.getMemoryOffset()
                + getLogRecordSize(getLogType(logicalLogLocator), getLogContentSize(logicalLogLocator))
                - LOG_CHECKSUM_SIZE);
    }

    @Override
    public int getLogContentBeginPos(LogicalLogLocator logicalLogLocator) {
        return logicalLogLocator.getMemoryOffset() + getLogHeaderSize(getLogType(logicalLogLocator));
    }

    @Override
    public int getLogContentEndPos(LogicalLogLocator logicalLogLocator) {
        return logicalLogLocator.getMemoryOffset()
                + getLogRecordSize(getLogType(logicalLogLocator), getLogContentSize(logicalLogLocator))
                - LOG_CHECKSUM_SIZE;
    }

    @Override
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
            case LogType.ENTITY_COMMIT:
                logTypeDisplay = "ENTITY_COMMIT";
                break;
        }
        builder.append(" LSN : ").append(logicalLogLocator.getLsn());
        builder.append(" Log Type : ").append(logTypeDisplay);
        builder.append(" Job Id : ").append(getJobId(logicalLogLocator));
        builder.append(" Dataset Id : ").append(getDatasetId(logicalLogLocator));
        builder.append(" PK Hash Value : ").append(getPKHashValue(logicalLogLocator));
        if (logType == LogType.UPDATE) {
            builder.append(" PrevLSN : ").append(getPrevLSN(logicalLogLocator).getLsn());
            builder.append(" Resource Id : ").append(getResourceId(logicalLogLocator));
            builder.append(" ResourceMgr Id : ").append(getResourceMgrId(logicalLogLocator));
            builder.append(" Log Record Size : ").append(
                    getLogRecordSize(logType, getLogContentSize(logicalLogLocator)));
        }
        return builder.toString();
    }

    @Override
    public void writeLogHeader(LogicalLogLocator logicalLogLocator, byte logType, TransactionContext context,
            int datasetId, int PKHashValue, long prevLogicalLogLocator, long resourceId, byte resourceMgrId,
            int logRecordSize) {

        /* magic no */
        (logicalLogLocator.getBuffer()).writeInt(logicalLogLocator.getMemoryOffset() + MAGIC_NO_POS,
                logManager.getLogManagerProperties().LOG_MAGIC_NUMBER);

        /* log type */
        (logicalLogLocator.getBuffer()).put(logicalLogLocator.getMemoryOffset() + LOG_TYPE_POS, logType);

        /* jobId */
        (logicalLogLocator.getBuffer()).writeInt(logicalLogLocator.getMemoryOffset() + JOB_ID_POS, context.getJobId()
                .getId());

        /* datasetId */
        (logicalLogLocator.getBuffer()).writeInt(logicalLogLocator.getMemoryOffset() + DATASET_ID_POS, datasetId);

        /* PK hash value */
        (logicalLogLocator.getBuffer()).writeInt(logicalLogLocator.getMemoryOffset() + PK_HASH_VALUE_POS, PKHashValue);

        if (logType == LogType.UPDATE) {
            /* prevLSN */
            (logicalLogLocator.getBuffer()).writeLong(logicalLogLocator.getMemoryOffset() + PREV_LSN_POS,
                    prevLogicalLogLocator);

            /* resourceId */
            (logicalLogLocator.getBuffer())
                    .writeLong(logicalLogLocator.getMemoryOffset() + RESOURCE_ID_POS, resourceId);

            /* resourceMgr id */
            (logicalLogLocator.getBuffer()).put(logicalLogLocator.getMemoryOffset() + RESOURCE_MGR_ID_POS,
                    resourceMgrId);

            /* log record size */
            (logicalLogLocator.getBuffer()).writeInt(logicalLogLocator.getMemoryOffset() + LOG_RECORD_SIZE_POS,
                    logRecordSize);

        }
    }

    @Override
    public boolean validateLogRecord(LogicalLogLocator logicalLogLocator) {
        int logLength = this.getLogRecordSize(getLogType(logicalLogLocator), getLogContentSize(logicalLogLocator));
        long expectedChecksum = DataUtil.getChecksum(logicalLogLocator.getBuffer(),
                logicalLogLocator.getMemoryOffset(), logLength - LOG_CHECKSUM_SIZE);
        long actualChecksum = getLogChecksum(logicalLogLocator);
        return expectedChecksum == actualChecksum;
    }

    /**
     * @param logType
     * @param logBodySize
     * @return
     */
    @Override
    public int getLogRecordSize(byte logType, int logBodySize) {
        if (logType == LogType.UPDATE) {
            return 46 + logBodySize;
        } else {
            return 25;
        }
    }

    @Override
    public int getLogHeaderSize(byte logType) {
        if (logType == LogType.UPDATE) {
            return 38;
        } else {
            return 17;
        }
    }

    @Override
    public int getLogChecksumSize() {
        return LOG_CHECKSUM_SIZE;
    }
}
