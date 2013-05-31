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

package edu.uci.ics.asterix.common.transactions;

/**
 * Helper class for writing/reading of log header and checksum as well as
 * validating log record by checksum comparison. Every ILogManager
 * implementation has an associated ILogRecordHelper implementation.
 */

public interface ILogRecordHelper {

    public byte getLogType(LogicalLogLocator logicalLogLocator);

    public int getJobId(LogicalLogLocator logicalLogLocator);

    public int getDatasetId(LogicalLogLocator logicalLogLocator);

    public int getPKHashValue(LogicalLogLocator logicalLogLocator);

    public PhysicalLogLocator getPrevLSN(LogicalLogLocator logicalLogLocator);

    public boolean getPrevLSN(PhysicalLogLocator physicalLogLocator, LogicalLogLocator logicalLogLocator);

    public long getResourceId(LogicalLogLocator logicalLogLocator);

    public byte getResourceMgrId(LogicalLogLocator logicalLogLocater);

    public int getLogContentSize(LogicalLogLocator logicalLogLocater);

    public long getLogChecksum(LogicalLogLocator logicalLogLocator);

    public int getLogContentBeginPos(LogicalLogLocator logicalLogLocator);

    public int getLogContentEndPos(LogicalLogLocator logicalLogLocator);

    public String getLogRecordForDisplay(LogicalLogLocator logicalLogLocator);

    public void writeLogHeader(LogicalLogLocator logicalLogLocator, byte logType, ITransactionContext context,
            int datasetId, int PKHashValue, long prevLogicalLogLocator, long resourceId, byte resourceMgrId,
            int logRecordSize);

    public boolean validateLogRecord(LogicalLogLocator logicalLogLocator);

    public int getLogRecordSize(byte logType, int logBodySize);

    public int getLogHeaderSize(byte logType);

    public int getLogChecksumSize();

    public int getCommitLogSize();

}