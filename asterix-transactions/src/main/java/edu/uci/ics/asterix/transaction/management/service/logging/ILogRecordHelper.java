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
 * Helper class for writing/reading of log header and checksum as well as
 * validating log record by checksum comparison. Every ILogManager
 * implementation has an associated ILogRecordHelper implementation.
 */

public interface ILogRecordHelper {

    byte getLogType(LogicalLogLocator logicalLogLocator);

    int getJobId(LogicalLogLocator logicalLogLocator);

    int getDatasetId(LogicalLogLocator logicalLogLocator);

    int getPKHashValue(LogicalLogLocator logicalLogLocator);

    PhysicalLogLocator getPrevLSN(LogicalLogLocator logicalLogLocator);

    boolean getPrevLSN(PhysicalLogLocator physicalLogLocator, LogicalLogLocator logicalLogLocator);
    
    long getResourceId(LogicalLogLocator logicalLogLocator);
    
    byte getResourceMgrId(LogicalLogLocator logicalLogLocater);

    int getLogContentSize(LogicalLogLocator logicalLogLocater);

    long getLogChecksum(LogicalLogLocator logicalLogLocator);

    int getLogContentBeginPos(LogicalLogLocator logicalLogLocator);

    int getLogContentEndPos(LogicalLogLocator logicalLogLocator);

    String getLogRecordForDisplay(LogicalLogLocator logicalLogLocator);

    void writeLogHeader(LogicalLogLocator logicalLogLocator, byte logType, TransactionContext context, int datasetId,
            int PKHashValue, long prevLogicalLogLocator, long resourceId, byte resourceMgrId, int logRecordSize);

    boolean validateLogRecord(LogicalLogLocator logicalLogLocator);

    int getLogRecordSize(byte logType, int logBodySize);

    int getLogHeaderSize(byte logType);

    int getLogChecksumSize();

}
