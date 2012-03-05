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

    public byte getLogType(LogicalLogLocator logicalLogLocator);

    public int getLogLength(LogicalLogLocator logicalLogLocator);

    public long getLogTimestamp(LogicalLogLocator logicalLogLocator);

    public long getLogChecksum(LogicalLogLocator logicalLogLocator);

    public long getLogTransactionId(LogicalLogLocator logicalLogLocator);

    public byte getResourceMgrId(LogicalLogLocator logicalLogLocator);

    public long getPageId(LogicalLogLocator logicalLogLocator);

    public int getLogContentBeginPos(LogicalLogLocator logicalLogLocator);

    public int getLogContentEndPos(LogicalLogLocator logicalLogLocator);

    public String getLogRecordForDisplay(LogicalLogLocator logicalLogLocator);

    public byte getLogActionType(LogicalLogLocator logicalLogLocator);

    public PhysicalLogLocator getPreviousLsnByTransaction(LogicalLogLocator logicalLogLocator);

    public boolean getPreviousLsnByTransaction(PhysicalLogLocator physicalLogLocator,
            LogicalLogLocator logicalLogLocator);

    public void writeLogHeader(TransactionContext context, LogicalLogLocator logicalLogLocator, byte resourceMgrId,
            long pageId, byte logType, byte logActionType, int logContentSize, long prevLsnValue);

    public void writeLogTail(LogicalLogLocator logicalLogLocator, ILogManager logManager);

    public boolean validateLogRecord(LogManagerProperties logManagerProperties, LogicalLogLocator logicalLogLocator);

}
