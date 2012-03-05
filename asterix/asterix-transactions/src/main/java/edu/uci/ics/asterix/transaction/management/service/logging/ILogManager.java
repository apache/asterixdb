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

import java.io.IOException;
import java.util.Map;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;

public interface ILogManager {

    /**
     * An API to write a log record.
     * 
     * @param logicalLogLocator
     *            A reusable object passed in by the caller. When the call
     *            returns, this object has the physical location of the log
     *            record that was written.
     * @param context
     *            the transaction context associated with the transaction that
     *            is writing the log record
     * @param resourceMgrId
     *            the unique identifier of the resource manager that would be
     *            handling (interpreting) the log record if there is a need to
     *            process and apply the log record during a redo/undo task.
     * @param pageId
     *            the unique identifier of the page where the operation
     *            corresponding to the log record is applied
     * @param logType
     *            the type of log record (@see LogType)
     * @param logActionType
     *            the action that needs to be taken when processing the log
     *            record (@see LogActionType)
     * @param length
     *            the length of the content inside the log record. This does not
     *            include the header or the checksum size.
     * @param logger
     *            an implementation of the @see ILogger interface that is
     *            invoked by the ILogManager instance to get the actual content
     *            for the log record.
     * @param loggerArguments
     *            Represent any additional arguments that needs to be passed
     *            back in the call the to ILogger interface APIs.
     * @throws ACIDException
     */
    public void log(LogicalLogLocator logicalLogLocator, TransactionContext context, byte resourceMgrId, long pageId,
            byte logType, byte logActionType, int length, ILogger logger, Map<Object, Object> loggerArguments)
            throws ACIDException;

    /**
     * @param physicalLogLocator
     *            specifies the physical location from where the logs need to be
     *            read
     * @param logFilter
     *            specifies the filtering criteria for the retrieved logs
     * @return LogCursor an iterator for the retrieved logs
     * @throws ACIDException
     */
    public ILogCursor readLog(PhysicalLogLocator physicalLogLocator, ILogFilter logFilter) throws IOException,
            ACIDException;

    /**
     * Provides a cursor for retrieving logs that satisfy a given ILogFilter
     * instance. Log records are retrieved in increasing order of lsn
     * 
     * @param logFilter
     *            specifies the filtering criteria for the retrieved logs
     * @return LogCursor an iterator for the retrieved logs
     * @throws ACIDException
     */
    public ILogCursor readLog(ILogFilter logFilter) throws ACIDException;

    /**
     * @param PhysicalLogLocator
     *            specifies the location of the log record to be read
     * @return LogicalLogLocator represents the in-memory location of the log
     *         record that has been fetched
     * @throws ACIDException
     */
    public LogicalLogLocator readLog(PhysicalLogLocator physicalLogLocator) throws ACIDException;

    /**
     * Flushes the log records up to the lsn represented by the
     * logicalLogLocator
     * 
     * @param logicalLogLocator
     * @throws ACIDException
     */
    public void flushLog(LogicalLogLocator logicalLogLocator) throws ACIDException;

    /**
     * Retrieves the configuration parameters of the ILogManager
     * 
     * @return LogManagerProperties: the configuration parameters for the
     *         ILogManager
     */
    public LogManagerProperties getLogManagerProperties();

    /**
     * Returns the ILogRecordHelper instance associated with this ILogManager
     * instance
     * 
     * @return ILogRecordHelper: the utility (class) for writing/reading log
     *         header.
     */
    public ILogRecordHelper getLogRecordHelper();

    /**
     * Returns the Transaction Provider associated with this ILogManager
     * instance
     * 
     * @return TransactionProvider
     */
    public TransactionProvider getTransactionProvider();

}
