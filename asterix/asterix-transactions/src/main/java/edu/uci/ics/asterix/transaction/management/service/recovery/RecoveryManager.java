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
package edu.uci.ics.asterix.transaction.management.service.recovery;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.resource.TransactionalResourceRepository;
import edu.uci.ics.asterix.transaction.management.service.logging.FileUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.IBuffer;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogCursor;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogFilter;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogManager;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogRecordHelper;
import edu.uci.ics.asterix.transaction.management.service.logging.LogActionType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogRecordHelper;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.LogicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.logging.PhysicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;

/**
 * This is the Recovery Manager and is responsible for rolling back a
 * transaction as well as doing a system recovery. TODO: Crash Recovery logic is
 * not in place completely. Once we have physical logging implemented, we would
 * add support for crash recovery.
 */
public class RecoveryManager implements IRecoveryManager {

    private static final Logger LOGGER = Logger.getLogger(RecoveryManager.class.getName());
    private TransactionProvider transactionProvider;

    /**
     * A file at a known location that contains the LSN of the last log record
     * traversed doing a successful checkpoint.
     */
    private static final String checkpoint_record_file = "last_checkpoint_lsn";
    private SystemState state;
    private Map<Long, TransactionTableEntry> transactionTable;
    private Map<Long, List<PhysicalLogLocator>> dirtyPagesTable;

    public RecoveryManager(TransactionProvider TransactionProvider) throws ACIDException {
        this.transactionProvider = TransactionProvider;
        try {
            FileUtil.createFileIfNotExists(checkpoint_record_file);
        } catch (IOException ioe) {
            throw new ACIDException(" unable to create checkpoint record file " + checkpoint_record_file, ioe);
        }
    }

    public SystemState getSystemState() throws ACIDException {
        return state;
    }

    private PhysicalLogLocator getBeginRecoveryLSN() throws ACIDException {
        return new PhysicalLogLocator(0, transactionProvider.getLogManager());
    }

    /**
     * TODO:This method is currently not implemented completely.
     */
    public SystemState startRecovery(boolean synchronous) throws IOException, ACIDException {
        ILogManager logManager = transactionProvider.getLogManager();
        state = SystemState.RECOVERING;
        transactionTable = new HashMap<Long, TransactionTableEntry>();
        dirtyPagesTable = new HashMap<Long, List<PhysicalLogLocator>>();

        PhysicalLogLocator beginLSN = getBeginRecoveryLSN();
        ILogCursor cursor = logManager.readLog(beginLSN, new ILogFilter() {
            public boolean accept(IBuffer logs, long startOffset, int endOffset) {
                return true;
            }
        });
        LogicalLogLocator memLSN = new LogicalLogLocator(beginLSN.getLsn(), null, -1, logManager);
        boolean logValidity = true;
        LogRecordHelper parser = new LogRecordHelper(logManager);
        try {
            while (logValidity) {
                logValidity = cursor.next(memLSN);
                if (!logValidity) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("reached end of log !");
                    }
                    break;
                }
                byte resourceMgrId = parser.getResourceMgrId(memLSN);
                IResourceManager resourceMgr = TransactionalResourceRepository
                        .getTransactionalResourceMgr(resourceMgrId);
                if (resourceMgr == null) {
                    throw new ACIDException("unknown resource mgr with id " + resourceMgrId);
                } else {
                    byte actionType = parser.getLogActionType(memLSN);
                    switch (actionType) {
                        case LogActionType.REDO:
                            resourceMgr.redo(parser, memLSN);
                            break;
                        case LogActionType.UNDO: /* skip these records */
                            break;
                        default: /* do nothing */
                    }
                }
                writeCheckpointRecord(memLSN.getLsn());
            }
            state = SystemState.HEALTHY;
        } catch (Exception e) {
            state = SystemState.CORRUPTED;
            throw new ACIDException(" could not recover , corrputed log !", e);
        }
        return state;
    }

    private void writeCheckpointRecord(long lsn) throws ACIDException {
        try {
            FileWriter writer = new FileWriter(new File(checkpoint_record_file));
            BufferedWriter buffWriter = new BufferedWriter(writer);
            buffWriter.write("" + lsn);
            buffWriter.flush();
        } catch (IOException ioe) {
            throw new ACIDException(" unable to create check point record", ioe);
        }
    }

    /*
     * Currently this method is not used, but will be used as part of crash
     * recovery logic.
     */
    private long getLastCheckpointRecordLSN() throws Exception {
        FileReader reader;
        BufferedReader buffReader;
        String content = null;
        reader = new FileReader(new File(checkpoint_record_file));
        buffReader = new BufferedReader(reader);
        content = buffReader.readLine();
        if (content != null) {
            return Long.parseLong(content);
        }
        return -1;
    }

    /**
     * Rollback a transaction (non-Javadoc)
     * 
     * @see edu.uci.ics.transaction.management.service.recovery.IRecoveryManager# rollbackTransaction (edu.uci.ics.transaction.management.service.transaction .TransactionContext)
     */
    @Override
    public void rollbackTransaction(TransactionContext txnContext) throws ACIDException {
        ILogManager logManager = transactionProvider.getLogManager();
        ILogRecordHelper parser = logManager.getLogRecordHelper();

        // Obtain the last log record written by the transaction
        PhysicalLogLocator lsn = txnContext.getLastLogLocator();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" rollbacking transaction log records at lsn " + lsn.getLsn());
        }

        // check if the transaction actually wrote some logs.
        if (lsn.getLsn() == TransactionManagementConstants.LogManagerConstants.TERMINAL_LSN) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(" no need to roll back as there were no operations by the transaction "
                        + txnContext.getTransactionID());
            }
            return;
        }

        // a dummy logLocator instance that is re-used during rollback
        LogicalLogLocator logLocator = LogUtil.getDummyLogicalLogLocator(logManager);

        while (true) {
            try {
                // read the log record at the given position
                logLocator = logManager.readLog(lsn);
            } catch (Exception e) {
                e.printStackTrace();
                state = SystemState.CORRUPTED;
                throw new ACIDException(" could not read log at lsn :" + lsn, e);
            }

            byte logType = parser.getLogType(logLocator);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine(" reading LSN value inside rollback transaction method " + txnContext.getLastLogLocator()
                        + " txn id " + parser.getLogTransactionId(logLocator) + " log type  " + logType);
            }

            switch (logType) {
                case LogType.UPDATE:

                    // extract the resource manager id from the log record.
                    byte resourceMgrId = parser.getResourceMgrId(logLocator);
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine(parser.getLogRecordForDisplay(logLocator));
                    }

                    // look up the repository to get the resource manager
                    IResourceManager resourceMgr = TransactionalResourceRepository
                            .getTransactionalResourceMgr(resourceMgrId);
                    if (resourceMgr == null) {
                        throw new ACIDException(txnContext, " unknown resource manager " + resourceMgrId);
                    } else {
                        byte actionType = parser.getLogActionType(logLocator);
                        switch (actionType) {
                            case LogActionType.REDO: // no need to do anything
                                break;
                            case LogActionType.UNDO: // undo the log record
                                resourceMgr.undo(parser, logLocator);
                                break;
                            case LogActionType.REDO_UNDO: // undo log record
                                resourceMgr.undo(parser, logLocator);
                                break;
                            default:
                        }
                    }
                case LogType.CLR: // skip the CLRs as they are not undone
                    break;
                case LogType.COMMIT:
                    throw new ACIDException(txnContext, " cannot rollback commmitted transaction");

            }

            // follow the previous LSN pointer to get the previous log record
            // written by the transaction
            // If the return value is true, the logLocator, it indicates that
            // the logLocator object has been
            // appropriately set to the location of the next log record to be
            // processed as part of the roll back
            boolean moreLogs = parser.getPreviousLsnByTransaction(lsn, logLocator);
            if (!moreLogs) {
                // no more logs to process
                break;
            }
        }

    }

}
