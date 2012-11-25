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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.logging.FileUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.IBuffer;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogCursor;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogFilter;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogManager;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogRecordHelper;
import edu.uci.ics.asterix.transaction.management.service.logging.IndexResourceManager;
import edu.uci.ics.asterix.transaction.management.service.logging.LogRecordHelper;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.LogicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.logging.PhysicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.JobId;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;

/**
 * This is the Recovery Manager and is responsible for rolling back a
 * transaction as well as doing a system recovery. TODO: Crash Recovery logic is
 * not in place completely. Once we have physical logging implemented, we would
 * add support for crash recovery.
 */
public class RecoveryManager implements IRecoveryManager {

    private static final Logger LOGGER = Logger.getLogger(RecoveryManager.class.getName());
    private TransactionSubsystem txnSubsystem;

    /**
     * A file at a known location that contains the LSN of the last log record
     * traversed doing a successful checkpoint.
     */
    private static final String checkpoint_record_file = "last_checkpoint_lsn";
    private SystemState state;
    private Map<Long, TransactionTableEntry> transactionTable;
    private Map<Long, List<PhysicalLogLocator>> dirtyPagesTable;

    public RecoveryManager(TransactionSubsystem TransactionProvider) throws ACIDException {
        this.txnSubsystem = TransactionProvider;
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
        return new PhysicalLogLocator(0, txnSubsystem.getLogManager());
    }

    /**
     * TODO:This method is currently not implemented completely.
     */
    public SystemState startRecovery(boolean synchronous) throws IOException, ACIDException {
        ILogManager logManager = txnSubsystem.getLogManager();
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
                IResourceManager resourceMgr = txnSubsystem.getTransactionalResourceRepository()
                        .getTransactionalResourceMgr(resourceMgrId);
                //register resourceMgr if it is not registered.
                if (resourceMgr == null) {
                    resourceMgr = new IndexResourceManager(resourceMgrId, txnSubsystem);
                    txnSubsystem.getTransactionalResourceRepository().registerTransactionalResourceManager(
                            resourceMgrId, resourceMgr);
                }
                resourceMgr.redo(parser, memLSN);

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
     * Rollback a transaction
     * 
     * @see edu.uci.ics.transaction.management.service.recovery.IRecoveryManager# rollbackTransaction (edu.uci.ics.TransactionContext.management.service.transaction .TransactionContext)
     */
    @Override
    public void rollbackTransaction(TransactionContext txnContext) throws ACIDException {
        ILogManager logManager = txnSubsystem.getLogManager();
        ILogRecordHelper logRecordHelper = logManager.getLogRecordHelper();
        Map<TxnId, List<Long>> loserTxnTable = new HashMap<TxnId, List<Long>>();
        TxnId tempKeyTxnId = new TxnId(-1, -1, -1);

        // Obtain the first log record written by the Job
        PhysicalLogLocator firstLSNLogLocator = txnContext.getFirstLogLocator();
        PhysicalLogLocator lastLSNLogLocator = txnContext.getLastLogLocator();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" rollbacking transaction log records from " + firstLSNLogLocator.getLsn() + "to"
                    + lastLSNLogLocator.getLsn());
        }

        // check if the transaction actually wrote some logs.
        if (firstLSNLogLocator.getLsn() == TransactionManagementConstants.LogManagerConstants.TERMINAL_LSN) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(" no need to roll back as there were no operations by the transaction "
                        + txnContext.getJobId());
            }
            return;
        }

        // While reading log records from firstLSN to lastLSN, collect uncommitted txn's LSNs 
        ILogCursor logCursor;
        try {
            logCursor = logManager.readLog(firstLSNLogLocator, new ILogFilter() {
                @Override
                public boolean accept(IBuffer buffer, long startOffset, int length) {
                    return true;
                }
            });
        } catch (IOException e) {
            throw new ACIDException("Failed to create LogCursor with LSN:" + firstLSNLogLocator.getLsn(), e);
        }

        LogicalLogLocator currentLogLocator = LogUtil.getDummyLogicalLogLocator(logManager);
        boolean valid;
        byte logType;
        List<Long> undoLSNSet = null;

        while (currentLogLocator.getLsn() != lastLSNLogLocator.getLsn()) {
            try {
                valid = logCursor.next(currentLogLocator);
            } catch (IOException e) {
                throw new ACIDException("Failed to read log at LSN:" + currentLogLocator.getLsn(), e);
            }
            if (!valid) {
                if (currentLogLocator.getLsn() != lastLSNLogLocator.getLsn()) {
                    throw new ACIDException("Log File Corruption: lastLSN mismatch");
                } else {
                    break;//End of Log File
                }
            }

            tempKeyTxnId.setTxnId(logRecordHelper.getJobId(currentLogLocator), logRecordHelper.getDatasetId(currentLogLocator),
                    logRecordHelper.getPKHashValue(currentLogLocator));
            logType = logRecordHelper.getLogType(currentLogLocator);

            switch (logType) {
                case LogType.UPDATE:
                    undoLSNSet = loserTxnTable.get(tempKeyTxnId);
                    if (undoLSNSet == null) {
                        TxnId txnId = new TxnId(logRecordHelper.getJobId(currentLogLocator),
                                logRecordHelper.getDatasetId(currentLogLocator), logRecordHelper.getPKHashValue(currentLogLocator));
                        undoLSNSet = new ArrayList<Long>();
                        loserTxnTable.put(txnId, undoLSNSet);
                    }
                    undoLSNSet.add(currentLogLocator.getLsn());
                    break;

                case LogType.COMMIT:
                    undoLSNSet = loserTxnTable.get(tempKeyTxnId);
                    if (undoLSNSet != null) {
                        loserTxnTable.remove(tempKeyTxnId);
                    }
                    break;

                default:
                    throw new ACIDException("Unsupported LogType: " + logType);
            }
        }

        //undo loserTxn's effect
        TxnId txnId;
        Iterator<Entry<TxnId, List<Long>>> iter = loserTxnTable.entrySet().iterator();
        byte resourceMgrId;
        while (iter.hasNext()) {
            Map.Entry<TxnId, List<Long>> loserTxn = (Map.Entry<TxnId, List<Long>>) iter.next();
            txnId = loserTxn.getKey();

            undoLSNSet = loserTxn.getValue();
            Comparator<Long> comparator = Collections.reverseOrder();
            Collections.sort(undoLSNSet, comparator);

            for (long undoLSN : undoLSNSet) {
                currentLogLocator.setLsn(undoLSN);
                // here, all the log records are UPDATE type. So, we don't need to check the type again.

                // extract the resource manager id from the log record.
                resourceMgrId = logRecordHelper.getResourceMgrId(currentLogLocator);
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine(logRecordHelper.getLogRecordForDisplay(currentLogLocator));
                }

                // look up the repository to get the resource manager
                IResourceManager resourceMgr = txnSubsystem.getTransactionalResourceRepository()
                        .getTransactionalResourceMgr(resourceMgrId);

                // register resourceMgr if it is not registered. 
                if (resourceMgr == null) {
                    resourceMgr = new IndexResourceManager(resourceMgrId, txnSubsystem);
                    txnSubsystem.getTransactionalResourceRepository().registerTransactionalResourceManager(
                            resourceMgrId, resourceMgr);
                }
                resourceMgr.undo(logRecordHelper, currentLogLocator);
            }
        }
    }
}

class TxnId {
    public int jobId;
    public int datasetId;
    public int pkHashVal;

    public TxnId(int jobId, int datasetId, int pkHashVal) {
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.pkHashVal = pkHashVal;
    }

    public void setTxnId(int jobId, int datasetId, int pkHashVal) {
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.pkHashVal = pkHashVal;
    }

    public void setTxnId(TxnId txnId) {
        this.jobId = txnId.jobId;
        this.datasetId = txnId.datasetId;
        this.pkHashVal = txnId.pkHashVal;
    }

    @Override
    public int hashCode() {
        return pkHashVal;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof JobId)) {
            return false;
        }
        TxnId txnId = (TxnId) o;

        return (txnId.pkHashVal == pkHashVal && txnId.datasetId == datasetId && txnId.jobId == jobId);
    }
}
