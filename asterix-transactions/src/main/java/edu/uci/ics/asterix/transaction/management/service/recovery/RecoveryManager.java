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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import edu.uci.ics.asterix.transaction.management.resource.ILocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.LSMInvertedIndexLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.LSMRTreeLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.TransactionalResourceManagerRepository;
import edu.uci.ics.asterix.transaction.management.service.logging.IBuffer;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogCursor;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogFilter;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogManager;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogRecordHelper;
import edu.uci.ics.asterix.transaction.management.service.logging.IndexResourceManager;
import edu.uci.ics.asterix.transaction.management.service.logging.LogManager;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.LogicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.logging.PhysicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager.ResourceType;
import edu.uci.ics.asterix.transaction.management.service.transaction.ITransactionManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexLifecycleManager;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;

/**
 * This is the Recovery Manager and is responsible for rolling back a
 * transaction as well as doing a system recovery. TODO: Crash Recovery logic is
 * not in place completely. Once we have physical logging implemented, we would
 * add support for crash recovery.
 */
public class RecoveryManager implements IRecoveryManager {

    public static final boolean IS_DEBUG_MODE = false;//true
    private static final Logger LOGGER = Logger.getLogger(RecoveryManager.class.getName());
    private TransactionSubsystem txnSubsystem;

    /**
     * A file at a known location that contains the LSN of the last log record
     * traversed doing a successful checkpoint.
     */
    private static final String CHECKPOINT_FILENAME_PREFIX = "checkpoint_";
    private SystemState state;

    public RecoveryManager(TransactionSubsystem TransactionProvider) throws ACIDException {
        this.txnSubsystem = TransactionProvider;
    }

    /**
     * returns system state which could be one of the three states: HEALTHY, RECOVERING, CORRUPTED.
     * This state information could be used in a case where more than one thread is running
     * in the bootstrap process to provide higher availability. In other words, while the system
     * is recovered, another thread may start a new transaction with understanding the side effect
     * of the operation, or the system can be recovered concurrently. This kind of concurrency is
     * not supported, yet.
     */
    public SystemState getSystemState() throws ACIDException {

        //#. read checkpoint file
        CheckpointObject checkpointObject = null;
        try {
            checkpointObject = readCheckpoint();
        } catch (FileNotFoundException e) {
            //This is initial bootstrap. 
            //Otherwise, the checkpoint file is deleted unfortunately. What we can do in this case?
            state = SystemState.CORRUPTED;
            new ACIDException("Checkpoint file doesn't exist", e);
        }

        //#. if minMCTFirstLSN is equal to -1,
        //   then return healthy state. Otherwise, return corrupted.
        if (checkpointObject.getMinMCTFirstLSN() == -1) {
            state = SystemState.HEALTHY;
            return state;
        } else {
            state = SystemState.CORRUPTED;
            return state;
        }
    }

    private PhysicalLogLocator getBeginRecoveryLSN() throws ACIDException {
        return new PhysicalLogLocator(0, txnSubsystem.getLogManager());
    }

    public void startRecovery(boolean synchronous) throws IOException, ACIDException {

        state = SystemState.RECOVERING;

        ILogManager logManager = txnSubsystem.getLogManager();
        ILogRecordHelper logRecordHelper = logManager.getLogRecordHelper();
        ITransactionManager txnManager = txnSubsystem.getTransactionManager();
        TransactionalResourceManagerRepository txnResourceRepository = txnSubsystem.getTransactionalResourceRepository();

        //winnerTxnTable is used to add pairs, <committed TxnId, the most recent commit LSN of the TxnId>
        Map<TxnId, Long> winnerTxnTable = new HashMap<TxnId, Long>();
        TxnId tempKeyTxnId = new TxnId(-1, -1, -1);
        byte logType;

        //#. read checkpoint file and set lowWaterMark where anaylsis and redo start
        CheckpointObject checkpointObject = readCheckpoint();
        long lowWaterMarkLSN = checkpointObject.getMinMCTFirstLSN();
        int maxJobId = checkpointObject.getMaxJobId();
        int currentJobId;

        //-------------------------------------------------------------------------
        //  [ analysis phase ]
        //  - collect all committed LSN 
        //  - if there are duplicate commits for the same TxnId, 
        //    keep only the mostRecentCommitLSN among the duplicates.
        //-------------------------------------------------------------------------

        //#. set log cursor to the lowWaterMarkLSN
        ILogCursor logCursor = logManager.readLog(new PhysicalLogLocator(lowWaterMarkLSN, logManager),
                new ILogFilter() {
                    public boolean accept(IBuffer logs, long startOffset, int endOffset) {
                        return true;
                    }
                });
        LogicalLogLocator currentLogLocator = LogUtil.getDummyLogicalLogLocator(logManager);

        //#. collect all committed txn's pairs,<TxnId, LSN>
        while (logCursor.next(currentLogLocator)) {

            if (LogManager.IS_DEBUG_MODE) {
                System.out.println(logManager.getLogRecordHelper().getLogRecordForDisplay(currentLogLocator));
            }

            logType = logRecordHelper.getLogType(currentLogLocator);

            //update max jobId
            currentJobId = logRecordHelper.getJobId(currentLogLocator);
            if (currentJobId > maxJobId) {
                maxJobId = currentJobId;
            }

            switch (logType) {
                case LogType.UPDATE:
                    //do nothing                    
                    break;

                case LogType.COMMIT:
                    tempKeyTxnId.setTxnId(logRecordHelper.getJobId(currentLogLocator),
                            logRecordHelper.getDatasetId(currentLogLocator),
                            logRecordHelper.getPKHashValue(currentLogLocator));
                    winnerTxnTable.put(tempKeyTxnId, currentLogLocator.getLsn());
                    break;

                default:
                    throw new ACIDException("Unsupported LogType: " + logType);
            }
        }

        //-------------------------------------------------------------------------
        //  [ redo phase ]
        //  - redo if
        //    1) The TxnId is committed --> gurantee durability
        //      &&  
        //    2) the currentLSN > maxDiskLastLSN of the index --> guarantee idempotance
        //-------------------------------------------------------------------------

        //#. set log cursor to the lowWaterMarkLSN again.
        logCursor = logManager.readLog(new PhysicalLogLocator(lowWaterMarkLSN, logManager), new ILogFilter() {
            public boolean accept(IBuffer logs, long startOffset, int endOffset) {
                return true;
            }
        });
        currentLogLocator = LogUtil.getDummyLogicalLogLocator(logManager);

        long resourceId;
        byte resourceMgrId;
        long maxDiskLastLSN;
        long currentLSN;
        IIndex index = null;
        LocalResource localResource = null;
        ILocalResourceMetadata localResourceMetadata = null;

        //#. get indexLifeCycleManager 
        IAsterixAppRuntimeContextProvider appRuntimeContext = txnSubsystem.getAsterixAppRuntimeContextProvider();
        IIndexLifecycleManager indexLifecycleManager = appRuntimeContext.getIndexLifecycleManager();
        ILocalResourceRepository localResourceRepository = appRuntimeContext.getLocalResourceRepository();

        //#. redo
        while (logCursor.next(currentLogLocator)) {

            if (LogManager.IS_DEBUG_MODE) {
                System.out.println(logManager.getLogRecordHelper().getLogRecordForDisplay(currentLogLocator));
            }

            logType = logRecordHelper.getLogType(currentLogLocator);

            switch (logType) {
                case LogType.UPDATE:
                    tempKeyTxnId.setTxnId(logRecordHelper.getJobId(currentLogLocator),
                            logRecordHelper.getDatasetId(currentLogLocator),
                            logRecordHelper.getPKHashValue(currentLogLocator));

                    if (winnerTxnTable.containsKey(tempKeyTxnId)) {
                        currentLSN = winnerTxnTable.get(tempKeyTxnId);
                        resourceId = logRecordHelper.getResourceId(currentLogLocator);

                        //get index instance from IndexLifeCycleManager
                        //if index is not registered into IndexLifeCycleManager,
                        //create the index using LocalMetadata stored in LocalResourceRepository
                        index = indexLifecycleManager.getIndex(resourceId);
                        if (index == null) {
                            localResource = localResourceRepository.getResourceById(resourceId);
                            localResourceMetadata = (ILocalResourceMetadata) localResource.getResourceObject();
                            index = localResourceMetadata.createIndexInstance(appRuntimeContext,
                                    localResource.getResourceName(), localResource.getPartition());
                            indexLifecycleManager.register(resourceId, index);
                        }

                        /***************************************************/
                        //TODO
                        //get the right maxDiskLastLSN with the resourceId from the Zach. :) 
                        maxDiskLastLSN = 100;
                        /***************************************************/

                        if (currentLSN > maxDiskLastLSN) {
                            resourceMgrId = logRecordHelper.getResourceMgrId(currentLogLocator);

                            // look up the repository to get the resource manager
                            // register resourceMgr if it is not registered. 
                            IResourceManager resourceMgr = txnSubsystem.getTransactionalResourceRepository()
                                    .getTransactionalResourceMgr(resourceMgrId);
                            if (resourceMgr == null) {
                                resourceMgr = new IndexResourceManager(resourceMgrId, txnSubsystem);
                                txnSubsystem.getTransactionalResourceRepository().registerTransactionalResourceManager(
                                        resourceMgrId, resourceMgr);
                            }

                            //redo finally.
                            resourceMgr.redo(logRecordHelper, currentLogLocator);
                        }
                    }
                    break;

                case LogType.COMMIT:
                    //do nothing
                    break;

                default:
                    throw new ACIDException("Unsupported LogType: " + logType);
            }
        }
    }

    @Override
    public void checkpoint() throws ACIDException {

        LogManager logMgr = (LogManager) txnSubsystem.getLogManager();
        TransactionManager txnMgr = (TransactionManager) txnSubsystem.getTransactionManager();
        String logDir = logMgr.getLogManagerProperties().getLogDir();

        //#. get the filename of the previous checkpoint files which are about to be deleted 
        //   right after the new checkpoint file is written.
        File[] prevCheckpointFiles = getPreviousCheckpointFiles();

        //#. create and store the checkpointObject into the new checkpoint file
        //TODO
        //put the correct minMCTFirstLSN by getting from Zach. :)
        long minMCTFirstLSM = -1;
        CheckpointObject checkpointObject = new CheckpointObject(minMCTFirstLSM, txnMgr.getMaxJobId(),
                System.currentTimeMillis());

        FileOutputStream fos = null;
        ObjectOutputStream oosToFos = null;
        try {
            String fileName = getFileName(logDir, Long.toString(checkpointObject.getTimeStamp()));
            fos = new FileOutputStream(fileName);
            oosToFos = new ObjectOutputStream(fos);
            oosToFos.writeObject(checkpointObject);
            oosToFos.flush();
        } catch (IOException e) {
            throw new ACIDException("Failed to checkpoint", e);
        } finally {
            if (oosToFos != null) {
                try {
                    oosToFos.close();
                } catch (IOException e) {
                    throw new ACIDException("Failed to checkpoint", e);
                }
            }
            if (oosToFos == null && fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    throw new ACIDException("Failed to checkpoint", e);
                }
            }
        }

        //#. delete the previous checkpoint files
        if (prevCheckpointFiles != null) {
            for (File file : prevCheckpointFiles) {
                file.delete();
            }
        }
    }

    private CheckpointObject readCheckpoint() throws ACIDException, FileNotFoundException {

        CheckpointObject checkpointObject = null;

        //#. read all checkpointObjects from the existing checkpoint files
        File[] prevCheckpointFiles = getPreviousCheckpointFiles();

        if (prevCheckpointFiles == null || prevCheckpointFiles.length == 0) {
            throw new FileNotFoundException("Checkpoint file is not found");
        }

        List<CheckpointObject> checkpointObjectList = new ArrayList<CheckpointObject>();

        for (File file : prevCheckpointFiles) {
            FileInputStream fis = null;
            ObjectInputStream oisFromFis = null;

            try {
                fis = new FileInputStream(file);
                oisFromFis = new ObjectInputStream(fis);
                checkpointObject = (CheckpointObject) oisFromFis.readObject();
                checkpointObjectList.add(checkpointObject);
            } catch (Exception e) {
                throw new ACIDException("Failed to read a checkpoint file", e);
            } finally {
                if (oisFromFis != null) {
                    try {
                        oisFromFis.close();
                    } catch (IOException e) {
                        throw new ACIDException("Failed to read a checkpoint file", e);
                    }
                }
                if (oisFromFis == null && fis != null) {
                    try {
                        fis.close();
                    } catch (IOException e) {
                        throw new ACIDException("Failed to read a checkpoint file", e);
                    }
                }
            }
        }

        //#. sort checkpointObjects in descending order by timeStamp to find out the most recent one.
        Collections.sort(checkpointObjectList);

        //#. return the most recent one (the first one in sorted list)
        return checkpointObjectList.get(0);
    }

    private File[] getPreviousCheckpointFiles() {
        String logDir = txnSubsystem.getLogManager().getLogManagerProperties().getLogDir();

        File parentDir = new File(logDir);

        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name.contains(CHECKPOINT_FILENAME_PREFIX)) {
                    return true;
                } else {
                    return false;
                }
            }
        };

        File[] prevCheckpointFiles = parentDir.listFiles(filter);

        return prevCheckpointFiles;
    }

    private String getFileName(String baseDir, String suffix) {

        if (!baseDir.endsWith(System.getProperty("file.separator"))) {
            baseDir += System.getProperty("file.separator");
        }

        return baseDir + CHECKPOINT_FILENAME_PREFIX + suffix;
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

        int updateLogCount = 0;
        int commitLogCount = 0;

        // Obtain the first log record written by the Job
        PhysicalLogLocator firstLSNLogLocator = txnContext.getFirstLogLocator();
        PhysicalLogLocator lastLSNLogLocator = txnContext.getLastLogLocator();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" rollbacking transaction log records from " + firstLSNLogLocator.getLsn() + " to "
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

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" collecting loser transaction's LSNs from " + firstLSNLogLocator.getLsn() + " to "
                    + +lastLSNLogLocator.getLsn());
        }

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

            if (LogManager.IS_DEBUG_MODE) {
                System.out.println(logManager.getLogRecordHelper().getLogRecordForDisplay(currentLogLocator));
            }

            tempKeyTxnId.setTxnId(logRecordHelper.getJobId(currentLogLocator),
                    logRecordHelper.getDatasetId(currentLogLocator), logRecordHelper.getPKHashValue(currentLogLocator));
            logType = logRecordHelper.getLogType(currentLogLocator);

            switch (logType) {
                case LogType.UPDATE:
                    undoLSNSet = loserTxnTable.get(tempKeyTxnId);
                    if (undoLSNSet == null) {
                        TxnId txnId = new TxnId(logRecordHelper.getJobId(currentLogLocator),
                                logRecordHelper.getDatasetId(currentLogLocator),
                                logRecordHelper.getPKHashValue(currentLogLocator));
                        undoLSNSet = new ArrayList<Long>();
                        loserTxnTable.put(txnId, undoLSNSet);
                    }
                    undoLSNSet.add(currentLogLocator.getLsn());
                    if (IS_DEBUG_MODE) {
                        updateLogCount++;
                        System.out.println("" + Thread.currentThread().getId() + "======> update["
                                + currentLogLocator.getLsn() + "]:" + tempKeyTxnId);
                    }
                    break;

                case LogType.COMMIT:
                    undoLSNSet = loserTxnTable.get(tempKeyTxnId);
                    if (undoLSNSet != null) {
                        loserTxnTable.remove(tempKeyTxnId);
                    }
                    if (IS_DEBUG_MODE) {
                        commitLogCount++;
                        System.out.println("" + Thread.currentThread().getId() + "======> commit["
                                + currentLogLocator.getLsn() + "]" + tempKeyTxnId);
                    }
                    break;

                default:
                    throw new ACIDException("Unsupported LogType: " + logType);
            }
        }

        //undo loserTxn's effect
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" undoing loser transaction's effect");
        }

        TxnId txnId = null;
        Iterator<Entry<TxnId, List<Long>>> iter = loserTxnTable.entrySet().iterator();
        byte resourceMgrId;
        int undoCount = 0;
        while (iter.hasNext()) {

            Map.Entry<TxnId, List<Long>> loserTxn = (Map.Entry<TxnId, List<Long>>) iter.next();
            txnId = loserTxn.getKey();

            undoLSNSet = loserTxn.getValue();
            Comparator<Long> comparator = Collections.reverseOrder();
            Collections.sort(undoLSNSet, comparator);

            for (long undoLSN : undoLSNSet) {
                // here, all the log records are UPDATE type. So, we don't need to check the type again.

                //read the corresponding log record to be undone.
                logManager.readLog(undoLSN, currentLogLocator);

                if (LogManager.IS_DEBUG_MODE) {
                    System.out.println(logManager.getLogRecordHelper().getLogRecordForDisplay(currentLogLocator));
                }

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

                if (IS_DEBUG_MODE) {
                    undoCount++;
                }
            }
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" undone loser transaction's effect");
        }
        if (IS_DEBUG_MODE) {
            System.out.println("UpdateLogCount/CommitLogCount/UndoCount:" + updateLogCount + "/" + commitLogCount + "/"
                    + undoCount);
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
    public String toString() {
        return "[" + jobId + "," + datasetId + "," + pkHashVal + "]";
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
        if (!(o instanceof TxnId)) {
            return false;
        }
        TxnId txnId = (TxnId) o;

        return (txnId.pkHashVal == pkHashVal && txnId.datasetId == datasetId && txnId.jobId == jobId);
    }
}
