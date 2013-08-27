/*
 * Copyright 2009-2013 by The Regents of the University of California
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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.ILocalResourceMetadata;
import edu.uci.ics.asterix.common.context.BaseOperationTracker;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import edu.uci.ics.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.asterix.common.transactions.ILogReader;
import edu.uci.ics.asterix.common.transactions.ILogRecord;
import edu.uci.ics.asterix.common.transactions.IRecoveryManager;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.transaction.management.service.logging.LogManager;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallbackWrapper;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;

/**
 * This is the Recovery Manager and is responsible for rolling back a
 * transaction as well as doing a system recovery. TODO: Crash Recovery logic is
 * not in place completely. Once we have physical logging implemented, we would
 * add support for crash recovery.
 */
public class RecoveryManager implements IRecoveryManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;//true
    private static final Logger LOGGER = Logger.getLogger(RecoveryManager.class.getName());
    private final TransactionSubsystem txnSubsystem;
    private final LogManager logMgr;
    private final int checkpointHistory;

    /**
     * A file at a known location that contains the LSN of the last log record
     * traversed doing a successful checkpoint.
     */
    private static final String CHECKPOINT_FILENAME_PREFIX = "checkpoint_";
    private SystemState state;

    public RecoveryManager(TransactionSubsystem txnSubsystem) throws ACIDException {
        this.txnSubsystem = txnSubsystem;
        this.logMgr = (LogManager) txnSubsystem.getLogManager();
        this.checkpointHistory = this.txnSubsystem.getTransactionProperties().getCheckpointHistory();
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
            state = SystemState.NEW_UNIVERSE;
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("The checkpoint file doesn't exist: systemState = NEW_UNIVERSE");
            }
            return state;
        }

        //#. if minMCTFirstLSN is equal to -1 && 
        //   checkpointLSN in the checkpoint file is equal to the lastLSN in the log file,
        //   then return healthy state. Otherwise, return corrupted.
        if ((checkpointObject.getMinMCTFirstLsn() == -2 && logMgr.getAppendLSN() == 0)
                || (checkpointObject.getMinMCTFirstLsn() == -1 && checkpointObject.getCheckpointLsn() == logMgr
                        .getAppendLSN())) {
            state = SystemState.HEALTHY;
            return state;
        } else {
            if (logMgr.getAppendLSN() == 0) {
                throw new IllegalStateException("Transaction log files are lost.");
            }
            state = SystemState.CORRUPTED;
            return state;
        }
    }

    public void startRecovery(boolean synchronous) throws IOException, ACIDException {

        int updateLogCount = 0;
        int commitLogCount = 0;
        int redoCount = 0;

        state = SystemState.RECOVERING;

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("[RecoveryMgr] starting recovery ...");
        }

        //winnerTxnTable is used to add pairs, <committed TxnId, the most recent commit Lsn of the TxnId>
        Map<TxnId, Long> winnerTxnTable = new HashMap<TxnId, Long>();
        TxnId tempKeyTxnId = new TxnId(-1, -1, -1);

        //#. read checkpoint file and set lowWaterMark where anaylsis and redo start
        CheckpointObject checkpointObject = readCheckpoint();
        long lowWaterMarkLsn = checkpointObject.getMinMCTFirstLsn();
        if (lowWaterMarkLsn == -1 || lowWaterMarkLsn == -2) {
            lowWaterMarkLsn = 0;
        }
        int maxJobId = checkpointObject.getMaxJobId();

        //-------------------------------------------------------------------------
        //  [ analysis phase ]
        //  - collect all committed Lsn 
        //  - if there are duplicate commits for the same TxnId, 
        //    keep only the mostRecentCommitLsn among the duplicates.
        //-------------------------------------------------------------------------
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("[RecoveryMgr] in analysis phase");
        }

        //#. set log reader to the lowWaterMarkLsn
        ILogReader logReader = logMgr.getLogReader(true);
        logReader.initializeScan(lowWaterMarkLsn);
        ILogRecord logRecord = logReader.next();
        while (logRecord != null) {
            if (IS_DEBUG_MODE) {
                System.out.println(logRecord.getLogRecordForDisplay());
            }
            //update max jobId
            if (logRecord.getJobId() > maxJobId) {
                maxJobId = logRecord.getJobId();
            }
            TxnId commitTxnId = null;
            switch (logRecord.getLogType()) {
                case LogType.UPDATE:
                    if (IS_DEBUG_MODE) {
                        updateLogCount++;
                    }
                    break;

                case LogType.JOB_COMMIT:
                case LogType.ENTITY_COMMIT:
                    commitTxnId = new TxnId(logRecord.getJobId(), logRecord.getDatasetId(), logRecord.getPKHashValue());
                    winnerTxnTable.put(commitTxnId, logRecord.getLSN());
                    if (IS_DEBUG_MODE) {
                        commitLogCount++;
                    }
                    break;

                default:
                    throw new ACIDException("Unsupported LogType: " + logRecord.getLogType());
            }
            logRecord = logReader.next();
        }

        //-------------------------------------------------------------------------
        //  [ redo phase ]
        //  - redo if
        //    1) The TxnId is committed && --> guarantee durability
        //    2) lsn < commitLog's Lsn && --> deal with a case of pkHashValue collision
        //    3) lsn > maxDiskLastLsn of the index --> guarantee idempotance
        //-------------------------------------------------------------------------
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("[RecoveryMgr] in redo phase");
        }
        //#. set log reader to the lowWaterMarkLsn again.
        logReader.initializeScan(lowWaterMarkLsn);

        long resourceId;
        long maxDiskLastLsn;
        long lsn = -1;
        long commitLsn = -1;
        ILSMIndex index = null;
        LocalResource localResource = null;
        ILocalResourceMetadata localResourceMetadata = null;
        Map<Long, Long> resourceId2MaxLsnMap = new HashMap<Long, Long>();
        TxnId jobLevelTxnId = new TxnId(-1, -1, -1);
        boolean foundWinnerTxn = false;

        //#. get indexLifeCycleManager 
        IAsterixAppRuntimeContextProvider appRuntimeContext = txnSubsystem.getAsterixAppRuntimeContextProvider();
        IIndexLifecycleManager indexLifecycleManager = appRuntimeContext.getIndexLifecycleManager();
        ILocalResourceRepository localResourceRepository = appRuntimeContext.getLocalResourceRepository();

        logRecord = logReader.next();
        while (logRecord != null) {
            lsn = logRecord.getLSN();
            foundWinnerTxn = false;
            if (LogManager.IS_DEBUG_MODE) {
                System.out.println(logRecord.getLogRecordForDisplay());
            }
            switch (logRecord.getLogType()) {
                case LogType.UPDATE:
                    tempKeyTxnId.setTxnId(logRecord.getJobId(), logRecord.getDatasetId(), logRecord.getPKHashValue());
                    jobLevelTxnId.setTxnId(logRecord.getJobId(), -1, -1);
                    if (winnerTxnTable.containsKey(tempKeyTxnId)) {
                        commitLsn = winnerTxnTable.get(tempKeyTxnId);
                        if (lsn < commitLsn) {
                            foundWinnerTxn = true;
                        }
                    } else if (winnerTxnTable.containsKey(jobLevelTxnId)) {
                        commitLsn = winnerTxnTable.get(jobLevelTxnId);
                        if (lsn < commitLsn) {
                            foundWinnerTxn = true;
                        }
                    }

                    if (foundWinnerTxn) {
                        resourceId = logRecord.getResourceId();
                        localResource = localResourceRepository.getResourceById(resourceId);

                        //get index instance from IndexLifeCycleManager
                        //if index is not registered into IndexLifeCycleManager,
                        //create the index using LocalMetadata stored in LocalResourceRepository
                        index = (ILSMIndex) indexLifecycleManager.getIndex(resourceId);
                        if (index == null) {

                            /*******************************************************************
                             * [Notice]
                             * -> Issue
                             * Delete index may cause a problem during redo.
                             * The index operation to be redone couldn't be redone because the corresponding index
                             * may not exist in NC due to the possible index drop DDL operation.
                             * -> Approach
                             * Avoid the problem during redo.
                             * More specifically, the problem will be detected when the localResource of
                             * the corresponding index is retrieved, which will end up with 'null' return from
                             * localResourceRepository. If null is returned, then just go and process the next
                             * log record.
                             *******************************************************************/
                            if (localResource == null) {
                                continue;
                            }
                            /*******************************************************************/

                            //#. create index instance and register to indexLifeCycleManager
                            localResourceMetadata = (ILocalResourceMetadata) localResource.getResourceObject();
                            index = localResourceMetadata.createIndexInstance(appRuntimeContext,
                                    localResource.getResourceName(), localResource.getPartition());
                            indexLifecycleManager.register(resourceId, index);
                            indexLifecycleManager.open(resourceId);

                            //#. get maxDiskLastLSN
                            ILSMIndex lsmIndex = (ILSMIndex) index;
                            BaseOperationTracker indexOpTracker = (BaseOperationTracker) lsmIndex.getOperationTracker();
                            AbstractLSMIOOperationCallback abstractLSMIOCallback = (AbstractLSMIOOperationCallback) indexOpTracker
                                    .getIOOperationCallback();
                            maxDiskLastLsn = abstractLSMIOCallback.getComponentLSN(index.getImmutableComponents());

                            //#. set resourceId and maxDiskLastLSN to the map
                            resourceId2MaxLsnMap.put(resourceId, maxDiskLastLsn);
                        } else {
                            maxDiskLastLsn = resourceId2MaxLsnMap.get(resourceId);
                        }

                        if (lsn > maxDiskLastLsn) {
                            redo(logRecord);
                            if (IS_DEBUG_MODE) {
                                redoCount++;
                            }
                        }
                    }
                    break;

                case LogType.JOB_COMMIT:
                case LogType.ENTITY_COMMIT:
                    //do nothing
                    break;

                default:
                    throw new ACIDException("Unsupported LogType: " + logRecord.getLogType());
            }

            logRecord = logReader.next();
        }

        //close all indexes
        Set<Long> resourceIdList = resourceId2MaxLsnMap.keySet();
        for (long r : resourceIdList) {
            indexLifecycleManager.close(r);
        }

        logReader.close();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("[RecoveryMgr] recovery is completed.");
        }
        if (IS_DEBUG_MODE) {
            System.out.println("[RecoveryMgr] Count: Update/Commit/Redo = " + updateLogCount + "/" + commitLogCount
                    + "/" + redoCount);
        }
    }

    @Override
    public synchronized void checkpoint(boolean isSharpCheckpoint) throws ACIDException, HyracksDataException {

        long minMCTFirstLSN;
        if (isSharpCheckpoint && LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting sharp checkpoint ... ");
        }

        TransactionManager txnMgr = (TransactionManager) txnSubsystem.getTransactionManager();
        String logDir = logMgr.getLogManagerProperties().getLogDir();

        //#. get the filename of the previous checkpoint files which are about to be deleted 
        //   right after the new checkpoint file is written.
        File[] prevCheckpointFiles = getPreviousCheckpointFiles();

        IIndexLifecycleManager indexLifecycleManager = txnSubsystem.getAsterixAppRuntimeContextProvider()
                .getIndexLifecycleManager();
        List<IIndex> openIndexList = indexLifecycleManager.getOpenIndexes();

        //#. flush all in-memory components if it is the sharp checkpoint
        if (isSharpCheckpoint) {
            ///////////////////////////////////////////////
            //TODO : change the code inside the if statement into indexLifeCycleManager.flushAllDatasets()
            //indexLifeCycleManager.flushAllDatasets();
            ///////////////////////////////////////////////
            List<BlockingIOOperationCallbackWrapper> callbackList = new LinkedList<BlockingIOOperationCallbackWrapper>();
            for (IIndex index : openIndexList) {
                ILSMIndex lsmIndex = (ILSMIndex) index;
                ILSMIndexAccessor indexAccessor = lsmIndex.createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);
                BaseOperationTracker indexOpTracker = (BaseOperationTracker) lsmIndex.getOperationTracker();
                BlockingIOOperationCallbackWrapper cb = new BlockingIOOperationCallbackWrapper(
                        indexOpTracker.getIOOperationCallback());
                callbackList.add(cb);
                try {
                    indexAccessor.scheduleFlush(cb);
                } catch (HyracksDataException e) {
                    throw new ACIDException(e);
                }
            }

            for (BlockingIOOperationCallbackWrapper cb : callbackList) {
                try {
                    cb.waitForIO();
                } catch (InterruptedException e) {
                    throw new ACIDException(e);
                }
            }
            minMCTFirstLSN = -2;
        } else {
            long firstLSN;
            minMCTFirstLSN = Long.MAX_VALUE;
            if (openIndexList.size() > 0) {
                for (IIndex index : openIndexList) {
                    firstLSN = ((BaseOperationTracker) ((ILSMIndex) index).getOperationTracker()).getFirstLSN();
                    minMCTFirstLSN = Math.min(minMCTFirstLSN, firstLSN);
                }
            } else {
                minMCTFirstLSN = -1;
            }
        }
        CheckpointObject checkpointObject = new CheckpointObject(logMgr.getAppendLSN(), minMCTFirstLSN,
                txnMgr.getMaxJobId(), System.currentTimeMillis());

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
            // sort the filenames lexicographically to keep the latest checkpointHistory files.
            Arrays.sort(prevCheckpointFiles);
            for (int i = 0; i < prevCheckpointFiles.length - this.checkpointHistory; ++i) {
                prevCheckpointFiles[i].delete();
            }
        }

        if (isSharpCheckpoint) {
            logMgr.renewLogFiles();
        }

        if (isSharpCheckpoint && LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Completed sharp checkpoint.");
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
        String logDir = ((LogManager) txnSubsystem.getLogManager()).getLogManagerProperties().getLogDir();

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
    public void rollbackTransaction(ITransactionContext txnContext) throws ACIDException {
        Map<TxnId, List<Long>> loserTxnTable = new HashMap<TxnId, List<Long>>();
        TxnId tempKeyTxnId = new TxnId(-1, -1, -1);

        int updateLogCount = 0;
        int commitLogCount = 0;

        // Obtain the first log record written by the Job
        long firstLSN = txnContext.getFirstLSN();
        long lastLSN = txnContext.getLastLSN();
        //TODO: make sure that the lastLsn is not updated anymore by another thread belonging to the same job.
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" rollbacking transaction log records from " + firstLSN + " to " + lastLSN);
        }

        // check if the transaction actually wrote some logs.
        if (firstLSN == TransactionManagementConstants.LogManagerConstants.TERMINAL_LSN) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(" no need to roll back as there were no operations by the transaction "
                        + txnContext.getJobId());
            }
            return;
        }

        // While reading log records from firstLsn to lastLsn, collect uncommitted txn's Lsns
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" collecting loser transaction's LSNs from " + firstLSN + " to " + lastLSN);
        }
        boolean reachedLastLog = false;
        List<Long> undoLSNSet = null;
        ILogReader logReader = logMgr.getLogReader(false);
        logReader.initializeScan(firstLSN);
        ILogRecord logRecord = logReader.next();
        while (logRecord != null) {
            if (IS_DEBUG_MODE) {
                System.out.println(logRecord.getLogRecordForDisplay());
            }

            tempKeyTxnId.setTxnId(logRecord.getJobId(), logRecord.getDatasetId(), logRecord.getPKHashValue());
            switch (logRecord.getLogType()) {
                case LogType.UPDATE:
                    undoLSNSet = loserTxnTable.get(tempKeyTxnId);
                    if (undoLSNSet == null) {
                        TxnId txnId = new TxnId(logRecord.getJobId(), logRecord.getDatasetId(),
                                logRecord.getPKHashValue());
                        undoLSNSet = new LinkedList<Long>();
                        loserTxnTable.put(txnId, undoLSNSet);
                    }
                    undoLSNSet.add(logRecord.getLSN());
                    if (IS_DEBUG_MODE) {
                        updateLogCount++;
                        System.out.println("" + Thread.currentThread().getId() + "======> update[" + logRecord.getLSN()
                                + "]:" + tempKeyTxnId);
                    }
                    break;

                case LogType.JOB_COMMIT:
                case LogType.ENTITY_COMMIT:
                    undoLSNSet = loserTxnTable.get(tempKeyTxnId);
                    if (undoLSNSet != null) {
                        loserTxnTable.remove(tempKeyTxnId);
                    }
                    if (IS_DEBUG_MODE) {
                        commitLogCount++;
                        System.out.println("" + Thread.currentThread().getId() + "======> commit[" + logRecord.getLSN()
                                + "]" + tempKeyTxnId);
                    }
                    break;

                default:
                    throw new ACIDException("Unsupported LogType: " + logRecord.getLogType());
            }
            if (logRecord.getLSN() == lastLSN) {
                reachedLastLog = true;
                break;
            } else if (logRecord.getLSN() > lastLSN) {
                throw new IllegalStateException("LastLSN mismatch");
            }
            logRecord = logReader.next();
        }

        if (!reachedLastLog) {
            throw new ACIDException("LastLSN mismatch: " + lastLSN + " vs " + logRecord.getLSN()
                    + " during Rollback a transaction( " + txnContext.getJobId() + ")");
        }

        //undo loserTxn's effect
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" undoing loser transaction's effect");
        }

        TxnId txnId = null;
        Iterator<Entry<TxnId, List<Long>>> iter = loserTxnTable.entrySet().iterator();
        int undoCount = 0;
        while (iter.hasNext()) {
            //TODO 
            //Sort the lsns in order to undo in one pass. 

            Map.Entry<TxnId, List<Long>> loserTxn = (Map.Entry<TxnId, List<Long>>) iter.next();
            txnId = loserTxn.getKey();

            undoLSNSet = loserTxn.getValue();

            for (long undoLSN : undoLSNSet) {
                // here, all the log records are UPDATE type. So, we don't need to check the type again.

                //read the corresponding log record to be undone.
                logRecord = logReader.read(undoLSN);
                assert logRecord != null;
                if (IS_DEBUG_MODE) {
                    System.out.println(logRecord.getLogRecordForDisplay());
                }
                undo(logRecord);
                if (IS_DEBUG_MODE) {
                    undoCount++;
                }
            }
        }

        logReader.close();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" undone loser transaction's effect");
        }
        if (IS_DEBUG_MODE) {
            System.out.println("UpdateLogCount/CommitLogCount/UndoCount:" + updateLogCount + "/" + commitLogCount + "/"
                    + undoCount);
        }
    }

    @Override
    public void start() {
        //no op
    }

    @Override
    public void stop(boolean dumpState, OutputStream os) {
        //no op
    }

    private void undo(ILogRecord logRecord) {
        try {
            ILSMIndex index = (ILSMIndex) txnSubsystem.getAsterixAppRuntimeContextProvider().getIndexLifecycleManager()
                    .getIndex(logRecord.getResourceId());
            ILSMIndexAccessor indexAccessor = index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            if (logRecord.getResourceType() == ResourceType.LSM_BTREE) {
                if (logRecord.getOldOp() != IndexOperation.NOOP.ordinal()) {
                    if (logRecord.getOldOp() == IndexOperation.DELETE.ordinal()) {
                        indexAccessor.forceDelete(logRecord.getOldValue());
                    } else {
                        indexAccessor.forceInsert(logRecord.getOldValue());
                    }
                } else {
                    indexAccessor.forcePhysicalDelete(logRecord.getNewValue());
                }
            } else {
                if (logRecord.getNewOp() == IndexOperation.DELETE.ordinal()) {
                    indexAccessor.forceInsert(logRecord.getNewValue());
                } else {
                    indexAccessor.forceDelete(logRecord.getNewValue());
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to undo", e);
        }
    }

    private void redo(ILogRecord logRecord) {
        try {
            ILSMIndex index = (ILSMIndex) txnSubsystem.getAsterixAppRuntimeContextProvider().getIndexLifecycleManager()
                    .getIndex(logRecord.getResourceId());
            ILSMIndexAccessor indexAccessor = index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            if (logRecord.getNewOp() == IndexOperation.INSERT.ordinal()) {
                indexAccessor.forceInsert(logRecord.getNewValue());
            } else if (logRecord.getNewOp() == IndexOperation.DELETE.ordinal()) {
                indexAccessor.forceDelete(logRecord.getNewValue());
            } else {
                throw new IllegalStateException("Unsupported OperationType: " + logRecord.getNewOp());
            }
            ((BaseOperationTracker) index.getOperationTracker()).updateLastLSN(logRecord.getLSN());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to redo", e);
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
