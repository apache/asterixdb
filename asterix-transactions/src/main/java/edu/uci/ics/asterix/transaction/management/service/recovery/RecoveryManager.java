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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.ILocalResourceMetadata;
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
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
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
    private final long SHARP_CHECKPOINT_LSN = -1;

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

        long readableSmallestLSN = logMgr.getReadableSmallestLSN();
        if (logMgr.getAppendLSN() == readableSmallestLSN) {
            if (checkpointObject.getMinMCTFirstLsn() != SHARP_CHECKPOINT_LSN) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("[Warning] ---------------------------------------------------");
                    LOGGER.info("[Warning] Some(or all) of transaction log files are lost.");
                    LOGGER.info("[Warning] ---------------------------------------------------");
                    //No choice but continuing when the log files are lost. 
                }
            }
            state = SystemState.HEALTHY;
            return state;
        } else if (checkpointObject.getCheckpointLsn() == logMgr.getAppendLSN()
                && checkpointObject.getMinMCTFirstLsn() == SHARP_CHECKPOINT_LSN) {
            state = SystemState.HEALTHY;
            return state;
        } else {
            state = SystemState.CORRUPTED;
            return state;
        }
    }

    public void startRecovery(boolean synchronous) throws IOException, ACIDException {

        int updateLogCount = 0;
        int entityCommitLogCount = 0;
        int jobCommitLogCount = 0;
        int redoCount = 0;
        int abortLogCount = 0;
        int jobId = -1;

        state = SystemState.RECOVERING;

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("[RecoveryMgr] starting recovery ...");
        }

        Set<Integer> winnerJobSet = new HashSet<Integer>();
        Map<Integer, Set<TxnId>> jobId2WinnerEntitiesMap = new HashMap<Integer, Set<TxnId>>();
        //winnerEntity is used to add pairs, <committed TxnId, the most recent commit Lsn of the TxnId>
        Set<TxnId> winnerEntitySet = null;
        TxnId tempKeyTxnId = new TxnId(-1, -1, -1, null, -1, false);
        TxnId winnerEntity = null;

        //#. read checkpoint file and set lowWaterMark where anaylsis and redo start
        long readableSmallestLSN = logMgr.getReadableSmallestLSN();
        CheckpointObject checkpointObject = readCheckpoint();
        long lowWaterMarkLSN = checkpointObject.getMinMCTFirstLsn();
        if (lowWaterMarkLSN < readableSmallestLSN) {
            lowWaterMarkLSN = readableSmallestLSN;
        }
        int maxJobId = checkpointObject.getMaxJobId();

        //-------------------------------------------------------------------------
        //  [ analysis phase ]
        //  - collect all committed Lsn 
        //-------------------------------------------------------------------------
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("[RecoveryMgr] in analysis phase");
        }

        //#. set log reader to the lowWaterMarkLsn
        ILogReader logReader = logMgr.getLogReader(true);
        logReader.initializeScan(lowWaterMarkLSN);
        ILogRecord logRecord = logReader.next();
        while (logRecord != null) {
            if (IS_DEBUG_MODE) {
                LOGGER.info(logRecord.getLogRecordForDisplay());
            }
            //update max jobId
            if (logRecord.getJobId() > maxJobId) {
                maxJobId = logRecord.getJobId();
            }
            switch (logRecord.getLogType()) {
                case LogType.UPDATE:
                    updateLogCount++;
                    break;
                case LogType.JOB_COMMIT:
                    winnerJobSet.add(Integer.valueOf(logRecord.getJobId()));
                    jobId2WinnerEntitiesMap.remove(Integer.valueOf(logRecord.getJobId()));
                    jobCommitLogCount++;
                    break;
                case LogType.ENTITY_COMMIT:
                    jobId = logRecord.getJobId();
                    winnerEntity = new TxnId(jobId, logRecord.getDatasetId(), logRecord.getPKHashValue(),
                            logRecord.getPKValue(), logRecord.getPKValueSize(), true);
                    if (!jobId2WinnerEntitiesMap.containsKey(Integer.valueOf(jobId))) {
                        winnerEntitySet = new HashSet<TxnId>();
                        jobId2WinnerEntitiesMap.put(Integer.valueOf(jobId), winnerEntitySet);
                    } else {
                        winnerEntitySet = jobId2WinnerEntitiesMap.get(Integer.valueOf(jobId));
                    }
                    winnerEntitySet.add(winnerEntity);
                    entityCommitLogCount++;
                    break;
                case LogType.ABORT:
                    abortLogCount++;
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
        //    2) lsn > maxDiskLastLsn of the index --> guarantee idempotence
        //-------------------------------------------------------------------------
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("[RecoveryMgr] in redo phase");
        }
        long resourceId;
        long maxDiskLastLsn;
        long LSN = -1;
        ILSMIndex index = null;
        LocalResource localResource = null;
        ILocalResourceMetadata localResourceMetadata = null;
        Map<Long, Long> resourceId2MaxLSNMap = new HashMap<Long, Long>();
        boolean foundWinner = false;

        //#. get indexLifeCycleManager 
        IAsterixAppRuntimeContextProvider appRuntimeContext = txnSubsystem.getAsterixAppRuntimeContextProvider();
        IIndexLifecycleManager indexLifecycleManager = appRuntimeContext.getIndexLifecycleManager();
        ILocalResourceRepository localResourceRepository = appRuntimeContext.getLocalResourceRepository();

        //#. set log reader to the lowWaterMarkLsn again.
        logReader.initializeScan(lowWaterMarkLSN);
        logRecord = logReader.next();
        while (logRecord != null) {
            if (IS_DEBUG_MODE) {
                LOGGER.info(logRecord.getLogRecordForDisplay());
            }
            LSN = logRecord.getLSN();
            jobId = logRecord.getJobId();
            foundWinner = false;
            switch (logRecord.getLogType()) {
                case LogType.UPDATE:
                    if (winnerJobSet.contains(Integer.valueOf(jobId))) {
                        foundWinner = true;
                    } else if (jobId2WinnerEntitiesMap.containsKey(Integer.valueOf(jobId))) {
                        winnerEntitySet = jobId2WinnerEntitiesMap.get(Integer.valueOf(jobId));
                        tempKeyTxnId.setTxnId(jobId, logRecord.getDatasetId(), logRecord.getPKHashValue(),
                                logRecord.getPKValue(), logRecord.getPKValueSize());
                        if (winnerEntitySet.contains(tempKeyTxnId)) {
                            foundWinner = true;
                        }
                    }
                    if (foundWinner) {
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
                                logRecord = logReader.next();
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
                            maxDiskLastLsn = ((AbstractLSMIOOperationCallback) lsmIndex.getIOOperationCallback())
                                    .getComponentLSN(lsmIndex.getImmutableComponents());

                            //#. set resourceId and maxDiskLastLSN to the map
                            resourceId2MaxLSNMap.put(Long.valueOf(resourceId), Long.valueOf(maxDiskLastLsn));
                        } else {
                            maxDiskLastLsn = resourceId2MaxLSNMap.get(Long.valueOf(resourceId));
                        }

                        if (LSN > maxDiskLastLsn) {
                            redo(logRecord);
                            redoCount++;
                        }
                    }
                    break;

                case LogType.JOB_COMMIT:
                case LogType.ENTITY_COMMIT:
                case LogType.ABORT:
                    //do nothing
                    break;

                default:
                    throw new ACIDException("Unsupported LogType: " + logRecord.getLogType());
            }
            logRecord = logReader.next();
        }

        //close all indexes
        Set<Long> resourceIdList = resourceId2MaxLSNMap.keySet();
        for (long r : resourceIdList) {
            indexLifecycleManager.close(r);
        }

        logReader.close();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("[RecoveryMgr] recovery is completed.");
            LOGGER.info("[RecoveryMgr's recovery log count] update/entityCommit/jobCommit/abort/redo = " + updateLogCount + "/"
                    + entityCommitLogCount + "/" + jobCommitLogCount + "/" + abortLogCount + "/" + redoCount);
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
                BlockingIOOperationCallbackWrapper cb = new BlockingIOOperationCallbackWrapper(
                        lsmIndex.getIOOperationCallback());
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
            minMCTFirstLSN = SHARP_CHECKPOINT_LSN;
        } else {
            long firstLSN;
            minMCTFirstLSN = Long.MAX_VALUE;
            if (openIndexList.size() > 0) {
                for (IIndex index : openIndexList) {
                    firstLSN = ((AbstractLSMIOOperationCallback) ((ILSMIndex) index).getIOOperationCallback())
                            .getFirstLSN();
                    minMCTFirstLSN = Math.min(minMCTFirstLSN, firstLSN);
                }
            } else {
                minMCTFirstLSN = SHARP_CHECKPOINT_LSN;
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
        TxnId tempKeyTxnId = new TxnId(-1, -1, -1, null, -1, false);

        int updateLogCount = 0;
        int entityCommitLogCount = 0;
        int jobId = -1;
        int abortedJobId = txnContext.getJobId().getId();
        long currentLSN = -1;
        TxnId loserEntity = null;

        // Obtain the first/last log record LSNs written by the Job
        long firstLSN = txnContext.getFirstLSN();
        long lastLSN = txnContext.getLastLSN();
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
        List<Long> undoLSNSet = null;
        ILogReader logReader = logMgr.getLogReader(false);
        logReader.initializeScan(firstLSN);
        ILogRecord logRecord = null;
        while (currentLSN < lastLSN) {
            logRecord = logReader.next();
            if (logRecord == null) {
                break;
            } else {
                if (IS_DEBUG_MODE) {
                    LOGGER.info(logRecord.getLogRecordForDisplay());
                }
                currentLSN = logRecord.getLSN();
            }
            jobId = logRecord.getJobId();
            if (jobId != abortedJobId) {
                continue;
            }
            tempKeyTxnId.setTxnId(jobId, logRecord.getDatasetId(), logRecord.getPKHashValue(), logRecord.getPKValue(),
                    logRecord.getPKValueSize());
            switch (logRecord.getLogType()) {
                case LogType.UPDATE:
                    undoLSNSet = loserTxnTable.get(tempKeyTxnId);
                    if (undoLSNSet == null) {
                        loserEntity = new TxnId(jobId, logRecord.getDatasetId(), logRecord.getPKHashValue(),
                                logRecord.getPKValue(), logRecord.getPKValueSize(), true);
                        undoLSNSet = new LinkedList<Long>();
                        loserTxnTable.put(loserEntity, undoLSNSet);
                    }
                    undoLSNSet.add(Long.valueOf(currentLSN));
                    updateLogCount++;
                    if (IS_DEBUG_MODE) {
                        LOGGER.info("" + Thread.currentThread().getId() + "======> update[" + currentLSN + "]:"
                                + tempKeyTxnId);
                    }
                    break;

                case LogType.JOB_COMMIT:
                    throw new ACIDException("Unexpected LogType(" + logRecord.getLogType() + ") during abort.");

                case LogType.ENTITY_COMMIT:
                    undoLSNSet = loserTxnTable.remove(tempKeyTxnId);
                    if (undoLSNSet == null) {
                        undoLSNSet = loserTxnTable.remove(tempKeyTxnId);
                    }
                    entityCommitLogCount++;
                    if (IS_DEBUG_MODE) {
                        LOGGER.info("" + Thread.currentThread().getId() + "======> entity_commit[" + currentLSN + "]"
                                + tempKeyTxnId);
                    }
                    break;

                case LogType.ABORT:
                    //ignore
                    break;

                default:
                    throw new ACIDException("Unsupported LogType: " + logRecord.getLogType());
            }
        }
        if (currentLSN != lastLSN) {
            throw new ACIDException("LastLSN mismatch: lastLSN(" + lastLSN + ") vs currentLSN(" + currentLSN
                    + ") during abort( " + txnContext.getJobId() + ")");
        }

        //undo loserTxn's effect
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" undoing loser transaction's effect");
        }

        Iterator<Entry<TxnId, List<Long>>> iter = loserTxnTable.entrySet().iterator();
        int undoCount = 0;
        while (iter.hasNext()) {
            //TODO 
            //Sort the lsns in order to undo in one pass. 

            Map.Entry<TxnId, List<Long>> loserTxn = (Map.Entry<TxnId, List<Long>>) iter.next();
            undoLSNSet = loserTxn.getValue();

            for (long undoLSN : undoLSNSet) {
                //here, all the log records are UPDATE type. So, we don't need to check the type again.
                //read the corresponding log record to be undone.
                logRecord = logReader.read(undoLSN);
                if (logRecord == null) {
                    throw new ACIDException("IllegalState exception during abort( " + txnContext.getJobId() + ")");
                }
                if (IS_DEBUG_MODE) {
                    LOGGER.info(logRecord.getLogRecordForDisplay());
                }
                undo(logRecord);
                undoCount++;
            }
        }
        logReader.close();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" undone loser transaction's effect");
            LOGGER.info("[RecoveryManager's rollback log count] update/entityCommit/undo:" + updateLogCount + "/" + entityCommitLogCount + "/"
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
            ((AbstractLSMIOOperationCallback) index.getIOOperationCallback()).updateLastLSN(logRecord.getLSN());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to redo", e);
        }
    }
}

class TxnId {
    public boolean isByteArrayPKValue;
    public int jobId;
    public int datasetId;
    public int pkHashValue;
    public int pkSize;
    public byte[] byteArrayPKValue;
    public ITupleReference tupleReferencePKValue;

    public TxnId(int jobId, int datasetId, int pkHashValue, ITupleReference pkValue, int pkSize,
            boolean isByteArrayPKValue) {
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.pkHashValue = pkHashValue;
        this.pkSize = pkSize;
        this.isByteArrayPKValue = isByteArrayPKValue;
        if (isByteArrayPKValue) {
            this.byteArrayPKValue = new byte[pkSize];
            readPKValueIntoByteArray(pkValue, pkSize, byteArrayPKValue);
        } else {
            this.tupleReferencePKValue = pkValue;
        }
    }

    private void readPKValueIntoByteArray(ITupleReference pkValue, int pkSize, byte[] byteArrayPKValue) {
        int readOffset = pkValue.getFieldStart(0);
        byte[] readBuffer = pkValue.getFieldData(0);
        for (int i = 0; i < pkSize; i++) {
            byteArrayPKValue[i] = readBuffer[readOffset + i];
        }
    }

    public void setTxnId(int jobId, int datasetId, int pkHashValue, ITupleReference pkValue, int pkSize) {
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.pkHashValue = pkHashValue;
        this.tupleReferencePKValue = pkValue;
        this.pkSize = pkSize;
        isByteArrayPKValue = false;
    }

    @Override
    public String toString() {
        return "[" + jobId + "," + datasetId + "," + pkHashValue + "," + pkSize + "]";
    }

    @Override
    public int hashCode() {
        return pkHashValue;
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
        return (txnId.pkHashValue == pkHashValue && txnId.datasetId == datasetId && txnId.jobId == jobId
                && pkSize == txnId.pkSize && isEqualTo(txnId));
    }

    private boolean isEqualTo(TxnId txnId) {
        if (isByteArrayPKValue && txnId.isByteArrayPKValue) {
            return isEqual(byteArrayPKValue, txnId.byteArrayPKValue, pkSize);
        } else if (isByteArrayPKValue && (!txnId.isByteArrayPKValue)) {
            return isEqual(byteArrayPKValue, txnId.tupleReferencePKValue, pkSize);
        } else if ((!isByteArrayPKValue) && txnId.isByteArrayPKValue) {
            return isEqual(txnId.byteArrayPKValue, tupleReferencePKValue, pkSize);
        } else {
            return isEqual(tupleReferencePKValue, txnId.tupleReferencePKValue, pkSize);
        }
    }

    private boolean isEqual(byte[] a, byte[] b, int size) {
        for (int i = 0; i < size; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    private boolean isEqual(byte[] a, ITupleReference b, int size) {
        int readOffset = b.getFieldStart(0);
        byte[] readBuffer = b.getFieldData(0);
        for (int i = 0; i < size; i++) {
            if (a[i] != readBuffer[readOffset + i]) {
                return false;
            }
        }
        return true;
    }

    private boolean isEqual(ITupleReference a, ITupleReference b, int size) {
        int aOffset = a.getFieldStart(0);
        byte[] aBuffer = a.getFieldData(0);
        int bOffset = b.getFieldStart(0);
        byte[] bBuffer = b.getFieldData(0);
        for (int i = 0; i < size; i++) {
            if (aBuffer[aOffset + i] != bBuffer[bOffset + i]) {
                return false;
            }
        }
        return true;
    }
}
