/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.app.nc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.common.transactions.IAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.ICheckpointManager;
import org.apache.asterix.common.transactions.ILogReader;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.service.logging.LogManager;
import org.apache.asterix.transaction.management.service.recovery.AbstractCheckpointManager;
import org.apache.asterix.transaction.management.service.recovery.TxnId;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.LocalResource;

/**
 * This is the Recovery Manager and is responsible for rolling back a
 * transaction as well as doing a system recovery.
 */
public class RecoveryManager implements IRecoveryManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;
    private static final Logger LOGGER = Logger.getLogger(RecoveryManager.class.getName());
    private final ITransactionSubsystem txnSubsystem;
    private final LogManager logMgr;
    private final boolean replicationEnabled;
    private static final String RECOVERY_FILES_DIR_NAME = "recovery_temp";
    private Map<Integer, JobEntityCommits> jobId2WinnerEntitiesMap = null;
    private final long cachedEntityCommitsPerJobSize;
    private final PersistentLocalResourceRepository localResourceRepository;
    private final ICheckpointManager checkpointManager;
    private SystemState state;
    private final INCServiceContext serviceCtx;

    public RecoveryManager(ITransactionSubsystem txnSubsystem, INCServiceContext serviceCtx) {
        this.serviceCtx = serviceCtx;
        this.txnSubsystem = txnSubsystem;
        logMgr = (LogManager) txnSubsystem.getLogManager();
        ReplicationProperties repProperties =
                txnSubsystem.getAsterixAppRuntimeContextProvider().getAppContext().getReplicationProperties();
        replicationEnabled = repProperties.isParticipant(txnSubsystem.getId());
        localResourceRepository = (PersistentLocalResourceRepository) txnSubsystem.getAsterixAppRuntimeContextProvider()
                .getLocalResourceRepository();
        cachedEntityCommitsPerJobSize = txnSubsystem.getTransactionProperties().getJobRecoveryMemorySize();
        checkpointManager = txnSubsystem.getCheckpointManager();
    }

    /**
     * returns system state which could be one of the three states: HEALTHY, RECOVERING, CORRUPTED.
     * This state information could be used in a case where more than one thread is running
     * in the bootstrap process to provide higher availability. In other words, while the system
     * is recovered, another thread may start a new transaction with understanding the side effect
     * of the operation, or the system can be recovered concurrently. This kind of concurrency is
     * not supported, yet.
     */
    @Override
    public SystemState getSystemState() throws ACIDException {
        //read checkpoint file
        Checkpoint checkpointObject = checkpointManager.getLatest();
        if (checkpointObject == null) {
            //The checkpoint file doesn't exist => Failure happened during NC initialization.
            //Retry to initialize the NC by setting the state to PERMANENT_DATA_LOSS
            state = SystemState.PERMANENT_DATA_LOSS;
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("The checkpoint file doesn't exist: systemState = PERMANENT_DATA_LOSS");
            }
            return state;
        }

        if (replicationEnabled) {
            if (checkpointObject.getMinMCTFirstLsn() == AbstractCheckpointManager.SHARP_CHECKPOINT_LSN) {
                //no logs exist
                state = SystemState.HEALTHY;
            } else if (checkpointObject.getCheckpointLsn() == logMgr.getAppendLSN() && checkpointObject.isSharp()) {
                //only remote logs exist
                state = SystemState.HEALTHY;
            } else {
                //need to perform remote recovery
                state = SystemState.CORRUPTED;
            }
        } else {
            long readableSmallestLSN = logMgr.getReadableSmallestLSN();
            if (logMgr.getAppendLSN() == readableSmallestLSN) {
                if (checkpointObject.getMinMCTFirstLsn() != AbstractCheckpointManager.SHARP_CHECKPOINT_LSN) {
                    LOGGER.warning("Some(or all) of transaction log files are lost.");
                    //No choice but continuing when the log files are lost.
                }
                state = SystemState.HEALTHY;
            } else if (checkpointObject.getCheckpointLsn() == logMgr.getAppendLSN()
                    && checkpointObject.getMinMCTFirstLsn() == AbstractCheckpointManager.SHARP_CHECKPOINT_LSN) {
                state = SystemState.HEALTHY;
            } else {
                state = SystemState.CORRUPTED;
            }
        }
        return state;
    }

    //This method is used only when replication is disabled.
    @Override
    public void startRecovery(boolean synchronous) throws IOException, ACIDException {
        state = SystemState.RECOVERING;
        LOGGER.log(Level.INFO, "starting recovery ...");

        long readableSmallestLSN = logMgr.getReadableSmallestLSN();
        Checkpoint checkpointObject = checkpointManager.getLatest();
        long lowWaterMarkLSN = checkpointObject.getMinMCTFirstLsn();
        if (lowWaterMarkLSN < readableSmallestLSN) {
            lowWaterMarkLSN = readableSmallestLSN;
        }

        //delete any recovery files from previous failed recovery attempts
        deleteRecoveryTemporaryFiles();

        //get active partitions on this node
        Set<Integer> activePartitions = localResourceRepository.getNodeOrignalPartitions();
        replayPartitionsLogs(activePartitions, logMgr.getLogReader(true), lowWaterMarkLSN);
    }

    @Override
    public void startLocalRecovery(Set<Integer> partitions) throws IOException, ACIDException {
        state = SystemState.RECOVERING;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("starting recovery ...");
        }

        long readableSmallestLSN = logMgr.getReadableSmallestLSN();
        Checkpoint checkpointObject = checkpointManager.getLatest();
        long lowWaterMarkLSN = checkpointObject.getMinMCTFirstLsn();
        if (lowWaterMarkLSN < readableSmallestLSN) {
            lowWaterMarkLSN = readableSmallestLSN;
        }

        //delete any recovery files from previous failed recovery attempts
        deleteRecoveryTemporaryFiles();

        //get active partitions on this node
        replayPartitionsLogs(partitions, logMgr.getLogReader(true), lowWaterMarkLSN);
    }

    @Override
    public synchronized void replayPartitionsLogs(Set<Integer> partitions, ILogReader logReader, long lowWaterMarkLSN)
            throws IOException, ACIDException {
        try {
            Set<Integer> winnerJobSet = startRecoverysAnalysisPhase(partitions, logReader, lowWaterMarkLSN);
            startRecoveryRedoPhase(partitions, logReader, lowWaterMarkLSN, winnerJobSet);
        } finally {
            logReader.close();
            deleteRecoveryTemporaryFiles();
        }
    }

    private synchronized Set<Integer> startRecoverysAnalysisPhase(Set<Integer> partitions, ILogReader logReader,
            long lowWaterMarkLSN) throws IOException, ACIDException {
        int updateLogCount = 0;
        int entityCommitLogCount = 0;
        int jobCommitLogCount = 0;
        int abortLogCount = 0;
        Set<Integer> winnerJobSet = new HashSet<>();
        jobId2WinnerEntitiesMap = new HashMap<>();
        //set log reader to the lowWaterMarkLsn
        ILogRecord logRecord;
        logReader.initializeScan(lowWaterMarkLSN);
        logRecord = logReader.next();
        while (logRecord != null) {
            if (IS_DEBUG_MODE) {
                LOGGER.info(logRecord.getLogRecordForDisplay());
            }
            switch (logRecord.getLogType()) {
                case LogType.UPDATE:
                    if (partitions.contains(logRecord.getResourcePartition())) {
                        updateLogCount++;
                    }
                    break;
                case LogType.JOB_COMMIT:
                    winnerJobSet.add(logRecord.getJobId());
                    cleanupJobCommits(logRecord.getJobId());
                    jobCommitLogCount++;
                    break;
                case LogType.ENTITY_COMMIT:
                    if (partitions.contains(logRecord.getResourcePartition())) {
                        analyzeEntityCommitLog(logRecord);
                        entityCommitLogCount++;
                    }
                    break;
                case LogType.ABORT:
                    abortLogCount++;
                    break;
                case LogType.FLUSH:
                case LogType.WAIT:
                case LogType.MARKER:
                    break;
                default:
                    throw new ACIDException("Unsupported LogType: " + logRecord.getLogType());
            }
            logRecord = logReader.next();
        }

        //prepare winners for search after analysis is done to flush anything remaining in memory to disk.
        for (JobEntityCommits winners : jobId2WinnerEntitiesMap.values()) {
            winners.prepareForSearch();
        }

        LOGGER.info("Logs analysis phase completed.");
        LOGGER.info("Analysis log count update/entityCommit/jobCommit/abort = " + updateLogCount + "/"
                + entityCommitLogCount + "/" + jobCommitLogCount + "/" + abortLogCount);

        return winnerJobSet;
    }

    private void cleanupJobCommits(int jobId) {
        if (jobId2WinnerEntitiesMap.containsKey(jobId)) {
            JobEntityCommits jobEntityWinners = jobId2WinnerEntitiesMap.get(jobId);
            //to delete any spilled files as well
            jobEntityWinners.clear();
            jobId2WinnerEntitiesMap.remove(jobId);
        }
    }

    private void analyzeEntityCommitLog(ILogRecord logRecord) throws IOException {
        int jobId = logRecord.getJobId();
        JobEntityCommits jobEntityWinners;
        if (!jobId2WinnerEntitiesMap.containsKey(jobId)) {
            jobEntityWinners = new JobEntityCommits(jobId);
            if (needToFreeMemory()) {
                // If we don't have enough memory for one more job,
                // we will force all jobs to spill their cached entities to disk.
                // This could happen only when we have many jobs with small
                // number of records and none of them have job commit.
                freeJobsCachedEntities(jobId);
            }
            jobId2WinnerEntitiesMap.put(jobId, jobEntityWinners);
        } else {
            jobEntityWinners = jobId2WinnerEntitiesMap.get(jobId);
        }
        jobEntityWinners.add(logRecord);
    }

    private synchronized void startRecoveryRedoPhase(Set<Integer> partitions, ILogReader logReader,
            long lowWaterMarkLSN, Set<Integer> winnerJobSet) throws IOException, ACIDException {
        int redoCount = 0;
        int jobId = -1;

        long resourceId;
        long maxDiskLastLsn;
        long lsn = -1;
        ILSMIndex index = null;
        LocalResource localResource = null;
        DatasetLocalResource localResourceMetadata = null;
        boolean foundWinner = false;
        JobEntityCommits jobEntityWinners = null;

        IAppRuntimeContextProvider appRuntimeContext = txnSubsystem.getAsterixAppRuntimeContextProvider();
        IDatasetLifecycleManager datasetLifecycleManager = appRuntimeContext.getDatasetLifecycleManager();

        Map<Long, LocalResource> resourcesMap = localResourceRepository.loadAndGetAllResources();
        Map<Long, Long> resourceId2MaxLSNMap = new HashMap<>();
        TxnId tempKeyTxnId = new TxnId(-1, -1, -1, null, -1, false);

        ILogRecord logRecord = null;
        try {
            logReader.initializeScan(lowWaterMarkLSN);
            logRecord = logReader.next();
            while (logRecord != null) {
                if (IS_DEBUG_MODE) {
                    LOGGER.info(logRecord.getLogRecordForDisplay());
                }
                lsn = logRecord.getLSN();
                jobId = logRecord.getJobId();
                foundWinner = false;
                switch (logRecord.getLogType()) {
                    case LogType.UPDATE:
                        if (partitions.contains(logRecord.getResourcePartition())) {
                            if (winnerJobSet.contains(jobId)) {
                                foundWinner = true;
                            } else if (jobId2WinnerEntitiesMap.containsKey(jobId)) {
                                jobEntityWinners = jobId2WinnerEntitiesMap.get(jobId);
                                tempKeyTxnId.setTxnId(jobId, logRecord.getDatasetId(), logRecord.getPKHashValue(),
                                        logRecord.getPKValue(), logRecord.getPKValueSize());
                                if (jobEntityWinners.containsEntityCommitForTxnId(lsn, tempKeyTxnId)) {
                                    foundWinner = true;
                                }
                            }
                            if (foundWinner) {
                                resourceId = logRecord.getResourceId();
                                localResource = resourcesMap.get(resourceId);
                                /*******************************************************************
                                 * [Notice]
                                 * -> Issue
                                 * Delete index may cause a problem during redo.
                                 * The index operation to be redone couldn't be redone because the corresponding index
                                 * may not exist in NC due to the possible index drop DDL operation.
                                 * -> Approach
                                 * Avoid the problem during redo.
                                 * More specifically, the problem will be detected when the localResource of
                                 * the corresponding index is retrieved, which will end up with 'null'.
                                 * If null is returned, then just go and process the next
                                 * log record.
                                 *******************************************************************/
                                if (localResource == null) {
                                    LOGGER.log(Level.WARNING, "resource was not found for resource id " + resourceId);
                                    logRecord = logReader.next();
                                    continue;
                                }
                                /*******************************************************************/

                                //get index instance from IndexLifeCycleManager
                                //if index is not registered into IndexLifeCycleManager,
                                //create the index using LocalMetadata stored in LocalResourceRepository
                                //get partition path in this node
                                localResourceMetadata = (DatasetLocalResource) localResource.getResource();
                                index = (ILSMIndex) datasetLifecycleManager.get(localResource.getPath());
                                if (index == null) {
                                    //#. create index instance and register to indexLifeCycleManager
                                    index = (ILSMIndex) localResourceMetadata.createInstance(serviceCtx);
                                    datasetLifecycleManager.register(localResource.getPath(), index);
                                    datasetLifecycleManager.open(localResource.getPath());

                                    //#. get maxDiskLastLSN
                                    ILSMIndex lsmIndex = index;
                                    try {
                                        maxDiskLastLsn =
                                                ((AbstractLSMIOOperationCallback) lsmIndex.getIOOperationCallback())
                                                        .getComponentLSN(lsmIndex.getImmutableComponents());
                                    } catch (HyracksDataException e) {
                                        datasetLifecycleManager.close(localResource.getPath());
                                        throw e;
                                    }

                                    //#. set resourceId and maxDiskLastLSN to the map
                                    resourceId2MaxLSNMap.put(resourceId, maxDiskLastLsn);
                                } else {
                                    maxDiskLastLsn = resourceId2MaxLSNMap.get(resourceId);
                                }

                                if (lsn > maxDiskLastLsn) {
                                    redo(logRecord, datasetLifecycleManager);
                                    redoCount++;
                                }
                            }
                        }
                        break;
                    case LogType.JOB_COMMIT:
                    case LogType.ENTITY_COMMIT:
                    case LogType.ABORT:
                    case LogType.FLUSH:
                    case LogType.WAIT:
                    case LogType.MARKER:
                        //do nothing
                        break;
                    default:
                        throw new ACIDException("Unsupported LogType: " + logRecord.getLogType());
                }
                logRecord = logReader.next();
            }
            LOGGER.info("Logs REDO phase completed. Redo logs count: " + redoCount);
        } finally {
            //close all indexes
            Set<Long> resourceIdList = resourceId2MaxLSNMap.keySet();
            for (long r : resourceIdList) {
                datasetLifecycleManager.close(resourcesMap.get(r).getPath());
            }
        }
    }

    private boolean needToFreeMemory() {
        return Runtime.getRuntime().freeMemory() < cachedEntityCommitsPerJobSize;
    }

    @Override
    public long getMinFirstLSN() throws HyracksDataException {
        long minFirstLSN = getLocalMinFirstLSN();

        //if replication is enabled, consider replica resources min LSN
        if (replicationEnabled) {
            long remoteMinFirstLSN = getRemoteMinFirstLSN();
            minFirstLSN = Math.min(minFirstLSN, remoteMinFirstLSN);
        }

        return minFirstLSN;
    }

    @Override
    public long getLocalMinFirstLSN() throws HyracksDataException {
        IDatasetLifecycleManager datasetLifecycleManager =
                txnSubsystem.getAsterixAppRuntimeContextProvider().getDatasetLifecycleManager();
        List<IIndex> openIndexList = datasetLifecycleManager.getOpenResources();
        long firstLSN;
        //the min first lsn can only be the current append or smaller
        long minFirstLSN = logMgr.getAppendLSN();
        if (!openIndexList.isEmpty()) {
            for (IIndex index : openIndexList) {
                AbstractLSMIOOperationCallback ioCallback =
                        (AbstractLSMIOOperationCallback) ((ILSMIndex) index).getIOOperationCallback();
                if (!((AbstractLSMIndex) index).isCurrentMutableComponentEmpty() || ioCallback.hasPendingFlush()) {
                    firstLSN = ioCallback.getFirstLSN();
                    minFirstLSN = Math.min(minFirstLSN, firstLSN);
                }
            }
        }
        return minFirstLSN;
    }

    private long getRemoteMinFirstLSN() {
        IReplicaResourcesManager remoteResourcesManager =
                txnSubsystem.getAsterixAppRuntimeContextProvider().getAppContext().getReplicaResourcesManager();
        return remoteResourcesManager.getPartitionsMinLSN(localResourceRepository.getInactivePartitions());
    }

    @Override
    public File createJobRecoveryFile(int jobId, String fileName) throws IOException {
        String recoveryDirPath = getRecoveryDirPath();
        Path jobRecoveryFolder = Paths.get(recoveryDirPath + File.separator + jobId);
        if (!Files.exists(jobRecoveryFolder)) {
            Files.createDirectories(jobRecoveryFolder);
        }

        File jobRecoveryFile = new File(jobRecoveryFolder.toString() + File.separator + fileName);
        if (!jobRecoveryFile.exists()) {
            if (!jobRecoveryFile.createNewFile()) {
                throw new IOException("Failed to create file: " + fileName + " for job id(" + jobId + ")");
            }
        } else {
            throw new IOException("File: " + fileName + " for job id(" + jobId + ") already exists");
        }
        return jobRecoveryFile;
    }

    @Override
    public void deleteRecoveryTemporaryFiles() {
        String recoveryDirPath = getRecoveryDirPath();
        Path recoveryFolderPath = Paths.get(recoveryDirPath);
        FileUtils.deleteQuietly(recoveryFolderPath.toFile());
    }

    private String getRecoveryDirPath() {
        String logDir = logMgr.getLogManagerProperties().getLogDir();
        if (!logDir.endsWith(File.separator)) {
            logDir += File.separator;
        }

        return logDir + RECOVERY_FILES_DIR_NAME;
    }

    private void freeJobsCachedEntities(int requestingJobId) throws IOException {
        if (jobId2WinnerEntitiesMap != null) {
            for (Entry<Integer, JobEntityCommits> jobEntityCommits : jobId2WinnerEntitiesMap.entrySet()) {
                //if the job is not the requester, free its memory
                if (jobEntityCommits.getKey() != requestingJobId) {
                    jobEntityCommits.getValue().spillToDiskAndfreeMemory();
                }
            }
        }
    }

    @Override
    public void rollbackTransaction(ITransactionContext txnContext) throws ACIDException {
        int abortedJobId = txnContext.getJobId().getId();
        // Obtain the first/last log record LSNs written by the Job
        long firstLSN = txnContext.getFirstLSN();
        /**
         * The effect of any log record with LSN below minFirstLSN has already been written to disk and
         * will not be rolled back. Therefore, we will set the first LSN of the job to the maximum of
         * minFirstLSN and the job's first LSN.
         */
        try {
            long localMinFirstLSN = getLocalMinFirstLSN();
            firstLSN = Math.max(firstLSN, localMinFirstLSN);
        } catch (HyracksDataException e) {
            throw new ACIDException(e);
        }
        long lastLSN = txnContext.getLastLSN();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("rollbacking transaction log records from " + firstLSN + " to " + lastLSN);
        }
        // check if the transaction actually wrote some logs.
        if (firstLSN == TransactionManagementConstants.LogManagerConstants.TERMINAL_LSN || firstLSN > lastLSN) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("no need to roll back as there were no operations by the job " + txnContext.getJobId());
            }
            return;
        }

        // While reading log records from firstLsn to lastLsn, collect uncommitted txn's Lsns
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("collecting loser transaction's LSNs from " + firstLSN + " to " + lastLSN);
        }

        Map<TxnId, List<Long>> jobLoserEntity2LSNsMap = new HashMap<>();
        TxnId tempKeyTxnId = new TxnId(-1, -1, -1, null, -1, false);
        int updateLogCount = 0;
        int entityCommitLogCount = 0;
        int logJobId = -1;
        long currentLSN = -1;
        TxnId loserEntity = null;
        List<Long> undoLSNSet = null;
        //get active partitions on this node
        Set<Integer> activePartitions = localResourceRepository.getActivePartitions();
        ILogReader logReader = logMgr.getLogReader(false);
        try {
            logReader.initializeScan(firstLSN);
            ILogRecord logRecord = null;
            while (currentLSN < lastLSN) {
                logRecord = logReader.next();
                if (logRecord == null) {
                    break;
                } else {
                    currentLSN = logRecord.getLSN();

                    if (IS_DEBUG_MODE) {
                        LOGGER.info(logRecord.getLogRecordForDisplay());
                    }
                }
                logJobId = logRecord.getJobId();
                if (logJobId != abortedJobId) {
                    continue;
                }
                tempKeyTxnId.setTxnId(logJobId, logRecord.getDatasetId(), logRecord.getPKHashValue(),
                        logRecord.getPKValue(), logRecord.getPKValueSize());
                switch (logRecord.getLogType()) {
                    case LogType.UPDATE:
                        if (activePartitions.contains(logRecord.getResourcePartition())) {
                            undoLSNSet = jobLoserEntity2LSNsMap.get(tempKeyTxnId);
                            if (undoLSNSet == null) {
                                loserEntity = new TxnId(logJobId, logRecord.getDatasetId(), logRecord.getPKHashValue(),
                                        logRecord.getPKValue(), logRecord.getPKValueSize(), true);
                                undoLSNSet = new LinkedList<>();
                                jobLoserEntity2LSNsMap.put(loserEntity, undoLSNSet);
                            }
                            undoLSNSet.add(currentLSN);
                            updateLogCount++;
                            if (IS_DEBUG_MODE) {
                                LOGGER.info(Thread.currentThread().getId() + "======> update[" + currentLSN + "]:"
                                        + tempKeyTxnId);
                            }
                        }
                        break;
                    case LogType.ENTITY_COMMIT:
                        if (activePartitions.contains(logRecord.getResourcePartition())) {
                            jobLoserEntity2LSNsMap.remove(tempKeyTxnId);
                            entityCommitLogCount++;
                            if (IS_DEBUG_MODE) {
                                LOGGER.info(Thread.currentThread().getId() + "======> entity_commit[" + currentLSN + "]"
                                        + tempKeyTxnId);
                            }
                        }
                        break;
                    case LogType.JOB_COMMIT:
                        throw new ACIDException("Unexpected LogType(" + logRecord.getLogType() + ") during abort.");
                    case LogType.ABORT:
                    case LogType.FLUSH:
                    case LogType.WAIT:
                    case LogType.MARKER:
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
            LOGGER.log(Level.INFO, "undoing loser transaction's effect");

            IDatasetLifecycleManager datasetLifecycleManager =
                    txnSubsystem.getAsterixAppRuntimeContextProvider().getDatasetLifecycleManager();
            //TODO sort loser entities by smallest LSN to undo in one pass.
            Iterator<Entry<TxnId, List<Long>>> iter = jobLoserEntity2LSNsMap.entrySet().iterator();
            int undoCount = 0;
            while (iter.hasNext()) {
                Map.Entry<TxnId, List<Long>> loserEntity2LSNsMap = iter.next();
                undoLSNSet = loserEntity2LSNsMap.getValue();
                // The step below is important since the upsert operations must be done in reverse order.
                Collections.reverse(undoLSNSet);
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
                    undo(logRecord, datasetLifecycleManager);
                    undoCount++;
                }
            }

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("undone loser transaction's effect");
                LOGGER.info("[RecoveryManager's rollback log count] update/entityCommit/undo:" + updateLogCount + "/"
                        + entityCommitLogCount + "/" + undoCount);
            }
        } finally {
            logReader.close();
        }
    }

    @Override
    public void start() {
        //no op
    }

    @Override
    public void stop(boolean dumpState, OutputStream os) throws IOException {
        // Shutdown checkpoint
        checkpointManager.doSharpCheckpoint();
    }

    @Override
    public void dumpState(OutputStream os) throws IOException {
        // do nothing
    }

    private static void undo(ILogRecord logRecord, IDatasetLifecycleManager datasetLifecycleManager) {
        try {
            ILSMIndex index =
                    (ILSMIndex) datasetLifecycleManager.getIndex(logRecord.getDatasetId(), logRecord.getResourceId());
            ILSMIndexAccessor indexAccessor =
                    index.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            if (logRecord.getNewOp() == AbstractIndexModificationOperationCallback.INSERT_BYTE) {
                indexAccessor.forceDelete(logRecord.getNewValue());
            } else if (logRecord.getNewOp() == AbstractIndexModificationOperationCallback.DELETE_BYTE) {
                indexAccessor.forceInsert(logRecord.getOldValue());
            } else if (logRecord.getNewOp() == AbstractIndexModificationOperationCallback.UPSERT_BYTE) {
                // undo, upsert the old value if found, otherwise, physical delete
                if (logRecord.getOldValue() == null) {
                    try {
                        indexAccessor.forcePhysicalDelete(logRecord.getNewValue());
                    } catch (HyracksDataException hde) {
                        // Since we're undoing according the write-ahead log, the actual upserting tuple
                        // might not have been written to memory yet.
                        if (hde.getErrorCode() != ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                            throw hde;
                        }
                    }
                } else {
                    indexAccessor.forceUpsert(logRecord.getOldValue());
                }
            } else {
                throw new IllegalStateException("Unsupported OperationType: " + logRecord.getNewOp());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to undo", e);
        }
    }

    private static void redo(ILogRecord logRecord, IDatasetLifecycleManager datasetLifecycleManager) {
        try {
            int datasetId = logRecord.getDatasetId();
            long resourceId = logRecord.getResourceId();
            ILSMIndex index = (ILSMIndex) datasetLifecycleManager.getIndex(datasetId, resourceId);
            ILSMIndexAccessor indexAccessor =
                    index.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            if (logRecord.getNewOp() == AbstractIndexModificationOperationCallback.INSERT_BYTE) {
                indexAccessor.forceInsert(logRecord.getNewValue());
            } else if (logRecord.getNewOp() == AbstractIndexModificationOperationCallback.DELETE_BYTE) {
                indexAccessor.forceDelete(logRecord.getNewValue());
            } else if (logRecord.getNewOp() == AbstractIndexModificationOperationCallback.UPSERT_BYTE) {
                // redo, upsert the new value
                indexAccessor.forceUpsert(logRecord.getNewValue());
            } else {
                throw new IllegalStateException("Unsupported OperationType: " + logRecord.getNewOp());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to redo", e);
        }
    }

    private class JobEntityCommits {
        private static final String PARTITION_FILE_NAME_SEPARATOR = "_";
        private final int jobId;
        private final Set<TxnId> cachedEntityCommitTxns = new HashSet<>();
        private final List<File> jobEntitCommitOnDiskPartitionsFiles = new ArrayList<>();
        //a flag indicating whether all the the commits for this jobs have been added.
        private boolean preparedForSearch = false;
        private TxnId winnerEntity = null;
        private int currentPartitionSize = 0;
        private long partitionMaxLSN = 0;
        private String currentPartitonName;

        public JobEntityCommits(int jobId) {
            this.jobId = jobId;
        }

        public void add(ILogRecord logRecord) throws IOException {
            if (preparedForSearch) {
                throw new IOException("Cannot add new entity commits after preparing for search.");
            }
            winnerEntity = new TxnId(logRecord.getJobId(), logRecord.getDatasetId(), logRecord.getPKHashValue(),
                    logRecord.getPKValue(), logRecord.getPKValueSize(), true);
            cachedEntityCommitTxns.add(winnerEntity);
            //since log file is read sequentially, LSNs are always increasing
            partitionMaxLSN = logRecord.getLSN();
            currentPartitionSize += winnerEntity.getCurrentSize();
            //if the memory budget for the current partition exceeded the limit, spill it to disk and free memory
            if (currentPartitionSize >= cachedEntityCommitsPerJobSize) {
                spillToDiskAndfreeMemory();
            }
        }

        public void spillToDiskAndfreeMemory() throws IOException {
            if (cachedEntityCommitTxns.size() > 0) {
                if (!preparedForSearch) {
                    writeCurrentPartitionToDisk();
                }
                cachedEntityCommitTxns.clear();
                partitionMaxLSN = 0;
                currentPartitionSize = 0;
                currentPartitonName = "";
            }
        }

        /**
         * Call this method when no more entity commits will be added to this job.
         *
         * @throws IOException
         */
        public void prepareForSearch() throws IOException {
            //if we have anything left in memory, we need to spill them to disk before searching other partitions.
            //However, if we don't have anything on disk, we will search from memory only
            if (jobEntitCommitOnDiskPartitionsFiles.size() > 0) {
                spillToDiskAndfreeMemory();
            } else {
                //set the name of the current in memory partition to the current partition
                currentPartitonName = getPartitionName(partitionMaxLSN);
            }
            preparedForSearch = true;
        }

        public boolean containsEntityCommitForTxnId(long logLSN, TxnId txnId) throws IOException {
            //if we don't have any partitions on disk, search only from memory
            if (jobEntitCommitOnDiskPartitionsFiles.size() == 0) {
                return cachedEntityCommitTxns.contains(txnId);
            } else {
                //get candidate partitions from disk
                ArrayList<File> candidatePartitions = getCandidiatePartitions(logLSN);
                for (File partition : candidatePartitions) {
                    if (serachPartition(partition, txnId)) {
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * @param logLSN
         * @return partitions that have a max LSN > logLSN
         */
        public ArrayList<File> getCandidiatePartitions(long logLSN) {
            ArrayList<File> candidiatePartitions = new ArrayList<>();
            for (File partition : jobEntitCommitOnDiskPartitionsFiles) {
                String partitionName = partition.getName();
                // entity commit log must come after the update log,
                // therefore, consider only partitions with max LSN > logLSN
                if (getPartitionMaxLSNFromName(partitionName) > logLSN) {
                    candidiatePartitions.add(partition);
                }
            }

            return candidiatePartitions;
        }

        public void clear() {
            cachedEntityCommitTxns.clear();
            for (File partition : jobEntitCommitOnDiskPartitionsFiles) {
                partition.delete();
            }
            jobEntitCommitOnDiskPartitionsFiles.clear();
        }

        private boolean serachPartition(File partition, TxnId txnId) throws IOException {
            //load partition from disk if it is not  already in memory
            if (!partition.getName().equals(currentPartitonName)) {
                loadPartitionToMemory(partition, cachedEntityCommitTxns);
                currentPartitonName = partition.getName();
            }
            return cachedEntityCommitTxns.contains(txnId);
        }

        private String getPartitionName(long maxLSN) {
            return jobId + PARTITION_FILE_NAME_SEPARATOR + maxLSN;
        }

        private long getPartitionMaxLSNFromName(String partitionName) {
            return Long.valueOf(partitionName.substring(partitionName.indexOf(PARTITION_FILE_NAME_SEPARATOR) + 1));
        }

        private void writeCurrentPartitionToDisk() throws IOException {
            //if we don't have enough memory to allocate for this partition,
            // we will ask recovery manager to free memory
            if (needToFreeMemory()) {
                freeJobsCachedEntities(jobId);
            }
            //allocate a buffer that can hold the current partition
            ByteBuffer buffer = ByteBuffer.allocate(currentPartitionSize);
            for (Iterator<TxnId> iterator = cachedEntityCommitTxns.iterator(); iterator.hasNext();) {
                TxnId txnId = iterator.next();
                //serialize the object and remove it from memory
                txnId.serialize(buffer);
                iterator.remove();
            }
            //name partition file based on job id and max lsn
            File partitionFile = createJobRecoveryFile(jobId, getPartitionName(partitionMaxLSN));
            //write file to disk
            try (FileOutputStream fileOutputstream = new FileOutputStream(partitionFile, false);
                    FileChannel fileChannel = fileOutputstream.getChannel()) {
                buffer.flip();
                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }
            }
            jobEntitCommitOnDiskPartitionsFiles.add(partitionFile);
        }

        private void loadPartitionToMemory(File partition, Set<TxnId> partitionTxn) throws IOException {
            partitionTxn.clear();
            //if we don't have enough memory to a load partition, we will ask recovery manager to free memory
            if (needToFreeMemory()) {
                freeJobsCachedEntities(jobId);
            }
            ByteBuffer buffer = ByteBuffer.allocateDirect((int) partition.length());
            //load partition to memory
            try (InputStream is = new FileInputStream(partition)) {
                int readByte;
                while ((readByte = is.read()) != -1) {
                    buffer.put((byte) readByte);
                }
            }
            buffer.flip();
            TxnId temp = null;
            while (buffer.remaining() != 0) {
                temp = TxnId.deserialize(buffer);
                partitionTxn.add(temp);
            }
        }
    }
}
