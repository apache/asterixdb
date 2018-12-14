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
import java.util.stream.Collectors;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.common.transactions.ICheckpointManager;
import org.apache.asterix.common.transactions.ILogReader;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.service.logging.LogManager;
import org.apache.asterix.transaction.management.service.recovery.AbstractCheckpointManager;
import org.apache.asterix.transaction.management.service.recovery.TxnEntityId;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId.IdCompareResult;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

/**
 * This is the Recovery Manager and is responsible for rolling back a
 * transaction as well as doing a system recovery.
 */
public class RecoveryManager implements IRecoveryManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;
    private static final long SMALLEST_POSSIBLE_LSN = 0;
    private static final Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger();
    private final ITransactionSubsystem txnSubsystem;
    private final LogManager logMgr;
    private final boolean replicationEnabled;
    private static final String RECOVERY_FILES_DIR_NAME = "recovery_temp";
    private Map<Long, JobEntityCommits> jobId2WinnerEntitiesMap = null;
    private final long cachedEntityCommitsPerJobSize;
    protected final PersistentLocalResourceRepository localResourceRepository;
    private final ICheckpointManager checkpointManager;
    private SystemState state;
    protected final INCServiceContext serviceCtx;
    protected final INcApplicationContext appCtx;
    private static final TxnId recoveryTxnId = new TxnId(-1);

    public RecoveryManager(INCServiceContext serviceCtx, ITransactionSubsystem txnSubsystem) {
        this.serviceCtx = serviceCtx;
        this.txnSubsystem = txnSubsystem;
        this.appCtx = txnSubsystem.getApplicationContext();
        logMgr = (LogManager) txnSubsystem.getLogManager();
        ReplicationProperties repProperties = appCtx.getReplicationProperties();
        replicationEnabled = repProperties.isReplicationEnabled();
        localResourceRepository = (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
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
            LOGGER.info("The checkpoint file doesn't exist: systemState = PERMANENT_DATA_LOSS");
            return state;
        }
        long readableSmallestLSN = logMgr.getReadableSmallestLSN();
        if (logMgr.getAppendLSN() == readableSmallestLSN) {
            if (checkpointObject.getMinMCTFirstLsn() != AbstractCheckpointManager.SHARP_CHECKPOINT_LSN) {
                LOGGER.warn("Some(or all) of transaction log files are lost.");
                //No choice but continuing when the log files are lost.
            }
            state = SystemState.HEALTHY;
        } else if (checkpointObject.getCheckpointLsn() == logMgr.getAppendLSN()
                && checkpointObject.getMinMCTFirstLsn() == AbstractCheckpointManager.SHARP_CHECKPOINT_LSN) {
            state = SystemState.HEALTHY;
        } else {
            state = SystemState.CORRUPTED;
        }
        return state;
    }

    @Override
    public void startLocalRecovery(Set<Integer> partitions) throws IOException, ACIDException {
        state = SystemState.RECOVERING;
        LOGGER.info("starting recovery ...");

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
            Set<Long> winnerJobSet = startRecoverysAnalysisPhase(partitions, logReader, lowWaterMarkLSN);
            startRecoveryRedoPhase(partitions, logReader, lowWaterMarkLSN, winnerJobSet);
        } finally {
            logReader.close();
            deleteRecoveryTemporaryFiles();
        }
    }

    private synchronized Set<Long> startRecoverysAnalysisPhase(Set<Integer> partitions, ILogReader logReader,
            long lowWaterMarkLSN) throws IOException, ACIDException {
        int updateLogCount = 0;
        int entityCommitLogCount = 0;
        int jobCommitLogCount = 0;
        int abortLogCount = 0;
        Set<Long> winnerJobSet = new HashSet<>();
        jobId2WinnerEntitiesMap = new HashMap<>();
        //set log reader to the lowWaterMarkLsn
        ILogRecord logRecord;
        logReader.setPosition(lowWaterMarkLSN);
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
                    winnerJobSet.add(logRecord.getTxnId());
                    cleanupTxnCommits(logRecord.getTxnId());
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
                case LogType.WAIT_FOR_FLUSHES:
                case LogType.MARKER:
                case LogType.FILTER:
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

    private void cleanupTxnCommits(long txnId) {
        if (jobId2WinnerEntitiesMap.containsKey(txnId)) {
            JobEntityCommits jobEntityWinners = jobId2WinnerEntitiesMap.get(txnId);
            //to delete any spilled files as well
            jobEntityWinners.clear();
            jobId2WinnerEntitiesMap.remove(txnId);
        }
    }

    private void analyzeEntityCommitLog(ILogRecord logRecord) throws IOException {
        long txnId = logRecord.getTxnId();
        JobEntityCommits jobEntityWinners;
        if (!jobId2WinnerEntitiesMap.containsKey(txnId)) {
            jobEntityWinners = new JobEntityCommits(txnId);
            if (needToFreeMemory()) {
                // If we don't have enough memory for one more job,
                // we will force all jobs to spill their cached entities to disk.
                // This could happen only when we have many jobs with small
                // number of records and none of them have job commit.
                freeJobsCachedEntities(txnId);
            }
            jobId2WinnerEntitiesMap.put(txnId, jobEntityWinners);
        } else {
            jobEntityWinners = jobId2WinnerEntitiesMap.get(txnId);
        }
        jobEntityWinners.add(logRecord);
    }

    private synchronized void startRecoveryRedoPhase(Set<Integer> partitions, ILogReader logReader,
            long lowWaterMarkLSN, Set<Long> winnerTxnSet) throws IOException, ACIDException {
        int redoCount = 0;
        long txnId = 0;

        long resourceId;
        long maxDiskLastLsn;
        long lsn = -1;
        ILSMIndex index = null;
        LocalResource localResource = null;
        DatasetLocalResource localResourceMetadata = null;
        boolean foundWinner = false;
        JobEntityCommits jobEntityWinners = null;

        IDatasetLifecycleManager datasetLifecycleManager = appCtx.getDatasetLifecycleManager();
        final IIndexCheckpointManagerProvider indexCheckpointManagerProvider =
                ((INcApplicationContext) (serviceCtx.getApplicationContext())).getIndexCheckpointManagerProvider();

        Map<Long, LocalResource> resourcesMap = localResourceRepository.loadAndGetAllResources();
        final Map<Long, Long> resourceId2MaxLSNMap = new HashMap<>();
        TxnEntityId tempKeyTxnEntityId = new TxnEntityId(-1, -1, -1, null, -1, false);

        ILogRecord logRecord = null;
        try {
            logReader.setPosition(lowWaterMarkLSN);
            logRecord = logReader.next();
            while (logRecord != null) {
                if (IS_DEBUG_MODE) {
                    LOGGER.info(logRecord.getLogRecordForDisplay());
                }
                lsn = logRecord.getLSN();
                txnId = logRecord.getTxnId();
                foundWinner = false;
                switch (logRecord.getLogType()) {
                    case LogType.UPDATE:
                        if (partitions.contains(logRecord.getResourcePartition())) {
                            if (winnerTxnSet.contains(txnId)) {
                                foundWinner = true;
                            } else if (jobId2WinnerEntitiesMap.containsKey(txnId)) {
                                jobEntityWinners = jobId2WinnerEntitiesMap.get(txnId);
                                tempKeyTxnEntityId.setTxnId(txnId, logRecord.getDatasetId(), logRecord.getPKHashValue(),
                                        logRecord.getPKValue(), logRecord.getPKValueSize());
                                if (jobEntityWinners.containsEntityCommitForTxnId(lsn, tempKeyTxnEntityId)) {
                                    foundWinner = true;
                                }
                            }
                            if (!foundWinner) {
                                break;
                            }
                        }
                        //fall through as FILTER is a subset of UPDATE
                    case LogType.FILTER:
                        if (partitions.contains(logRecord.getResourcePartition())) {
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
                                LOGGER.log(Level.WARN, "resource was not found for resource id " + resourceId);
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
                                try {
                                    final DatasetResourceReference resourceReference =
                                            DatasetResourceReference.of(localResource);
                                    maxDiskLastLsn =
                                            indexCheckpointManagerProvider.get(resourceReference).getLowWatermark();
                                } catch (HyracksDataException e) {
                                    datasetLifecycleManager.close(localResource.getPath());
                                    throw e;
                                }
                                //#. set resourceId and maxDiskLastLSN to the map
                                resourceId2MaxLSNMap.put(resourceId, maxDiskLastLsn);
                            } else {
                                maxDiskLastLsn = resourceId2MaxLSNMap.get(resourceId);
                            }
                            // lsn @ maxDiskLastLsn is either a flush log or a master replica log
                            if (lsn >= maxDiskLastLsn) {
                                redo(logRecord, datasetLifecycleManager);
                                redoCount++;
                            }
                        }
                        break;
                    case LogType.FLUSH:
                        int partition = logRecord.getResourcePartition();
                        if (partitions.contains(partition)) {
                            int datasetId = logRecord.getDatasetId();
                            if (!datasetLifecycleManager.isRegistered(datasetId)) {
                                // it's possible this dataset has been dropped
                                logRecord = logReader.next();
                                continue;
                            }
                            DatasetInfo dsInfo = datasetLifecycleManager.getDatasetInfo(datasetId);
                            // we only need to flush open indexes here (opened by previous update records)
                            // if an index has no ongoing updates, then it's memory component must be empty
                            // and there is nothing to flush
                            for (final IndexInfo iInfo : dsInfo.getIndexes().values()) {
                                if (iInfo.isOpen() && iInfo.getPartition() == partition) {
                                    Long maxLsnBeforeFlush = resourceId2MaxLSNMap.get(iInfo.getResourceId());
                                    if (maxLsnBeforeFlush != null) {
                                        // If there was at least one update to the resource.
                                        // IMPORTANT: Don't remove the check above
                                        // This check is to support indexes without transaction logs
                                        maxDiskLastLsn = maxLsnBeforeFlush;
                                        index = iInfo.getIndex();
                                        if (logRecord.getLSN() > maxDiskLastLsn
                                                && !index.isCurrentMutableComponentEmpty()) {
                                            // schedule flush
                                            redoFlush(index, logRecord);
                                            redoCount++;
                                        } else {
                                            // TODO: update checkpoint file?
                                        }
                                    } else {
                                        // TODO: update checkpoint file?
                                    }
                                }
                            }
                        }
                        break;
                    case LogType.JOB_COMMIT:
                    case LogType.ENTITY_COMMIT:
                    case LogType.ABORT:
                    case LogType.WAIT:
                    case LogType.WAIT_FOR_FLUSHES:
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
            txnSubsystem.getTransactionManager().ensureMaxTxnId(txnId);
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
        final IDatasetLifecycleManager datasetLifecycleManager = appCtx.getDatasetLifecycleManager();
        List<IIndex> openIndexList = datasetLifecycleManager.getOpenResources();
        long firstLSN;
        //the min first lsn can only be the current append or smaller
        long minFirstLSN = logMgr.getAppendLSN();
        if (!openIndexList.isEmpty()) {
            for (IIndex index : openIndexList) {
                LSMIOOperationCallback ioCallback =
                        (LSMIOOperationCallback) ((ILSMIndex) index).getIOOperationCallback();
                if (!((AbstractLSMIndex) index).isCurrentMutableComponentEmpty() || ioCallback.hasPendingFlush()) {
                    firstLSN = ioCallback.getPersistenceLsn();
                    minFirstLSN = Math.min(minFirstLSN, firstLSN);
                }
            }
        }
        return minFirstLSN;
    }

    private long getRemoteMinFirstLSN() throws HyracksDataException {
        // find the min first lsn of partitions that are replicated on this node
        final Set<Integer> allPartitions = localResourceRepository.getAllPartitions();
        final Set<Integer> masterPartitions = appCtx.getReplicaManager().getPartitions();
        allPartitions.removeAll(masterPartitions);
        return getPartitionsMinLSN(allPartitions);
    }

    private long getPartitionsMinLSN(Set<Integer> partitions) throws HyracksDataException {
        final IIndexCheckpointManagerProvider idxCheckpointMgrProvider = appCtx.getIndexCheckpointManagerProvider();
        long minRemoteLSN = Long.MAX_VALUE;
        for (Integer partition : partitions) {
            final List<DatasetResourceReference> partitionResources = localResourceRepository.getResources(resource -> {
                DatasetLocalResource dsResource = (DatasetLocalResource) resource.getResource();
                return dsResource.getPartition() == partition;
            }).values().stream().map(DatasetResourceReference::of).collect(Collectors.toList());
            for (DatasetResourceReference indexRef : partitionResources) {
                try {
                    final IIndexCheckpointManager idxCheckpointMgr = idxCheckpointMgrProvider.get(indexRef);
                    if (idxCheckpointMgr.getCheckpointCount() > 0) {
                        long remoteIndexMaxLSN = idxCheckpointMgrProvider.get(indexRef).getLowWatermark();
                        minRemoteLSN = Math.min(minRemoteLSN, remoteIndexMaxLSN);
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to get min LSN of resource {}", indexRef, e);
                    // ensure no logs will be deleted in case of unexpected failures
                    return SMALLEST_POSSIBLE_LSN;
                }
            }
        }
        return minRemoteLSN;
    }

    @Override
    public synchronized void replayReplicaPartitionLogs(Set<Integer> partitions, boolean flush)
            throws HyracksDataException {
        //replay logs > minLSN that belong to these partitions
        try {
            checkpointManager.secure(recoveryTxnId);
            long minLSN = getPartitionsMinLSN(partitions);
            long readableSmallestLSN = logMgr.getReadableSmallestLSN();
            if (minLSN < readableSmallestLSN) {
                minLSN = readableSmallestLSN;
            }
            replayPartitionsLogs(partitions, logMgr.getLogReader(true), minLSN);
            if (flush) {
                appCtx.getDatasetLifecycleManager().flushAllDatasets();
            }
        } catch (IOException | ACIDException e) {
            throw HyracksDataException.create(e);
        } finally {
            checkpointManager.completed(recoveryTxnId);
        }
    }

    @Override
    public File createJobRecoveryFile(long txnId, String fileName) throws IOException {
        String recoveryDirPath = getRecoveryDirPath();
        Path jobRecoveryFolder = Paths.get(recoveryDirPath + File.separator + txnId);
        if (!Files.exists(jobRecoveryFolder)) {
            Files.createDirectories(jobRecoveryFolder);
        }

        File jobRecoveryFile = new File(jobRecoveryFolder.toString() + File.separator + fileName);
        if (!jobRecoveryFile.exists()) {
            if (!jobRecoveryFile.createNewFile()) {
                throw new IOException("Failed to create file: " + fileName + " for txn id(" + txnId + ")");
            }
        } else {
            throw new IOException("File: " + fileName + " for txn id(" + txnId + ") already exists");
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

    private void freeJobsCachedEntities(long requestingTxnId) throws IOException {
        if (jobId2WinnerEntitiesMap != null) {
            for (Entry<Long, JobEntityCommits> jobEntityCommits : jobId2WinnerEntitiesMap.entrySet()) {
                //if the job is not the requester, free its memory
                if (jobEntityCommits.getKey() != requestingTxnId) {
                    jobEntityCommits.getValue().spillToDiskAndfreeMemory();
                }
            }
        }
    }

    @Override
    public void rollbackTransaction(ITransactionContext txnContext) throws ACIDException {
        long abortedTxnId = txnContext.getTxnId().getId();
        // Obtain the first/last log record LSNs written by the Job
        long firstLSN = txnContext.getFirstLSN();
        /*
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
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("rollbacking transaction log records from " + firstLSN + " to " + lastLSN);
        }
        // check if the transaction actually wrote some logs.
        if (firstLSN == TransactionManagementConstants.LogManagerConstants.TERMINAL_LSN || firstLSN > lastLSN) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("no need to roll back as there were no operations by the txn " + txnContext.getTxnId());
            }
            return;
        }

        // While reading log records from firstLsn to lastLsn, collect uncommitted txn's Lsns
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("collecting loser transaction's LSNs from " + firstLSN + " to " + lastLSN);
        }

        Map<TxnEntityId, List<Long>> jobLoserEntity2LSNsMap = new HashMap<>();
        TxnEntityId tempKeyTxnEntityId = new TxnEntityId(-1, -1, -1, null, -1, false);
        int updateLogCount = 0;
        int entityCommitLogCount = 0;
        long logTxnId;
        long currentLSN = -1;
        TxnEntityId loserEntity;
        List<Long> undoLSNSet = null;
        //get active partitions on this node
        Set<Integer> activePartitions = appCtx.getReplicaManager().getPartitions();
        ILogReader logReader = logMgr.getLogReader(false);
        try {
            logReader.setPosition(firstLSN);
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
                logTxnId = logRecord.getTxnId();
                if (logTxnId != abortedTxnId) {
                    continue;
                }
                tempKeyTxnEntityId.setTxnId(logTxnId, logRecord.getDatasetId(), logRecord.getPKHashValue(),
                        logRecord.getPKValue(), logRecord.getPKValueSize());
                switch (logRecord.getLogType()) {
                    case LogType.UPDATE:
                        if (activePartitions.contains(logRecord.getResourcePartition())) {
                            undoLSNSet = jobLoserEntity2LSNsMap.get(tempKeyTxnEntityId);
                            if (undoLSNSet == null) {
                                loserEntity =
                                        new TxnEntityId(logTxnId, logRecord.getDatasetId(), logRecord.getPKHashValue(),
                                                logRecord.getPKValue(), logRecord.getPKValueSize(), true);
                                undoLSNSet = new LinkedList<>();
                                jobLoserEntity2LSNsMap.put(loserEntity, undoLSNSet);
                            }
                            undoLSNSet.add(currentLSN);
                            updateLogCount++;
                            if (IS_DEBUG_MODE) {
                                LOGGER.info(Thread.currentThread().getId() + "======> update[" + currentLSN + "]:"
                                        + tempKeyTxnEntityId);
                            }
                        }
                        break;
                    case LogType.ENTITY_COMMIT:
                        if (activePartitions.contains(logRecord.getResourcePartition())) {
                            jobLoserEntity2LSNsMap.remove(tempKeyTxnEntityId);
                            entityCommitLogCount++;
                            if (IS_DEBUG_MODE) {
                                LOGGER.info(Thread.currentThread().getId() + "======> entity_commit[" + currentLSN + "]"
                                        + tempKeyTxnEntityId);
                            }
                        }
                        break;
                    case LogType.JOB_COMMIT:
                        throw new ACIDException("Unexpected LogType(" + logRecord.getLogType() + ") during abort.");
                    case LogType.ABORT:
                    case LogType.FLUSH:
                    case LogType.FILTER:
                    case LogType.WAIT:
                    case LogType.WAIT_FOR_FLUSHES:
                    case LogType.MARKER:
                        //ignore
                        break;
                    default:
                        throw new ACIDException("Unsupported LogType: " + logRecord.getLogType());
                }
            }

            if (currentLSN != lastLSN) {
                throw new ACIDException("LastLSN mismatch: lastLSN(" + lastLSN + ") vs currentLSN(" + currentLSN
                        + ") during abort( " + txnContext.getTxnId() + ")");
            }

            //undo loserTxn's effect
            LOGGER.log(Level.INFO, "undoing loser transaction's effect");

            final IDatasetLifecycleManager datasetLifecycleManager = appCtx.getDatasetLifecycleManager();
            //TODO sort loser entities by smallest LSN to undo in one pass.
            Iterator<Entry<TxnEntityId, List<Long>>> iter = jobLoserEntity2LSNsMap.entrySet().iterator();
            int undoCount = 0;
            while (iter.hasNext()) {
                Map.Entry<TxnEntityId, List<Long>> loserEntity2LSNsMap = iter.next();
                undoLSNSet = loserEntity2LSNsMap.getValue();
                // The step below is important since the upsert operations must be done in reverse order.
                Collections.reverse(undoLSNSet);
                for (long undoLSN : undoLSNSet) {
                    //here, all the log records are UPDATE type. So, we don't need to check the type again.
                    //read the corresponding log record to be undone.
                    logRecord = logReader.read(undoLSN);
                    if (logRecord == null) {
                        throw new ACIDException("IllegalState exception during abort( " + txnContext.getTxnId() + ")");
                    }
                    if (IS_DEBUG_MODE) {
                        LOGGER.info(logRecord.getLogRecordForDisplay());
                    }
                    undo(logRecord, datasetLifecycleManager);
                    undoCount++;
                }
            }

            if (LOGGER.isInfoEnabled()) {
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
    }

    @Override
    public void dumpState(OutputStream os) throws IOException {
        // do nothing
    }

    private static void undo(ILogRecord logRecord, IDatasetLifecycleManager datasetLifecycleManager) {
        try {
            ILSMIndex index =
                    (ILSMIndex) datasetLifecycleManager.getIndex(logRecord.getDatasetId(), logRecord.getResourceId());
            ILSMIndexAccessor indexAccessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            ILSMIndexOperationContext opCtx = indexAccessor.getOpContext();
            opCtx.setFilterSkip(true);
            try {
                switch (logRecord.getNewOp()) {
                    case AbstractIndexModificationOperationCallback.INSERT_BYTE:
                        indexAccessor.forceDelete(logRecord.getNewValue());
                        break;
                    case AbstractIndexModificationOperationCallback.DELETE_BYTE:
                        // use the same logic to undo delete as undo upsert, since
                        // the old value could be null as well if the deleted record is from disk component
                    case AbstractIndexModificationOperationCallback.UPSERT_BYTE:
                        // undo, upsert the old value if found, otherwise, physical delete
                        undoUpsertOrDelete(indexAccessor, logRecord);
                        break;
                    case AbstractIndexModificationOperationCallback.FILTER_BYTE:
                        //do nothing, can't undo filters
                        break;
                    default:
                        throw new IllegalStateException("Unsupported OperationType: " + logRecord.getNewOp());
                }
            } finally {
                indexAccessor.destroy();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to undo", e);
        }
    }

    private static void undoUpsertOrDelete(ILSMIndexAccessor indexAccessor, ILogRecord logRecord)
            throws HyracksDataException {
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
    }

    private static void redo(ILogRecord logRecord, IDatasetLifecycleManager datasetLifecycleManager) {
        try {
            int datasetId = logRecord.getDatasetId();
            long resourceId = logRecord.getResourceId();
            ILSMIndex index = (ILSMIndex) datasetLifecycleManager.getIndex(datasetId, resourceId);
            ILSMIndexAccessor indexAccessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            ILSMIndexOperationContext opCtx = indexAccessor.getOpContext();
            opCtx.setFilterSkip(true);
            opCtx.setRecovery(true);
            if (logRecord.getNewOp() == AbstractIndexModificationOperationCallback.INSERT_BYTE) {
                indexAccessor.forceInsert(logRecord.getNewValue());
            } else if (logRecord.getNewOp() == AbstractIndexModificationOperationCallback.DELETE_BYTE) {
                indexAccessor.forceDelete(logRecord.getNewValue());
            } else if (logRecord.getNewOp() == AbstractIndexModificationOperationCallback.UPSERT_BYTE) {
                // redo, upsert the new value
                indexAccessor.forceUpsert(logRecord.getNewValue());
            } else if (logRecord.getNewOp() == AbstractIndexModificationOperationCallback.FILTER_BYTE) {
                opCtx.setFilterSkip(false);
                indexAccessor.updateFilter(logRecord.getNewValue());
            } else {
                throw new IllegalStateException("Unsupported OperationType: " + logRecord.getNewOp());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to redo", e);
        }
    }

    private static void redoFlush(ILSMIndex index, ILogRecord logRecord) throws HyracksDataException {
        long flushLsn = logRecord.getLSN();
        Map<String, Object> flushMap = new HashMap<>();
        flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
        ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        accessor.getOpContext().setParameters(flushMap);
        long minId = logRecord.getFlushingComponentMinId();
        long maxId = logRecord.getFlushingComponentMaxId();
        ILSMComponentId id = new LSMComponentId(minId, maxId);
        flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, index.getCurrentMemoryComponent().getId());
        if (!index.getDiskComponents().isEmpty()) {
            ILSMDiskComponent diskComponent = index.getDiskComponents().get(0);
            ILSMComponentId maxDiskComponentId = diskComponent.getId();
            if (maxDiskComponentId.compareTo(id) != IdCompareResult.LESS_THAN) {
                throw new IllegalStateException("Illegal state of component Id. Max disk component Id "
                        + maxDiskComponentId + " should be less than redo flush component Id " + id);
            }
        }
        index.getCurrentMemoryComponent().resetId(id, true);
        ILSMIOOperation flush = accessor.scheduleFlush();
        try {
            flush.sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        }
        if (flush.getStatus() == LSMIOOperationStatus.FAILURE) {
            throw HyracksDataException.create(flush.getFailure());
        }
        index.resetCurrentComponentIndex();
    }

    private class JobEntityCommits {
        private static final String PARTITION_FILE_NAME_SEPARATOR = "_";
        private final long txnId;
        private final Set<TxnEntityId> cachedEntityCommitTxns = new HashSet<>();
        private final List<File> jobEntitCommitOnDiskPartitionsFiles = new ArrayList<>();
        //a flag indicating whether all the the commits for this jobs have been added.
        private boolean preparedForSearch = false;
        private TxnEntityId winnerEntity = null;
        private int currentPartitionSize = 0;
        private long partitionMaxLSN = 0;
        private String currentPartitonName;

        public JobEntityCommits(long txnId) {
            this.txnId = txnId;
        }

        public void add(ILogRecord logRecord) throws IOException {
            if (preparedForSearch) {
                throw new IOException("Cannot add new entity commits after preparing for search.");
            }
            winnerEntity = new TxnEntityId(logRecord.getTxnId(), logRecord.getDatasetId(), logRecord.getPKHashValue(),
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

        public boolean containsEntityCommitForTxnId(long logLSN, TxnEntityId txnEntityId) throws IOException {
            //if we don't have any partitions on disk, search only from memory
            if (jobEntitCommitOnDiskPartitionsFiles.size() == 0) {
                return cachedEntityCommitTxns.contains(txnEntityId);
            } else {
                //get candidate partitions from disk
                ArrayList<File> candidatePartitions = getCandidiatePartitions(logLSN);
                for (File partition : candidatePartitions) {
                    if (serachPartition(partition, txnEntityId)) {
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

        private boolean serachPartition(File partition, TxnEntityId txnEntityId) throws IOException {
            //load partition from disk if it is not  already in memory
            if (!partition.getName().equals(currentPartitonName)) {
                loadPartitionToMemory(partition, cachedEntityCommitTxns);
                currentPartitonName = partition.getName();
            }
            return cachedEntityCommitTxns.contains(txnEntityId);
        }

        private String getPartitionName(long maxLSN) {
            return txnId + PARTITION_FILE_NAME_SEPARATOR + maxLSN;
        }

        private long getPartitionMaxLSNFromName(String partitionName) {
            return Long.valueOf(partitionName.substring(partitionName.indexOf(PARTITION_FILE_NAME_SEPARATOR) + 1));
        }

        private void writeCurrentPartitionToDisk() throws IOException {
            //if we don't have enough memory to allocate for this partition,
            // we will ask recovery manager to free memory
            if (needToFreeMemory()) {
                freeJobsCachedEntities(txnId);
            }
            //allocate a buffer that can hold the current partition
            ByteBuffer buffer = ByteBuffer.allocate(currentPartitionSize);
            for (Iterator<TxnEntityId> iterator = cachedEntityCommitTxns.iterator(); iterator.hasNext();) {
                TxnEntityId txnEntityId = iterator.next();
                //serialize the object and remove it from memory
                txnEntityId.serialize(buffer);
                iterator.remove();
            }
            //name partition file based on job id and max lsn
            File partitionFile = createJobRecoveryFile(txnId, getPartitionName(partitionMaxLSN));
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

        private void loadPartitionToMemory(File partition, Set<TxnEntityId> partitionTxn) throws IOException {
            partitionTxn.clear();
            //if we don't have enough memory to a load partition, we will ask recovery manager to free memory
            if (needToFreeMemory()) {
                freeJobsCachedEntities(txnId);
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
            TxnEntityId temp;
            while (buffer.remaining() != 0) {
                temp = TxnEntityId.deserialize(buffer);
                partitionTxn.add(temp);
            }
        }
    }
}
