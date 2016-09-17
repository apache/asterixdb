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
package org.apache.asterix.transaction.management.service.recovery;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.ILocalResourceMetadata;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.AsterixMetadataProperties;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.ILogReader;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.service.logging.LogManager;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import org.apache.asterix.transaction.management.service.transaction.TransactionManager;
import org.apache.asterix.transaction.management.service.transaction.TransactionSubsystem;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.common.file.LocalResource;

/**
 * This is the Recovery Manager and is responsible for rolling back a
 * transaction as well as doing a system recovery.
 */
public class RecoveryManager implements IRecoveryManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;
    private static final Logger LOGGER = Logger.getLogger(RecoveryManager.class.getName());
    private final TransactionSubsystem txnSubsystem;
    private final LogManager logMgr;
    private final int checkpointHistory;
    private final long SHARP_CHECKPOINT_LSN = -1;
    private final boolean replicationEnabled;
    public static final long NON_SHARP_CHECKPOINT_TARGET_LSN = -1;
    private static final String RECOVERY_FILES_DIR_NAME = "recovery_temp";
    private Map<Integer, JobEntityCommits> jobId2WinnerEntitiesMap = null;
    private final long cachedEntityCommitsPerJobSize;
    private final PersistentLocalResourceRepository localResourceRepository;

    /**
     * A file at a known location that contains the LSN of the last log record
     * traversed doing a successful checkpoint.
     */
    private static final String CHECKPOINT_FILENAME_PREFIX = "checkpoint_";
    private SystemState state;

    public RecoveryManager(TransactionSubsystem txnSubsystem) {
        this.txnSubsystem = txnSubsystem;
        logMgr = (LogManager) txnSubsystem.getLogManager();
        checkpointHistory = txnSubsystem.getTransactionProperties().getCheckpointHistory();
        replicationEnabled = ClusterProperties.INSTANCE.isReplicationEnabled();
        localResourceRepository = (PersistentLocalResourceRepository) txnSubsystem.getAsterixAppRuntimeContextProvider()
                .getLocalResourceRepository();
        cachedEntityCommitsPerJobSize = txnSubsystem.getTransactionProperties().getJobRecoveryMemorySize();
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
        CheckpointObject checkpointObject = null;
        try {
            checkpointObject = readCheckpoint();
        } catch (FileNotFoundException e) {
            //The checkpoint file doesn't exist => Failure happened during NC initialization.
            //Retry to initialize the NC by setting the state to NEW_UNIVERSE
            state = SystemState.NEW_UNIVERSE;
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("The checkpoint file doesn't exist: systemState = NEW_UNIVERSE");
            }
            return state;
        }

        if (replicationEnabled) {
            if (checkpointObject.getMinMCTFirstLsn() == SHARP_CHECKPOINT_LSN) {
                //no logs exist
                state = SystemState.HEALTHY;
                return state;
            } else if (checkpointObject.getCheckpointLsn() == logMgr.getAppendLSN() && checkpointObject.isSharp()) {
                //only remote logs exist
                state = SystemState.HEALTHY;
                return state;
            } else {
                //need to perform remote recovery
                state = SystemState.CORRUPTED;
                return state;
            }
        } else {
            long readableSmallestLSN = logMgr.getReadableSmallestLSN();
            if (logMgr.getAppendLSN() == readableSmallestLSN) {
                if (checkpointObject.getMinMCTFirstLsn() != SHARP_CHECKPOINT_LSN) {
                    LOGGER.warning("Some(or all) of transaction log files are lost.");
                    //No choice but continuing when the log files are lost.
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
    }

    //This method is used only when replication is disabled.
    @Override
    public void startRecovery(boolean synchronous) throws IOException, ACIDException {
        state = SystemState.RECOVERING;
        LOGGER.log(Level.INFO, "starting recovery ...");

        long readableSmallestLSN = logMgr.getReadableSmallestLSN();
        CheckpointObject checkpointObject = readCheckpoint();
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
        int jobId = -1;

        Set<Integer> winnerJobSet = new HashSet<Integer>();
        jobId2WinnerEntitiesMap = new HashMap<>();

        //set log reader to the lowWaterMarkLsn
        ILogRecord logRecord = null;
        logReader.initializeScan(lowWaterMarkLSN);

        //collect all committed Lsn
        JobEntityCommits jobEntityWinners = null;

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
                    jobId = logRecord.getJobId();
                    winnerJobSet.add(jobId);
                    if (jobId2WinnerEntitiesMap.containsKey(jobId)) {
                        jobEntityWinners = jobId2WinnerEntitiesMap.get(jobId);
                        //to delete any spilled files as well
                        jobEntityWinners.clear();
                        jobId2WinnerEntitiesMap.remove(jobId);
                    }
                    jobCommitLogCount++;
                    break;
                case LogType.ENTITY_COMMIT:
                case LogType.UPSERT_ENTITY_COMMIT:
                    if (partitions.contains(logRecord.getResourcePartition())) {
                        jobId = logRecord.getJobId();
                        if (!jobId2WinnerEntitiesMap.containsKey(jobId)) {
                            jobEntityWinners = new JobEntityCommits(jobId);
                            if (needToFreeMemory()) {
                                //if we don't have enough memory for one more job, we will force all jobs to spill their cached entities to disk.
                                //This could happen only when we have many jobs with small number of records and none of them have job commit.
                                freeJobsCachedEntities(jobId);
                            }
                            jobId2WinnerEntitiesMap.put(jobId, jobEntityWinners);
                        } else {
                            jobEntityWinners = jobId2WinnerEntitiesMap.get(jobId);
                        }
                        jobEntityWinners.add(logRecord);
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

    private synchronized void startRecoveryRedoPhase(Set<Integer> partitions, ILogReader logReader,
            long lowWaterMarkLSN, Set<Integer> winnerJobSet) throws IOException, ACIDException {
        int redoCount = 0;
        int jobId = -1;

        long resourceId;
        long maxDiskLastLsn;
        long LSN = -1;
        ILSMIndex index = null;
        LocalResource localResource = null;
        ILocalResourceMetadata localResourceMetadata = null;
        boolean foundWinner = false;
        JobEntityCommits jobEntityWinners = null;

        IAsterixAppRuntimeContextProvider appRuntimeContext = txnSubsystem.getAsterixAppRuntimeContextProvider();
        IDatasetLifecycleManager datasetLifecycleManager = appRuntimeContext.getDatasetLifecycleManager();

        Map<Long, LocalResource> resourcesMap = localResourceRepository.loadAndGetAllResources();
        Map<Long, Long> resourceId2MaxLSNMap = new HashMap<Long, Long>();
        TxnId tempKeyTxnId = new TxnId(-1, -1, -1, null, -1, false);

        ILogRecord logRecord = null;
        try {
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
                        if (partitions.contains(logRecord.getResourcePartition())) {
                            if (winnerJobSet.contains(jobId)) {
                                foundWinner = true;
                            } else if (jobId2WinnerEntitiesMap.containsKey(jobId)) {
                                jobEntityWinners = jobId2WinnerEntitiesMap.get(jobId);
                                tempKeyTxnId.setTxnId(jobId, logRecord.getDatasetId(), logRecord.getPKHashValue(),
                                        logRecord.getPKValue(), logRecord.getPKValueSize());
                                if (jobEntityWinners.containsEntityCommitForTxnId(LSN, tempKeyTxnId)) {
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
                                    logRecord = logReader.next();
                                    continue;
                                }
                                /*******************************************************************/

                                //get index instance from IndexLifeCycleManager
                                //if index is not registered into IndexLifeCycleManager,
                                //create the index using LocalMetadata stored in LocalResourceRepository
                                //get partition path in this node
                                String partitionIODevicePath =
                                        localResourceRepository.getPartitionPath(localResource.getPartition());
                                String resourceAbsolutePath =
                                        partitionIODevicePath + File.separator + localResource.getResourceName();
                                localResource.setResourcePath(resourceAbsolutePath);
                                index = (ILSMIndex) datasetLifecycleManager.get(resourceAbsolutePath);
                                if (index == null) {
                                    //#. create index instance and register to indexLifeCycleManager
                                    localResourceMetadata = (ILocalResourceMetadata) localResource.getResourceObject();
                                    index = localResourceMetadata.createIndexInstance(appRuntimeContext,
                                            resourceAbsolutePath, localResource.getPartition(),
                                            localResourceRepository.getIODeviceNum(localResource.getPartition()));
                                    datasetLifecycleManager.register(resourceAbsolutePath, index);
                                    datasetLifecycleManager.open(resourceAbsolutePath);

                                    //#. get maxDiskLastLSN
                                    ILSMIndex lsmIndex = index;
                                    try {
                                        maxDiskLastLsn =
                                                ((AbstractLSMIOOperationCallback) lsmIndex.getIOOperationCallback())
                                                        .getComponentLSN(lsmIndex.getImmutableComponents());
                                    } catch (HyracksDataException e) {
                                        datasetLifecycleManager.close(resourceAbsolutePath);
                                        throw e;
                                    }

                                    //#. set resourceId and maxDiskLastLSN to the map
                                    resourceId2MaxLSNMap.put(resourceId, maxDiskLastLsn);
                                } else {
                                    maxDiskLastLsn = resourceId2MaxLSNMap.get(resourceId);
                                }

                                if (LSN > maxDiskLastLsn) {
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
                    case LogType.UPSERT_ENTITY_COMMIT:
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
                datasetLifecycleManager.close(resourcesMap.get(r).getResourcePath());
            }
        }
    }

    private boolean needToFreeMemory() {
        return Runtime.getRuntime().freeMemory() < cachedEntityCommitsPerJobSize;
    }

    @Override
    public synchronized long checkpoint(boolean isSharpCheckpoint, long nonSharpCheckpointTargetLSN)
            throws ACIDException, HyracksDataException {
        long minMCTFirstLSN;
        boolean nonSharpCheckpointSucceeded = false;

        if (isSharpCheckpoint) {
            LOGGER.log(Level.INFO, "Starting sharp checkpoint ... ");
        }

        TransactionManager txnMgr = (TransactionManager) txnSubsystem.getTransactionManager();
        String logDir = logMgr.getLogManagerProperties().getLogDir();

        //get the filename of the previous checkpoint files which are about to be deleted
        //right after the new checkpoint file is written.
        File[] prevCheckpointFiles = getPreviousCheckpointFiles();

        IDatasetLifecycleManager datasetLifecycleManager =
                txnSubsystem.getAsterixAppRuntimeContextProvider().getDatasetLifecycleManager();
        //flush all in-memory components if it is the sharp checkpoint
        if (isSharpCheckpoint) {
            datasetLifecycleManager.flushAllDatasets();
            if (!replicationEnabled) {
                minMCTFirstLSN = SHARP_CHECKPOINT_LSN;
            } else {
                //if is shutting down, need to check if we need to keep any remote logs for dead replicas
                if (txnSubsystem.getAsterixAppRuntimeContextProvider().getAppContext().isShuttingdown()) {
                    Set<String> deadReplicaIds = txnSubsystem.getAsterixAppRuntimeContextProvider().getAppContext()
                            .getReplicationManager().getDeadReplicasIds();
                    if (deadReplicaIds.isEmpty()) {
                        minMCTFirstLSN = SHARP_CHECKPOINT_LSN;
                    } else {
                        //get min LSN of dead replicas remote resources
                        IReplicaResourcesManager remoteResourcesManager = txnSubsystem
                                .getAsterixAppRuntimeContextProvider().getAppContext().getReplicaResourcesManager();
                        IAsterixPropertiesProvider propertiesProvider = (IAsterixPropertiesProvider) txnSubsystem
                                .getAsterixAppRuntimeContextProvider().getAppContext();
                        AsterixMetadataProperties metadataProperties = propertiesProvider.getMetadataProperties();
                        Set<Integer> deadReplicasPartitions = new HashSet<>();
                        //get partitions of the dead replicas that are not active on this node
                        for (String deadReplicaId : deadReplicaIds) {
                            ClusterPartition[] nodePartitons =
                                    metadataProperties.getNodePartitions().get(deadReplicaId);
                            for (ClusterPartition partition : nodePartitons) {
                                if (!localResourceRepository.getActivePartitions()
                                        .contains(partition.getPartitionId())) {
                                    deadReplicasPartitions.add(partition.getPartitionId());
                                }
                            }
                        }
                        minMCTFirstLSN = remoteResourcesManager.getPartitionsMinLSN(deadReplicasPartitions);
                    }
                } else {
                    //start up complete checkpoint. Avoid deleting remote recovery logs.
                    minMCTFirstLSN = getMinFirstLSN();
                }
            }
        } else {
            minMCTFirstLSN = getMinFirstLSN();
            if (minMCTFirstLSN >= nonSharpCheckpointTargetLSN) {
                nonSharpCheckpointSucceeded = true;
            } else {
                //flush datasets with indexes behind target checkpoint LSN
                datasetLifecycleManager.scheduleAsyncFlushForLaggingDatasets(nonSharpCheckpointTargetLSN);
                if (replicationEnabled) {
                    //request remote replicas to flush lagging indexes
                    IReplicationManager replicationManager =
                            txnSubsystem.getAsterixAppRuntimeContextProvider().getAppContext().getReplicationManager();
                    try {
                        replicationManager.requestFlushLaggingReplicaIndexes(nonSharpCheckpointTargetLSN);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        CheckpointObject checkpointObject = new CheckpointObject(logMgr.getAppendLSN(), minMCTFirstLSN,
                txnMgr.getMaxJobId(), System.currentTimeMillis(), isSharpCheckpoint);

        String fileName = getCheckpointFileName(logDir, Long.toString(checkpointObject.getTimeStamp()));

        try (FileOutputStream fos = new FileOutputStream(fileName);
                ObjectOutputStream oosToFos = new ObjectOutputStream(fos)) {
            oosToFos.writeObject(checkpointObject);
            oosToFos.flush();
        } catch (IOException e) {
            throw new ACIDException("Failed to checkpoint", e);
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
            try {
                if (minMCTFirstLSN == SHARP_CHECKPOINT_LSN) {
                    logMgr.renewLogFiles();
                } else {
                    logMgr.deleteOldLogFiles(minMCTFirstLSN);
                }
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        if (nonSharpCheckpointSucceeded) {
            logMgr.deleteOldLogFiles(minMCTFirstLSN);
        }

        if (isSharpCheckpoint) {
            LOGGER.info("Completed sharp checkpoint.");
        }

        //return the min LSN that was recorded in the checkpoint
        return minMCTFirstLSN;
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
        if (openIndexList.size() > 0) {
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
        long minRemoteLSN = remoteResourcesManager.getPartitionsMinLSN(localResourceRepository.getInactivePartitions());
        return minRemoteLSN;
    }

    private CheckpointObject readCheckpoint() throws ACIDException, FileNotFoundException {
        CheckpointObject checkpointObject = null;

        //read all checkpointObjects from the existing checkpoint files
        File[] prevCheckpointFiles = getPreviousCheckpointFiles();
        if (prevCheckpointFiles == null || prevCheckpointFiles.length == 0) {
            throw new FileNotFoundException("Checkpoint file is not found");
        }

        List<CheckpointObject> checkpointObjectList = new ArrayList<CheckpointObject>();
        for (File file : prevCheckpointFiles) {
            try (FileInputStream fis = new FileInputStream(file);
                    ObjectInputStream oisFromFis = new ObjectInputStream(fis)) {
                checkpointObject = (CheckpointObject) oisFromFis.readObject();
                checkpointObjectList.add(checkpointObject);
            } catch (Exception e) {
                throw new ACIDException("Failed to read a checkpoint file", e);
            }
        }

        //sort checkpointObjects in descending order by timeStamp to find out the most recent one.
        Collections.sort(checkpointObjectList);

        //return the most recent one (the first one in sorted list)
        return checkpointObjectList.get(0);
    }

    private File[] getPreviousCheckpointFiles() {
        String logDir = ((LogManager) txnSubsystem.getLogManager()).getLogManagerProperties().getLogDir();
        File parentDir = new File(logDir);

        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(CHECKPOINT_FILENAME_PREFIX);
            }
        };

        return parentDir.listFiles(filter);
    }

    private static String getCheckpointFileName(String baseDir, String suffix) {
        if (!baseDir.endsWith(System.getProperty("file.separator"))) {
            baseDir += System.getProperty("file.separator");
        }
        return baseDir + CHECKPOINT_FILENAME_PREFIX + suffix;
    }

    @Override
    public File createJobRecoveryFile(int jobId, String fileName) throws IOException {
        String recoveryDirPath = getRecoveryDirPath();
        Path JobRecoveryFolder = Paths.get(recoveryDirPath + File.separator + jobId);
        if (!Files.exists(JobRecoveryFolder)) {
            Files.createDirectories(JobRecoveryFolder);
        }

        File jobRecoveryFile = new File(JobRecoveryFolder.toString() + File.separator + fileName);
        if (!jobRecoveryFile.exists()) {
            jobRecoveryFile.createNewFile();
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
                LOGGER.info(
                        "no need to roll back as there were no operations by the transaction " + txnContext.getJobId());
            }
            return;
        }

        // While reading log records from firstLsn to lastLsn, collect uncommitted txn's Lsns
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("collecting loser transaction's LSNs from " + firstLSN + " to " + lastLSN);
        }

        Map<TxnId, List<Long>> jobLoserEntity2LSNsMap = new HashMap<TxnId, List<Long>>();
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
                                undoLSNSet = new LinkedList<Long>();
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
                    case LogType.UPSERT_ENTITY_COMMIT:
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
        try {
            checkpoint(true, NON_SHARP_CHECKPOINT_TARGET_LSN);
        } catch (HyracksDataException | ACIDException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
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
            if (logRecord.getNewOp() == IndexOperation.INSERT.ordinal()) {
                indexAccessor.forceDelete(logRecord.getNewValue());
            } else if (logRecord.getNewOp() == IndexOperation.DELETE.ordinal()) {
                indexAccessor.forceInsert(logRecord.getNewValue());
            } else {
                throw new IllegalStateException("Unsupported OperationType: " + logRecord.getNewOp());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to undo", e);
        }
    }

    private static void redo(ILogRecord logRecord, IDatasetLifecycleManager datasetLifecycleManager) {
        try {
            ILSMIndex index =
                    (ILSMIndex) datasetLifecycleManager.getIndex(logRecord.getDatasetId(), logRecord.getResourceId());
            ILSMIndexAccessor indexAccessor =
                    index.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            if (logRecord.getNewOp() == IndexOperation.INSERT.ordinal()) {
                indexAccessor.forceInsert(logRecord.getNewValue());
            } else if (logRecord.getNewOp() == IndexOperation.DELETE.ordinal()) {
                indexAccessor.forceDelete(logRecord.getNewValue());
            } else {
                throw new IllegalStateException("Unsupported OperationType: " + logRecord.getNewOp());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Failed to redo", e);
        }
    }

    private class JobEntityCommits {
        private static final String PARTITION_FILE_NAME_SEPARATOR = "_";
        private final int jobId;
        private final Set<TxnId> cachedEntityCommitTxns = new HashSet<TxnId>();
        private final List<File> jobEntitCommitOnDiskPartitionsFiles = new ArrayList<File>();
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
            ArrayList<File> candidiatePartitions = new ArrayList<File>();
            for (File partition : jobEntitCommitOnDiskPartitionsFiles) {
                String partitionName = partition.getName();
                //entity commit log must come after the update log, therefore, consider only partitions with max LSN > logLSN
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
            //if we don't have enough memory to allocate for this partition, we will ask recovery manager to free memory
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
