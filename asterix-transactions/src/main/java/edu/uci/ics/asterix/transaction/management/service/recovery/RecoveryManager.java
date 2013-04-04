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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.opcallbacks.IndexOperationTracker;
import edu.uci.ics.asterix.transaction.management.resource.ILocalResourceMetadata;
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
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTreeImmutableComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallbackWrapper;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexImmutableComponent;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTreeImmutableComponent;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
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
    private final TransactionSubsystem txnSubsystem;

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
            state = SystemState.NEW_UNIVERSE;
            return state;
        }

        //#. if minMCTFirstLSN is equal to -1 && 
        //   checkpointLSN in the checkpoint file is equal to the lastLSN in the log file,
        //   then return healthy state. Otherwise, return corrupted.
        LogManager logMgr = (LogManager) txnSubsystem.getLogManager();
        if (checkpointObject.getMinMCTFirstLSN() == -1
                && checkpointObject.getCheckpointLSN() == logMgr.getCurrentLsn().get()) {
            state = SystemState.HEALTHY;
            return state;
        } else {
            state = SystemState.CORRUPTED;
            return state;
        }
    }

    public void startRecovery(boolean synchronous) throws IOException, ACIDException {

        int updateLogCount = 0;
        int commitLogCount = 0;
        int redoCount = 0;

        state = SystemState.RECOVERING;

        ILogManager logManager = txnSubsystem.getLogManager();
        ILogRecordHelper logRecordHelper = logManager.getLogRecordHelper();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("[RecoveryMgr] starting recovery ...");
        }

        //winnerTxnTable is used to add pairs, <committed TxnId, the most recent commit LSN of the TxnId>
        Map<TxnId, Long> winnerTxnTable = new HashMap<TxnId, Long>();
        TxnId tempKeyTxnId = new TxnId(-1, -1, -1);
        byte logType;

        //#. read checkpoint file and set lowWaterMark where anaylsis and redo start
        CheckpointObject checkpointObject = readCheckpoint();
        long lowWaterMarkLSN = checkpointObject.getMinMCTFirstLSN();
        if (lowWaterMarkLSN == -1) {
            lowWaterMarkLSN = 0;
        }
        int maxJobId = checkpointObject.getMaxJobId();
        int currentJobId;

        //-------------------------------------------------------------------------
        //  [ analysis phase ]
        //  - collect all committed LSN 
        //  - if there are duplicate commits for the same TxnId, 
        //    keep only the mostRecentCommitLSN among the duplicates.
        //-------------------------------------------------------------------------
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("[RecoveryMgr] in analysis phase");
        }

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

            TxnId commitTxnId = null;
            switch (logType) {
                case LogType.UPDATE:
                    if (IS_DEBUG_MODE) {
                        updateLogCount++;
                    }
                    break;

                case LogType.COMMIT:
                case LogType.ENTITY_COMMIT:
                    commitTxnId = new TxnId(logRecordHelper.getJobId(currentLogLocator),
                            logRecordHelper.getDatasetId(currentLogLocator),
                            logRecordHelper.getPKHashValue(currentLogLocator));
                    winnerTxnTable.put(commitTxnId, currentLogLocator.getLsn());
                    if (IS_DEBUG_MODE) {
                        commitLogCount++;
                    }
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
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("[RecoveryMgr] in redo phase");
        }
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
        long currentLSN = -1;
        int resourceType;
        ILSMIndex index = null;
        LocalResource localResource = null;
        ILocalResourceMetadata localResourceMetadata = null;
        Map<Long, Long> resourceId2MaxLSNMap = new HashMap<Long, Long>();
        List<ILSMComponent> immutableDiskIndexList = null;
        TxnId jobLevelTxnId = new TxnId(-1, -1, -1);
        boolean foundWinnerTxn;

        //#. get indexLifeCycleManager 
        IAsterixAppRuntimeContextProvider appRuntimeContext = txnSubsystem.getAsterixAppRuntimeContextProvider();
        IIndexLifecycleManager indexLifecycleManager = appRuntimeContext.getIndexLifecycleManager();
        ILocalResourceRepository localResourceRepository = appRuntimeContext.getLocalResourceRepository();

        //#. redo
        while (logCursor.next(currentLogLocator)) {
            foundWinnerTxn = false;
            if (LogManager.IS_DEBUG_MODE) {
                System.out.println(logManager.getLogRecordHelper().getLogRecordForDisplay(currentLogLocator));
            }

            logType = logRecordHelper.getLogType(currentLogLocator);

            switch (logType) {
                case LogType.UPDATE:
                    tempKeyTxnId.setTxnId(logRecordHelper.getJobId(currentLogLocator),
                            logRecordHelper.getDatasetId(currentLogLocator),
                            logRecordHelper.getPKHashValue(currentLogLocator));
                    jobLevelTxnId.setTxnId(logRecordHelper.getJobId(currentLogLocator), -1, -1);
                    if (winnerTxnTable.containsKey(tempKeyTxnId)) {
                        currentLSN = winnerTxnTable.get(tempKeyTxnId);
                        foundWinnerTxn = true;
                    } else if (winnerTxnTable.containsKey(jobLevelTxnId)) {
                        currentLSN = winnerTxnTable.get(jobLevelTxnId);
                        foundWinnerTxn = true;
                    }

                    if (foundWinnerTxn) {
                        resourceId = logRecordHelper.getResourceId(currentLogLocator);
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
                            resourceType = localResource.getResourceType();
                            immutableDiskIndexList = index.getImmutableComponents();

                            maxDiskLastLSN = -1;
                            switch (resourceType) {

                                case ResourceType.LSM_BTREE:
                                    for (ILSMComponent c : immutableDiskIndexList) {
                                        BTree btree = ((LSMBTreeImmutableComponent) c).getBTree();
                                        maxDiskLastLSN = Math.max(getTreeIndexLSN(btree), maxDiskLastLSN);
                                    }
                                    break;

                                case ResourceType.LSM_RTREE:
                                    for (ILSMComponent c : immutableDiskIndexList) {
                                        RTree rtree = ((LSMRTreeImmutableComponent) c).getRTree();
                                        maxDiskLastLSN = Math.max(getTreeIndexLSN(rtree), maxDiskLastLSN);
                                    }
                                    break;

                                case ResourceType.LSM_INVERTED_INDEX:
                                    for (ILSMComponent c : immutableDiskIndexList) {
                                        BTree delKeyBtree = ((LSMInvertedIndexImmutableComponent) c)
                                                .getDeletedKeysBTree();
                                        maxDiskLastLSN = Math.max(getTreeIndexLSN(delKeyBtree), maxDiskLastLSN);
                                    }
                                    break;

                                default:
                                    throw new ACIDException("Unsupported resouce type");
                            }

                            //#. set resourceId and maxDiskLastLSN to the map
                            resourceId2MaxLSNMap.put(resourceId, maxDiskLastLSN);
                        } else {
                            maxDiskLastLSN = resourceId2MaxLSNMap.get(resourceId);
                        }

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
                            if (IS_DEBUG_MODE) {
                                redoCount++;
                            }
                        }
                    }
                    break;

                case LogType.COMMIT:
                case LogType.ENTITY_COMMIT:
                    //do nothing
                    break;

                default:
                    throw new ACIDException("Unsupported LogType: " + logType);
            }
        }

        //close all indexes
        Set<Long> resourceIdList = resourceId2MaxLSNMap.keySet();
        for (long r : resourceIdList) {
            indexLifecycleManager.close(r);
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("[RecoveryMgr] recovery is over");
        }
        if (IS_DEBUG_MODE) {
            System.out.println("[RecoveryMgr] Count: Update/Commit/Redo = " + updateLogCount + "/" + commitLogCount
                    + "/" + redoCount);
        }
    }

    //TODO
    //This function came from the AbstractLSMIOOperationCallback class. 
    //We'd better factor out this function into a component of reading/writing the local metadata of indexes.
    private long getTreeIndexLSN(ITreeIndex treeIndex) throws HyracksDataException {
        int fileId = treeIndex.getFileId();
        IBufferCache bufferCache = treeIndex.getBufferCache();
        ITreeIndexMetaDataFrame metadataFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory().createFrame();
        int metadataPageId = treeIndex.getFreePageManager().getFirstMetadataPage();
        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, metadataPageId), false);
        metadataPage.acquireReadLatch();
        try {
            metadataFrame.setPage(metadataPage);
            return metadataFrame.getLSN();
        } finally {
            metadataPage.releaseReadLatch();
            bufferCache.unpin(metadataPage);
        }
    }

    @Override
    public synchronized void checkpoint(boolean isSharpCheckpoint) throws ACIDException {

        LogManager logMgr = (LogManager) txnSubsystem.getLogManager();
        TransactionManager txnMgr = (TransactionManager) txnSubsystem.getTransactionManager();
        String logDir = logMgr.getLogManagerProperties().getLogDir();

        //#. get the filename of the previous checkpoint files which are about to be deleted 
        //   right after the new checkpoint file is written.
        File[] prevCheckpointFiles = getPreviousCheckpointFiles();

        IIndexLifecycleManager indexLifecycleManager = txnSubsystem.getAsterixAppRuntimeContextProvider()
                .getIndexLifecycleManager();
        List<IIndex> openIndexList = indexLifecycleManager.getOpenIndexes();
        List<BlockingIOOperationCallbackWrapper> callbackList = new LinkedList<BlockingIOOperationCallbackWrapper>();

        //#. flush all in-memory components if it is the sharp checkpoint
        if (isSharpCheckpoint) {
            for (IIndex index : openIndexList) {
                ILSMIndex lsmIndex = (ILSMIndex) index;
                ILSMIndexAccessor indexAccessor = lsmIndex.createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);
                IndexOperationTracker indexOpTracker = (IndexOperationTracker) lsmIndex.getOperationTracker();
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
        }

        //#. create and store the checkpointObject into the new checkpoint file
        long minMCTFirstLSN = Long.MAX_VALUE;
        long firstLSN;
        if (openIndexList.size() > 0) {
            for (IIndex index : openIndexList) {
                firstLSN = ((IndexOperationTracker) ((ILSMIndex) index).getOperationTracker()).getFirstLSN();
                minMCTFirstLSN = Math.min(minMCTFirstLSN, firstLSN);
            }
        } else {
            minMCTFirstLSN = -1;
        }

        CheckpointObject checkpointObject = new CheckpointObject(logMgr.getCurrentLsn().get(), minMCTFirstLSN,
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
            for (File file : prevCheckpointFiles) {
                file.delete();
            }
        }

        if (isSharpCheckpoint) {
            logMgr.renewLogFiles();
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
                    throw new ACIDException("LastLSN mismatch: " + lastLSNLogLocator.getLsn() + " vs "
                            + currentLogLocator.getLsn() + " during Rollback a transaction( " + txnContext.getJobId()
                            + ")");
                } else {
                    break;//End of Log File
                }
            }

            if (IS_DEBUG_MODE) {
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
                        undoLSNSet = new LinkedList<Long>();
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
                case LogType.ENTITY_COMMIT:
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
            //TODO 
            //Sort the lsns in order to undo in one pass. 

            Map.Entry<TxnId, List<Long>> loserTxn = (Map.Entry<TxnId, List<Long>>) iter.next();
            txnId = loserTxn.getKey();

            undoLSNSet = loserTxn.getValue();

            for (long undoLSN : undoLSNSet) {
                // here, all the log records are UPDATE type. So, we don't need to check the type again.

                //read the corresponding log record to be undone.
                logManager.readLog(undoLSN, currentLogLocator);

                if (IS_DEBUG_MODE) {
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
