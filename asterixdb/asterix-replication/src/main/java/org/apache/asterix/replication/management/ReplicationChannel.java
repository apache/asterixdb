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
package org.apache.asterix.replication.management;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.replication.IReplicationThread;
import org.apache.asterix.common.replication.Replica;
import org.apache.asterix.common.replication.ReplicaEvent;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.common.transactions.IAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.asterix.replication.functions.ReplicaFilesRequest;
import org.apache.asterix.replication.functions.ReplicaIndexFlushRequest;
import org.apache.asterix.replication.functions.ReplicationProtocol;
import org.apache.asterix.replication.functions.ReplicationProtocol.ReplicationRequestType;
import org.apache.asterix.replication.logging.RemoteLogMapping;
import org.apache.asterix.replication.messaging.CheckpointPartitionIndexesTask;
import org.apache.asterix.replication.messaging.DeleteFileTask;
import org.apache.asterix.replication.messaging.PartitionResourcesListTask;
import org.apache.asterix.replication.messaging.ReplicateFileTask;
import org.apache.asterix.replication.storage.LSMComponentLSNSyncTask;
import org.apache.asterix.replication.storage.LSMComponentProperties;
import org.apache.asterix.replication.storage.LSMIndexFileProperties;
import org.apache.asterix.replication.storage.ReplicaResourcesManager;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.service.logging.LogBuffer;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.StorageUtil.StorageUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class is used to receive and process replication requests from remote replicas or replica events from CC
 */
public class ReplicationChannel extends Thread implements IReplicationChannel {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int LOG_REPLICATION_END_HANKSHAKE_LOG_SIZE = 1;
    private final ExecutorService replicationThreads;
    private final String localNodeID;
    private final ILogManager logManager;
    private final ReplicaResourcesManager replicaResourcesManager;
    private ServerSocketChannel serverSocketChannel = null;
    private final IReplicationManager replicationManager;
    private final ReplicationProperties replicationProperties;
    private final IAppRuntimeContextProvider appContextProvider;
    private static final int INTIAL_BUFFER_SIZE = StorageUtil.getIntSizeInBytes(4, StorageUnit.KILOBYTE);
    private final LinkedBlockingQueue<LSMComponentLSNSyncTask> lsmComponentRemoteLSN2LocalLSNMappingTaskQ;
    private final LinkedBlockingQueue<LogRecord> pendingNotificationRemoteLogsQ;
    private final Map<String, LSMComponentProperties> lsmComponentId2PropertiesMap;
    private final Map<Long, RemoteLogMapping> localLsn2RemoteMapping;
    private final Map<String, RemoteLogMapping> replicaUniqueLSN2RemoteMapping;
    private final LSMComponentsSyncService lsmComponentLSNMappingService;
    private final ReplicationNotifier replicationNotifier;
    private final Object flushLogslock = new Object();
    private final IDatasetLifecycleManager dsLifecycleManager;
    private final PersistentLocalResourceRepository localResourceRep;
    private final IReplicationStrategy replicationStrategy;
    private final NCConfig ncConfig;
    private Set nodeHostedPartitions;
    private final IIndexCheckpointManagerProvider indexCheckpointManagerProvider;
    private final INcApplicationContext appCtx;

    public ReplicationChannel(String nodeId, ReplicationProperties replicationProperties, ILogManager logManager,
            IReplicaResourcesManager replicaResoucesManager, IReplicationManager replicationManager,
            INCServiceContext ncServiceContext, IAppRuntimeContextProvider asterixAppRuntimeContextProvider,
            IReplicationStrategy replicationStrategy) {
        this.logManager = logManager;
        this.localNodeID = nodeId;
        this.replicaResourcesManager = (ReplicaResourcesManager) replicaResoucesManager;
        this.replicationManager = replicationManager;
        this.replicationProperties = replicationProperties;
        this.appContextProvider = asterixAppRuntimeContextProvider;
        this.dsLifecycleManager = asterixAppRuntimeContextProvider.getDatasetLifecycleManager();
        this.localResourceRep = (PersistentLocalResourceRepository) asterixAppRuntimeContextProvider
                .getLocalResourceRepository();
        this.replicationStrategy = replicationStrategy;
        this.ncConfig = ((NodeControllerService) ncServiceContext.getControllerService()).getConfiguration();
        lsmComponentRemoteLSN2LocalLSNMappingTaskQ = new LinkedBlockingQueue<>();
        pendingNotificationRemoteLogsQ = new LinkedBlockingQueue<>();
        lsmComponentId2PropertiesMap = new ConcurrentHashMap<>();
        replicaUniqueLSN2RemoteMapping = new ConcurrentHashMap<>();
        localLsn2RemoteMapping = new ConcurrentHashMap<>();
        lsmComponentLSNMappingService = new LSMComponentsSyncService();
        replicationNotifier = new ReplicationNotifier();
        replicationThreads = Executors.newCachedThreadPool(ncServiceContext.getThreadFactory());
        Map<String, ClusterPartition[]> nodePartitions = asterixAppRuntimeContextProvider.getAppContext()
                .getMetadataProperties().getNodePartitions();
        Set<String> nodeReplicationClients = replicationStrategy.getRemotePrimaryReplicas(nodeId).stream()
                .map(Replica::getId).collect(Collectors.toSet());
        List<Integer> clientsPartitions = new ArrayList<>();
        for (String clientId : nodeReplicationClients) {
            for (ClusterPartition clusterPartition : nodePartitions.get(clientId)) {
                clientsPartitions.add(clusterPartition.getPartitionId());
            }
        }
        nodeHostedPartitions = new HashSet<>(clientsPartitions.size());
        nodeHostedPartitions.addAll(clientsPartitions);
        this.indexCheckpointManagerProvider =
                ((INcApplicationContext) ncServiceContext.getApplicationContext()).getIndexCheckpointManagerProvider();
        this.appCtx = (INcApplicationContext) ncServiceContext.getApplicationContext();
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Replication Channel Thread");

        String nodeIP = replicationProperties.getNodeIpFromId(localNodeID);
        int dataPort = ncConfig.getReplicationPublicPort();
        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(true);
            InetSocketAddress replicationChannelAddress = new InetSocketAddress(InetAddress.getByName(nodeIP),
                    dataPort);
            serverSocketChannel.socket().bind(replicationChannelAddress);
            lsmComponentLSNMappingService.start();
            replicationNotifier.start();
            LOGGER.log(Level.INFO, "opened Replication Channel @ IP Address: " + nodeIP + ":" + dataPort);

            //start accepting replication requests
            while (serverSocketChannel.isOpen()) {
                SocketChannel socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(true);
                //start a new thread to handle the request
                replicationThreads.execute(new ReplicationThread(socketChannel));
            }
        } catch (AsynchronousCloseException e) {
            LOGGER.warn("Replication channel closed", e);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Could not open replication channel @ IP Address: " + nodeIP + ":" + dataPort, e);
        }
    }

    private void updateLSMComponentRemainingFiles(String lsmComponentId) throws IOException {
        LSMComponentProperties lsmCompProp = lsmComponentId2PropertiesMap.get(lsmComponentId);
        int remainingFile = lsmCompProp.markFileComplete();

        //clean up when all the LSM component files have been received.
        if (remainingFile == 0) {
            if (lsmCompProp.getOpType() == LSMOperationType.FLUSH && lsmCompProp.getReplicaLSN() != null
                    && replicaUniqueLSN2RemoteMapping.containsKey(lsmCompProp.getNodeUniqueLSN())) {
                int remainingIndexes = replicaUniqueLSN2RemoteMapping
                        .get(lsmCompProp.getNodeUniqueLSN()).numOfFlushedIndexes.decrementAndGet();
                if (remainingIndexes == 0) {
                    /**
                     * Note: there is a chance that this will never be removed because some
                     * index in the dataset was not flushed because it is empty. This could
                     * be solved by passing only the number of successfully flushed indexes.
                     */
                    replicaUniqueLSN2RemoteMapping.remove(lsmCompProp.getNodeUniqueLSN());
                }
            }

            //delete mask to indicate that this component is now valid.
            replicaResourcesManager.markLSMComponentReplicaAsValid(lsmCompProp);
            lsmComponentId2PropertiesMap.remove(lsmComponentId);
            LOGGER.log(Level.INFO, "Completed LSMComponent " + lsmComponentId + " Replication.");
        }
    }

    @Override
    public void close() throws IOException {
        serverSocketChannel.close();
        LOGGER.log(Level.INFO, "Replication channel closed.");
    }

    /**
     * A replication thread is created per received replication request.
     */
    private class ReplicationThread implements IReplicationThread {
        private final SocketChannel socketChannel;
        private final LogRecord remoteLog;
        private ByteBuffer inBuffer;
        private ByteBuffer outBuffer;

        public ReplicationThread(SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
            inBuffer = ByteBuffer.allocate(INTIAL_BUFFER_SIZE);
            outBuffer = ByteBuffer.allocate(INTIAL_BUFFER_SIZE);
            remoteLog = new LogRecord();
        }

        @Override
        public void run() {
            Thread.currentThread().setName("Replication Thread");
            try {
                ReplicationRequestType replicationFunction = ReplicationProtocol.getRequestType(socketChannel,
                        inBuffer);
                while (replicationFunction != ReplicationRequestType.GOODBYE) {
                    switch (replicationFunction) {
                        case REPLICATE_LOG:
                            handleLogReplication();
                            break;
                        case LSM_COMPONENT_PROPERTIES:
                            handleLSMComponentProperties();
                            break;
                        case REPLICATE_FILE:
                            handleReplicateFile();
                            break;
                        case DELETE_FILE:
                            handleDeleteFile();
                            break;
                        case REPLICA_EVENT:
                            handleReplicaEvent();
                            break;
                        case GET_REPLICA_MAX_LSN:
                            handleGetReplicaMaxLSN();
                            break;
                        case GET_REPLICA_FILES:
                            handleGetReplicaFiles();
                            break;
                        case FLUSH_INDEX:
                            handleFlushIndex();
                            break;
                        case PARTITION_RESOURCES_REQUEST:
                            handleGetPartitionResources();
                            break;
                        case REPLICATE_RESOURCE_FILE:
                            handleReplicateResourceFile();
                            break;
                        case DELETE_RESOURCE_FILE:
                            handleDeleteResourceFile();
                            break;
                        case CHECKPOINT_PARTITION:
                            handleCheckpointPartition();
                            break;
                        default:
                            throw new IllegalStateException("Unknown replication request");
                    }
                    replicationFunction = ReplicationProtocol.getRequestType(socketChannel, inBuffer);
                }
            } catch (Exception e) {
                LOGGER.warn("Unexpectedly error during replication.", e);
            } finally {
                if (socketChannel.isOpen()) {
                    try {
                        socketChannel.close();
                    } catch (IOException e) {
                        LOGGER.warn("Filed to close replication socket.", e);
                    }
                }
            }
        }

        private void handleFlushIndex() throws IOException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            //read which indexes are requested to be flushed from remote replica
            ReplicaIndexFlushRequest request = ReplicationProtocol.readReplicaIndexFlushRequest(inBuffer);
            Set<Long> requestedIndexesToBeFlushed = request.getLaggingRescouresIds();

            /**
             * check which indexes can be flushed (open indexes) and which cannot be
             * flushed (closed or have empty memory component).
             */
            IDatasetLifecycleManager datasetLifeCycleManager = appContextProvider.getDatasetLifecycleManager();
            List<IndexInfo> openIndexesInfo = datasetLifeCycleManager.getOpenIndexesInfo();
            Set<Integer> datasetsToForceFlush = new HashSet<>();
            for (IndexInfo iInfo : openIndexesInfo) {
                if (requestedIndexesToBeFlushed.contains(iInfo.getResourceId())) {
                    AbstractLSMIOOperationCallback ioCallback = (AbstractLSMIOOperationCallback) iInfo.getIndex()
                            .getIOOperationCallback();
                    //if an index has a pending flush, then the request to flush it will succeed.
                    if (ioCallback.hasPendingFlush()) {
                        //remove index to indicate that it will be flushed
                        requestedIndexesToBeFlushed.remove(iInfo.getResourceId());
                    } else if (!((AbstractLSMIndex) iInfo.getIndex()).isCurrentMutableComponentEmpty()) {
                        /**
                         * if an index has something to be flushed, then the request to flush it
                         * will succeed and we need to schedule it to be flushed.
                         */
                        datasetsToForceFlush.add(iInfo.getDatasetId());
                        //remove index to indicate that it will be flushed
                        requestedIndexesToBeFlushed.remove(iInfo.getResourceId());
                    }
                }
            }

            //schedule flush for datasets requested to be flushed
            for (int datasetId : datasetsToForceFlush) {
                datasetLifeCycleManager.flushDataset(datasetId, true);
            }

            //the remaining indexes in the requested set are those which cannot be flushed.
            //respond back to the requester that those indexes cannot be flushed
            ReplicaIndexFlushRequest laggingIndexesResponse = new ReplicaIndexFlushRequest(requestedIndexesToBeFlushed);
            outBuffer = ReplicationProtocol.writeGetReplicaIndexFlushRequest(outBuffer, laggingIndexesResponse);
            NetworkingUtil.transferBufferToChannel(socketChannel, outBuffer);
        }

        private void handleLSMComponentProperties() throws IOException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            LSMComponentProperties lsmCompProp = ReplicationProtocol.readLSMPropertiesRequest(inBuffer);
            //create mask to indicate that this component is not valid yet
            replicaResourcesManager.createRemoteLSMComponentMask(lsmCompProp);
            lsmComponentId2PropertiesMap.put(lsmCompProp.getComponentId(), lsmCompProp);
        }

        private void handleReplicateFile() throws IOException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            LSMIndexFileProperties afp = ReplicationProtocol.readFileReplicationRequest(inBuffer);

            //get index path
            String indexPath = replicaResourcesManager.getIndexPath(afp);
            String replicaFilePath = indexPath + File.separator + afp.getFileName();

            //create file
            File destFile = new File(replicaFilePath);
            destFile.createNewFile();

            try (RandomAccessFile fileOutputStream = new RandomAccessFile(destFile, "rw");
                    FileChannel fileChannel = fileOutputStream.getChannel()) {
                fileOutputStream.setLength(afp.getFileSize());
                NetworkingUtil.downloadFile(fileChannel, socketChannel);
                fileChannel.force(true);

                if (afp.requiresAck()) {
                    ReplicationProtocol.sendAck(socketChannel);
                }
                if (afp.isLSMComponentFile()) {
                    String componentId = LSMComponentProperties.getLSMComponentID(afp.getFilePath());
                    final LSMComponentProperties lsmComponentProperties = lsmComponentId2PropertiesMap.get(componentId);
                    // merge operations do not generate flush logs
                    if (afp.requiresAck() && lsmComponentProperties.getOpType() == LSMOperationType.FLUSH) {
                        LSMComponentLSNSyncTask syncTask =
                                new LSMComponentLSNSyncTask(componentId, destFile.getAbsolutePath());
                        lsmComponentRemoteLSN2LocalLSNMappingTaskQ.offer(syncTask);
                    } else {
                        updateLSMComponentRemainingFiles(componentId);
                    }
                } else {
                    //index metadata file
                    final ResourceReference indexRef = ResourceReference.of(destFile.getAbsolutePath());
                    indexCheckpointManagerProvider.get(indexRef).init(logManager.getAppendLSN());
                }
            }
        }

        private void handleGetReplicaMaxLSN() throws IOException {
            long maxLNS = logManager.getAppendLSN();
            outBuffer.clear();
            outBuffer.putLong(maxLNS);
            outBuffer.flip();
            NetworkingUtil.transferBufferToChannel(socketChannel, outBuffer);
        }

        private void handleGetReplicaFiles() throws IOException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            ReplicaFilesRequest request = ReplicationProtocol.readReplicaFileRequest(inBuffer);

            LSMIndexFileProperties fileProperties = new LSMIndexFileProperties();

            List<String> filesList;
            Set<Integer> partitionIds = request.getPartitionIds();
            Set<String> requesterExistingFiles = request.getExistingFiles();
            Map<Integer, ClusterPartition> clusterPartitions = appContextProvider.getAppContext()
                    .getMetadataProperties().getClusterPartitions();

            // Flush replicated datasets to generate the latest LSM components
            dsLifecycleManager.flushDataset(replicationStrategy);
            for (Integer partitionId : partitionIds) {
                ClusterPartition partition = clusterPartitions.get(partitionId);
                filesList = replicaResourcesManager.getPartitionIndexesFiles(partition.getPartitionId(), false);
                //start sending files
                for (String filePath : filesList) {
                    // Send only files of datasets that are replciated.
                    DatasetResourceReference indexFileRef = localResourceRep.getLocalResourceReference(filePath);
                    if (!replicationStrategy.isMatch(indexFileRef.getDatasetId())) {
                        continue;
                    }
                    String relativeFilePath = StoragePathUtil.getIndexFileRelativePath(filePath);
                    //if the file already exists on the requester, skip it
                    if (!requesterExistingFiles.contains(relativeFilePath)) {
                        try (RandomAccessFile fromFile = new RandomAccessFile(filePath, "r");
                                FileChannel fileChannel = fromFile.getChannel();) {
                            long fileSize = fileChannel.size();
                            fileProperties.initialize(filePath, fileSize, partition.getNodeId(), false, false);
                            outBuffer = ReplicationProtocol.writeFileReplicationRequest(outBuffer, fileProperties,
                                    ReplicationRequestType.REPLICATE_FILE);

                            //send file info
                            NetworkingUtil.transferBufferToChannel(socketChannel, outBuffer);

                            //transfer file
                            NetworkingUtil.sendFile(fileChannel, socketChannel);
                        }
                    }
                }
            }

            //send goodbye (end of files)
            ReplicationProtocol.sendGoodbye(socketChannel);
        }

        private void handleReplicaEvent() throws IOException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            ReplicaEvent event = ReplicationProtocol.readReplicaEventRequest(inBuffer);
            replicationManager.reportReplicaEvent(event);
        }

        private void handleDeleteFile() throws IOException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            LSMIndexFileProperties fileProp = ReplicationProtocol.readFileReplicationRequest(inBuffer);
            replicaResourcesManager.deleteIndexFile(fileProp);
            if (fileProp.requiresAck()) {
                ReplicationProtocol.sendAck(socketChannel);
            }
        }

        private void handleLogReplication() throws IOException, ACIDException {
            //set initial buffer size to a log buffer page size
            inBuffer = ByteBuffer.allocate(logManager.getLogPageSize());
            while (true) {
                //read a batch of logs
                inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
                //check if it is end of handshake (a single byte log)
                if (inBuffer.remaining() == LOG_REPLICATION_END_HANKSHAKE_LOG_SIZE) {
                    break;
                }

                processLogsBatch(inBuffer);
            }
        }

        private void processLogsBatch(ByteBuffer buffer) throws ACIDException {
            while (buffer.hasRemaining()) {
                //get rid of log size
                inBuffer.getInt();
                //Deserialize log
                remoteLog.readRemoteLog(inBuffer);
                remoteLog.setLogSource(LogSource.REMOTE);

                switch (remoteLog.getLogType()) {
                    case LogType.UPDATE:
                    case LogType.ENTITY_COMMIT:
                        logManager.log(remoteLog);
                        break;
                    case LogType.JOB_COMMIT:
                    case LogType.ABORT:
                        LogRecord jobTerminationLog = new LogRecord();
                        TransactionUtil.formJobTerminateLogRecord(jobTerminationLog, remoteLog.getTxnId(),
                                remoteLog.getLogType() == LogType.JOB_COMMIT);
                        jobTerminationLog.setReplicationThread(this);
                        jobTerminationLog.setLogSource(LogSource.REMOTE);
                        logManager.log(jobTerminationLog);
                        break;
                    case LogType.FLUSH:
                        //store mapping information for flush logs to use them in incoming LSM components.
                        RemoteLogMapping flushLogMap = new RemoteLogMapping();
                        LogRecord flushLog = new LogRecord();
                        TransactionUtil.formFlushLogRecord(flushLog, remoteLog.getDatasetId(), null,
                                remoteLog.getNodeId(), remoteLog.getNumOfFlushedIndexes());
                        flushLog.setReplicationThread(this);
                        flushLog.setLogSource(LogSource.REMOTE);
                        flushLogMap.setRemoteNodeID(remoteLog.getNodeId());
                        flushLogMap.setRemoteLSN(remoteLog.getLSN());
                        synchronized (localLsn2RemoteMapping) {
                            logManager.log(flushLog);
                            //the log LSN value is updated by logManager.log(.) to a local value
                            flushLogMap.setLocalLSN(flushLog.getLSN());
                            flushLogMap.numOfFlushedIndexes.set(flushLog.getNumOfFlushedIndexes());
                            replicaUniqueLSN2RemoteMapping.put(flushLogMap.getNodeUniqueLSN(), flushLogMap);
                            localLsn2RemoteMapping.put(flushLog.getLSN(), flushLogMap);
                        }
                        synchronized (flushLogslock) {
                            flushLogslock.notify();
                        }
                        break;
                    default:
                        LOGGER.error("Unsupported LogType: " + remoteLog.getLogType());
                }
            }
        }

        /**
         * this method is called sequentially by {@link LogBuffer#notifyReplicationTermination()}
         * for JOB_COMMIT, JOB_ABORT, and FLUSH log types.
         */
        @Override
        public void notifyLogReplicationRequester(LogRecord logRecord) {
            switch (logRecord.getLogType()) {
                case LogType.JOB_COMMIT:
                case LogType.ABORT:
                    pendingNotificationRemoteLogsQ.offer(logRecord);
                    break;
                case LogType.FLUSH:
                    final RemoteLogMapping remoteLogMapping;
                    synchronized (localLsn2RemoteMapping) {
                        remoteLogMapping = localLsn2RemoteMapping.remove(logRecord.getLSN());
                    }
                    checkpointReplicaIndexes(remoteLogMapping, logRecord.getDatasetId());
                    break;
                default:
                    throw new IllegalStateException("Unexpected log type: " + logRecord.getLogType());
            }
        }

        @Override
        public SocketChannel getChannel() {
            return socketChannel;
        }

        @Override
        public ByteBuffer getReusableBuffer() {
            return outBuffer;
        }

        private void checkpointReplicaIndexes(RemoteLogMapping remoteLogMapping, int datasetId) {
            try {
                Predicate<LocalResource> replicaIndexesPredicate = lr -> {
                    DatasetLocalResource dls = (DatasetLocalResource) lr.getResource();
                    return dls.getDatasetId() == datasetId && !localResourceRep.getActivePartitions()
                            .contains(dls.getPartition());
                };
                final Map<Long, LocalResource> resources = localResourceRep.getResources(replicaIndexesPredicate);
                final List<DatasetResourceReference> replicaIndexesRef =
                        resources.values().stream().map(DatasetResourceReference::of).collect(Collectors.toList());
                for (DatasetResourceReference replicaIndexRef : replicaIndexesRef) {
                    final IIndexCheckpointManager indexCheckpointManager =
                            indexCheckpointManagerProvider.get(replicaIndexRef);
                    synchronized (indexCheckpointManager) {
                        indexCheckpointManager
                                .masterFlush(remoteLogMapping.getRemoteLSN(), remoteLogMapping.getLocalLSN());
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Failed to checkpoint replica indexes", e);
            }
        }

        private void handleGetPartitionResources() throws IOException {
            final PartitionResourcesListTask task = (PartitionResourcesListTask) ReplicationProtocol
                    .readMessage(ReplicationRequestType.PARTITION_RESOURCES_REQUEST, socketChannel, inBuffer);
            task.perform(appCtx, this);
        }

        private void handleReplicateResourceFile() throws HyracksDataException {
            ReplicateFileTask task = (ReplicateFileTask) ReplicationProtocol
                    .readMessage(ReplicationRequestType.REPLICATE_RESOURCE_FILE, socketChannel, inBuffer);
            task.perform(appCtx, this);
        }

        private void handleDeleteResourceFile() throws HyracksDataException {
            DeleteFileTask task = (DeleteFileTask) ReplicationProtocol
                    .readMessage(ReplicationRequestType.DELETE_RESOURCE_FILE, socketChannel, inBuffer);
            task.perform(appCtx, this);
        }

        private void handleCheckpointPartition() throws HyracksDataException {
            CheckpointPartitionIndexesTask task = (CheckpointPartitionIndexesTask) ReplicationProtocol
                    .readMessage(ReplicationRequestType.CHECKPOINT_PARTITION, socketChannel, inBuffer);
            task.perform(appCtx, this);
        }
    }

    /**
     * This thread is responsible for sending JOB_COMMIT/ABORT ACKs to replication clients.
     */
    private class ReplicationNotifier extends Thread {
        @Override
        public void run() {
            Thread.currentThread().setName("ReplicationNotifier Thread");
            while (true) {
                try {
                    LogRecord logRecord = pendingNotificationRemoteLogsQ.take();
                    //send ACK to requester
                    logRecord.getReplicationThread().getChannel().socket().getOutputStream()
                            .write((localNodeID + ReplicationProtocol.JOB_REPLICATION_ACK + logRecord.getTxnId()
                                    + System.lineSeparator()).getBytes());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    LOGGER.warn("Failed to send job replication ACK", e);
                }
            }
        }
    }

    /**
     * This thread is responsible for synchronizing the LSN of
     * the received LSM components to a local LSN.
     */
    private class LSMComponentsSyncService extends Thread {

        @Override
        public void run() {
            Thread.currentThread().setName("LSMComponentsSyncService Thread");

            while (true) {
                try {
                    LSMComponentLSNSyncTask syncTask = lsmComponentRemoteLSN2LocalLSNMappingTaskQ.take();
                    LSMComponentProperties lsmCompProp = lsmComponentId2PropertiesMap.get(syncTask.getComponentId());
                    syncLSMComponentFlushLSN(lsmCompProp, syncTask);
                    updateLSMComponentRemainingFiles(lsmCompProp.getComponentId());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LOGGER.error("Unexpected exception during LSN synchronization", e);
                }
            }
        }

        private void syncLSMComponentFlushLSN(LSMComponentProperties lsmCompProp, LSMComponentLSNSyncTask syncTask)
                throws InterruptedException, IOException {
            final String componentFilePath = syncTask.getComponentFilePath();
            final ResourceReference indexRef = ResourceReference.of(componentFilePath);
            final IIndexCheckpointManager indexCheckpointManager = indexCheckpointManagerProvider.get(indexRef);
            synchronized (indexCheckpointManager) {
                long masterLsn = lsmCompProp.getOriginalLSN();
                // wait until the lsn mapping is flushed to disk
                while (!indexCheckpointManager.isFlushed(masterLsn)) {
                    indexCheckpointManager.wait();
                }
                indexCheckpointManager
                        .replicated(AbstractLSMIndexFileManager.getComponentEndTime(indexRef.getName()), masterLsn);
            }
        }
    }
}
