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
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.context.DatasetLifecycleManager.IndexInfo;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.replication.IReplicationThread;
import org.apache.asterix.common.replication.ReplicaEvent;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ILogReader;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.asterix.replication.functions.ReplicaFilesRequest;
import org.apache.asterix.replication.functions.ReplicaIndexFlushRequest;
import org.apache.asterix.replication.functions.ReplicaLogsRequest;
import org.apache.asterix.replication.functions.ReplicationProtocol;
import org.apache.asterix.replication.functions.ReplicationProtocol.ReplicationRequestType;
import org.apache.asterix.replication.logging.RemoteLogMapping;
import org.apache.asterix.replication.storage.LSMComponentLSNSyncTask;
import org.apache.asterix.replication.storage.LSMComponentProperties;
import org.apache.asterix.replication.storage.LSMIndexFileProperties;
import org.apache.asterix.replication.storage.ReplicaResourcesManager;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.storage.am.common.api.IMetaDataPageManager;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;

/**
 * This class is used to receive and process replication requests from remote replicas or replica events from CC
 */
public class ReplicationChannel extends Thread implements IReplicationChannel {

    private static final Logger LOGGER = Logger.getLogger(ReplicationChannel.class.getName());
    private final ExecutorService replicationThreads;
    private final String localNodeID;
    private final ILogManager logManager;
    private final ReplicaResourcesManager replicaResourcesManager;
    private SocketChannel socketChannel = null;
    private ServerSocketChannel serverSocketChannel = null;
    private final IReplicationManager replicationManager;
    private final AsterixReplicationProperties replicationProperties;
    private final IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider;
    private final static int INTIAL_BUFFER_SIZE = 4000; //4KB
    private final LinkedBlockingQueue<LSMComponentLSNSyncTask> lsmComponentRemoteLSN2LocalLSNMappingTaskQ;
    private final Map<String, LSMComponentProperties> lsmComponentId2PropertiesMap;
    private final Map<Long, RemoteLogMapping> localLSN2RemoteLSNMap;
    private final LSMComponentsSyncService lsmComponentLSNMappingService;
    private final Set<Integer> nodeHostedPartitions;

    public ReplicationChannel(String nodeId, AsterixReplicationProperties replicationProperties, ILogManager logManager,
            IReplicaResourcesManager replicaResoucesManager, IReplicationManager replicationManager,
            INCApplicationContext appContext, IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider) {
        this.logManager = logManager;
        this.localNodeID = nodeId;
        this.replicaResourcesManager = (ReplicaResourcesManager) replicaResoucesManager;
        this.replicationManager = replicationManager;
        this.replicationProperties = replicationProperties;
        this.asterixAppRuntimeContextProvider = asterixAppRuntimeContextProvider;
        lsmComponentRemoteLSN2LocalLSNMappingTaskQ = new LinkedBlockingQueue<LSMComponentLSNSyncTask>();
        lsmComponentId2PropertiesMap = new ConcurrentHashMap<String, LSMComponentProperties>();
        localLSN2RemoteLSNMap = new ConcurrentHashMap<Long, RemoteLogMapping>();
        lsmComponentLSNMappingService = new LSMComponentsSyncService();
        replicationThreads = Executors.newCachedThreadPool(appContext.getThreadFactory());
        Map<String, ClusterPartition[]> nodePartitions = ((IAsterixPropertiesProvider) asterixAppRuntimeContextProvider
                .getAppContext()).getMetadataProperties().getNodePartitions();
        Set<String> nodeReplicationClients = replicationProperties.getNodeReplicationClients(nodeId);
        List<Integer> clientsPartitions = new ArrayList<>();
        for (String clientId : nodeReplicationClients) {
            for (ClusterPartition clusterPartition : nodePartitions.get(clientId)) {
                clientsPartitions.add(clusterPartition.getPartitionId());
            }
        }
        nodeHostedPartitions = new HashSet<>(clientsPartitions.size());
        nodeHostedPartitions.addAll(clientsPartitions);
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Replication Channel Thread");

        String nodeIP = replicationProperties.getReplicaIPAddress(localNodeID);
        int dataPort = replicationProperties.getDataReplicationPort(localNodeID);
        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(true);
            InetSocketAddress replicationChannelAddress = new InetSocketAddress(InetAddress.getByName(nodeIP),
                    dataPort);
            serverSocketChannel.socket().bind(replicationChannelAddress);
            lsmComponentLSNMappingService.start();

            LOGGER.log(Level.INFO, "opened Replication Channel @ IP Address: " + nodeIP + ":" + dataPort);

            //start accepting replication requests
            while (true) {
                socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(true);
                //start a new thread to handle the request
                replicationThreads.execute(new ReplicationThread(socketChannel));
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Could not opened replication channel @ IP Address: " + nodeIP + ":" + dataPort, e);
        }
    }

    private void updateLSMComponentRemainingFiles(String lsmComponentId) throws IOException {
        LSMComponentProperties lsmCompProp = lsmComponentId2PropertiesMap.get(lsmComponentId);
        int remainingFile = lsmCompProp.markFileComplete();

        //clean up when all the LSM component files have been received.
        if (remainingFile == 0) {
            if (lsmCompProp.getOpType() == LSMOperationType.FLUSH && lsmCompProp.getReplicaLSN() != null) {
                //if this LSN wont be used for any other index, remove it
                if (localLSN2RemoteLSNMap.containsKey(lsmCompProp.getReplicaLSN())) {
                    int remainingIndexes = localLSN2RemoteLSNMap.get(lsmCompProp.getReplicaLSN()).numOfFlushedIndexes
                            .decrementAndGet();
                    if (remainingIndexes == 0) {
                        //Note: there is a chance that this is never deleted because some index in the dataset was not flushed because it is empty.
                        //This could be solved by passing only the number of successfully flushed indexes
                        localLSN2RemoteLSNMap.remove(lsmCompProp.getReplicaLSN());
                    }
                }
            }

            //delete mask to indicate that this component is now valid.
            replicaResourcesManager.markLSMComponentReplicaAsValid(lsmCompProp);
            lsmComponentId2PropertiesMap.remove(lsmComponentId);
            LOGGER.log(Level.INFO, "Completed LSMComponent " + lsmComponentId + " Replication.");
        }
    }

    /**
     * @param replicaId
     *            the remote replica id this log belongs to.
     * @param remoteLSN
     *            the remote LSN received from the remote replica.
     * @return The local log mapping if found. Otherwise null.
     */
    private RemoteLogMapping getRemoteLogMapping(String replicaId, long remoteLSN) {
        for (RemoteLogMapping mapping : localLSN2RemoteLSNMap.values()) {
            if (mapping.getRemoteLSN() == remoteLSN && mapping.getRemoteNodeID().equals(replicaId)) {
                return mapping;
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (!serverSocketChannel.isOpen()) {
            serverSocketChannel.close();
            LOGGER.log(Level.INFO, "Replication channel closed.");
        }
    }

    private static void sendRemoteRecoveryLog(ILogRecord logRecord, SocketChannel socketChannel, ByteBuffer outBuffer)
            throws IOException {
        logRecord.setLogSource(LogSource.REMOTE_RECOVERY);
        if (logRecord.getSerializedLogSize() > outBuffer.capacity()) {
            int requestSize = logRecord.getSerializedLogSize() + ReplicationProtocol.REPLICATION_REQUEST_HEADER_SIZE;
            outBuffer = ByteBuffer.allocate(requestSize);
        }
        //set log source to REMOTE_RECOVERY to avoid re-logging on the recipient side
        ReplicationProtocol.writeRemoteRecoveryLogRequest(outBuffer, logRecord);
        NetworkingUtil.transferBufferToChannel(socketChannel, outBuffer);
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
                        case GET_REPLICA_MIN_LSN:
                            handleGetReplicaMinLSN();
                            break;
                        case GET_REPLICA_FILES:
                            handleGetReplicaFiles();
                            break;
                        case GET_REPLICA_LOGS:
                            handleGetRemoteLogs();
                            break;
                        case FLUSH_INDEX:
                            handleFlushIndex();
                            break;
                        default: {
                            throw new IllegalStateException("Unknown replication request");
                        }
                    }
                    replicationFunction = ReplicationProtocol.getRequestType(socketChannel, inBuffer);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (socketChannel.isOpen()) {
                    try {
                        socketChannel.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        private void handleFlushIndex() throws IOException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            //1. read which indexes are requested to be flushed from remote replica
            ReplicaIndexFlushRequest request = ReplicationProtocol.readReplicaIndexFlushRequest(inBuffer);
            Set<Long> requestedIndexesToBeFlushed = request.getLaggingRescouresIds();

            //2. check which indexes can be flushed (open indexes) and which cannot be flushed (closed or have empty memory component)
            IDatasetLifecycleManager datasetLifeCycleManager = asterixAppRuntimeContextProvider
                    .getDatasetLifecycleManager();
            List<IndexInfo> openIndexesInfo = datasetLifeCycleManager.getOpenIndexesInfo();
            Set<Integer> datasetsToForceFlush = new HashSet<Integer>();
            for (IndexInfo iInfo : openIndexesInfo) {
                if (requestedIndexesToBeFlushed.contains(iInfo.getResourceId())) {
                    AbstractLSMIOOperationCallback ioCallback = (AbstractLSMIOOperationCallback) iInfo.getIndex()
                            .getIOOperationCallback();
                    //if an index has a pending flush, then the request to flush it will succeed.
                    if (ioCallback.hasPendingFlush()) {
                        //remove index to indicate that it will be flushed
                        requestedIndexesToBeFlushed.remove(iInfo.getResourceId());
                    } else if (!((AbstractLSMIndex) iInfo.getIndex()).isCurrentMutableComponentEmpty()) {
                        //if an index has something to be flushed, then the request to flush it will succeed and we need to schedule it to be flushed.
                        datasetsToForceFlush.add(iInfo.getDatasetId());
                        //remove index to indicate that it will be flushed
                        requestedIndexesToBeFlushed.remove(iInfo.getResourceId());
                    }
                }
            }

            //3. force flush datasets requested to be flushed
            for (int datasetId : datasetsToForceFlush) {
                datasetLifeCycleManager.flushDataset(datasetId, true);
            }

            //the remaining indexes in the requested set are those which cannot be flushed.
            //4. respond back to the requester that those indexes cannot be flushed
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
                    if (afp.getLSNByteOffset() != IMetaDataPageManager.INVALID_LSN_OFFSET) {
                        LSMComponentLSNSyncTask syncTask = new LSMComponentLSNSyncTask(componentId,
                                destFile.getAbsolutePath(), afp.getLSNByteOffset());
                        lsmComponentRemoteLSN2LocalLSNMappingTaskQ.offer(syncTask);
                    } else {
                        updateLSMComponentRemainingFiles(componentId);
                    }
                } else {
                    //index metadata file
                    replicaResourcesManager.initializeReplicaIndexLSNMap(indexPath, logManager.getAppendLSN());
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

        private void handleGetReplicaMinLSN() throws IOException {
            long minLSN = asterixAppRuntimeContextProvider.getAppContext().getTransactionSubsystem()
                    .getRecoveryManager().getMinFirstLSN();
            outBuffer.clear();
            outBuffer.putLong(minLSN);
            outBuffer.flip();
            NetworkingUtil.transferBufferToChannel(socketChannel, outBuffer);
        }

        private void handleGetReplicaFiles() throws IOException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            ReplicaFilesRequest request = ReplicationProtocol.readReplicaFileRequest(inBuffer);

            LSMIndexFileProperties fileProperties = new LSMIndexFileProperties();

            List<String> filesList;
            Set<String> replicaIds = request.getReplicaIds();
            Set<String> requesterExistingFiles = request.getExistingFiles();
            Map<String, ClusterPartition[]> nodePartitions = ((IAsterixPropertiesProvider) asterixAppRuntimeContextProvider
                    .getAppContext()).getMetadataProperties().getNodePartitions();
            for (String replicaId : replicaIds) {
                //get replica partitions
                ClusterPartition[] replicaPatitions = nodePartitions.get(replicaId);
                for (ClusterPartition partition : replicaPatitions) {
                    filesList = replicaResourcesManager.getPartitionIndexesFiles(partition.getPartitionId(), false);
                    //start sending files
                    for (String filePath : filesList) {
                        String relativeFilePath = PersistentLocalResourceRepository.getResourceRelativePath(filePath);
                        //if the file already exists on the requester, skip it
                        if (!requesterExistingFiles.contains(relativeFilePath)) {
                            try (RandomAccessFile fromFile = new RandomAccessFile(filePath, "r");
                                    FileChannel fileChannel = fromFile.getChannel();) {
                                long fileSize = fileChannel.size();
                                fileProperties.initialize(filePath, fileSize, replicaId, false,
                                        IMetaDataPageManager.INVALID_LSN_OFFSET, false);
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
            }

            //send goodbye (end of files)
            ReplicationProtocol.sendGoodbye(socketChannel);
        }

        private void handleGetRemoteLogs() throws IOException, ACIDException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            ReplicaLogsRequest request = ReplicationProtocol.readReplicaLogsRequest(inBuffer);

            Set<String> replicaIds = request.getReplicaIds();
            //get list of partitions that belong to the replicas in the request
            Set<Integer> requestedPartitions = new HashSet<>();
            Map<String, ClusterPartition[]> nodePartitions = ((IAsterixPropertiesProvider) asterixAppRuntimeContextProvider
                    .getAppContext()).getMetadataProperties().getNodePartitions();
            for (String replicaId : replicaIds) {
                //get replica partitions
                ClusterPartition[] replicaPatitions = nodePartitions.get(replicaId);
                for (ClusterPartition partition : replicaPatitions) {
                    requestedPartitions.add(partition.getPartitionId());
                }
            }

            long fromLSN = request.getFromLSN();
            long minLocalFirstLSN = asterixAppRuntimeContextProvider.getAppContext().getTransactionSubsystem()
                    .getRecoveryManager().getLocalMinFirstLSN();

            //get Log reader
            ILogReader logReader = logManager.getLogReader(true);
            try {
                if (fromLSN < logManager.getReadableSmallestLSN()) {
                    fromLSN = logManager.getReadableSmallestLSN();
                }

                logReader.initializeScan(fromLSN);
                ILogRecord logRecord = logReader.next();
                Set<Integer> requestedPartitionsJobs = new HashSet<>();
                while (logRecord != null) {
                    //we should not send any local log which has already been converted to disk component
                    if (logRecord.getLogSource() == LogSource.LOCAL && logRecord.getLSN() < minLocalFirstLSN) {
                        logRecord = logReader.next();
                        continue;
                    }
                    //send only logs that belong to the partitions of the request and required for recovery
                    switch (logRecord.getLogType()) {
                        case LogType.UPDATE:
                        case LogType.ENTITY_COMMIT:
                        case LogType.UPSERT_ENTITY_COMMIT:
                            if (requestedPartitions.contains(logRecord.getResourcePartition())) {
                                sendRemoteRecoveryLog(logRecord, socketChannel, outBuffer);
                                requestedPartitionsJobs.add(logRecord.getJobId());
                            }
                            break;
                        case LogType.JOB_COMMIT:
                            if (requestedPartitionsJobs.contains(logRecord.getJobId())) {
                                sendRemoteRecoveryLog(logRecord, socketChannel, outBuffer);
                                requestedPartitionsJobs.remove(logRecord.getJobId());
                            }
                            break;
                        case LogType.ABORT:
                        case LogType.FLUSH:
                            break;
                        default:
                            throw new ACIDException("Unsupported LogType: " + logRecord.getLogType());
                    }
                    logRecord = logReader.next();
                }
            } finally {
                logReader.close();
            }

            //send goodbye (end of logs)
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
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);

            //Deserialize log
            remoteLog.readRemoteLog(inBuffer, false);
            remoteLog.setLogSource(LogSource.REMOTE);

            switch (remoteLog.getLogType()) {
                case LogType.UPDATE:
                case LogType.ENTITY_COMMIT:
                case LogType.UPSERT_ENTITY_COMMIT:
                    //if the log partition belongs to a partitions hosted on this node, replicate it
                    if (nodeHostedPartitions.contains(remoteLog.getResourcePartition())) {
                        logManager.log(remoteLog);
                    }
                    break;
                case LogType.JOB_COMMIT:
                    LogRecord jobCommitLog = new LogRecord();
                    TransactionUtil.formJobTerminateLogRecord(jobCommitLog, remoteLog.getJobId(), true);
                    jobCommitLog.setReplicationThread(this);
                    jobCommitLog.setLogSource(LogSource.REMOTE);
                    logManager.log(jobCommitLog);
                    break;
                case LogType.FLUSH:
                    LogRecord flushLog = new LogRecord();
                    TransactionUtil.formFlushLogRecord(flushLog, remoteLog.getDatasetId(), null, remoteLog.getNodeId(),
                            remoteLog.getNumOfFlushedIndexes());
                    flushLog.setReplicationThread(this);
                    flushLog.setLogSource(LogSource.REMOTE);
                    synchronized (localLSN2RemoteLSNMap) {
                        logManager.log(flushLog);
                        //store mapping information for flush logs to use them in incoming LSM components.
                        RemoteLogMapping flushLogMap = new RemoteLogMapping();
                        flushLogMap.setRemoteLSN(remoteLog.getLSN());
                        flushLogMap.setRemoteNodeID(remoteLog.getNodeId());
                        flushLogMap.setLocalLSN(flushLog.getLSN());
                        flushLogMap.numOfFlushedIndexes.set(remoteLog.getNumOfFlushedIndexes());
                        localLSN2RemoteLSNMap.put(flushLog.getLSN(), flushLogMap);
                        localLSN2RemoteLSNMap.notifyAll();
                    }
                    break;
                default:
                    throw new ACIDException("Unsupported LogType: " + remoteLog.getLogType());
            }
        }

        //this method is called sequentially by LogPage (notifyReplicationTerminator) for JOB_COMMIT and FLUSH log types
        @Override
        public void notifyLogReplicationRequester(LogRecord logRecord) {
            //Note: this could be optimized by moving this to a different thread and freeing the LogPage thread faster
            if (logRecord.getLogType() == LogType.JOB_COMMIT) {
                //send ACK to requester
                try {
                    socketChannel.socket().getOutputStream()
                            .write((localNodeID + ReplicationProtocol.JOB_COMMIT_ACK + logRecord.getJobId() + "\n")
                                    .getBytes());
                    socketChannel.socket().getOutputStream().flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else if (logRecord.getLogType() == LogType.FLUSH) {
                synchronized (localLSN2RemoteLSNMap) {
                    RemoteLogMapping remoteLogMap = localLSN2RemoteLSNMap.get(logRecord.getLSN());
                    synchronized (remoteLogMap) {
                        remoteLogMap.setFlushed(true);
                        remoteLogMap.notifyAll();
                    }
                }
            }
        }
    }

    /**
     * This thread is responsible for synchronizing the LSN of the received LSM components to a local LSN.
     */
    private class LSMComponentsSyncService extends Thread {
        @Override
        public void run() {
            Thread.currentThread().setName("LSMComponentsSyncService Thread");

            while (true) {
                try {
                    LSMComponentLSNSyncTask syncTask = lsmComponentRemoteLSN2LocalLSNMappingTaskQ.take();
                    LSMComponentProperties lsmCompProp = lsmComponentId2PropertiesMap.get(syncTask.getComponentId());
                    try {
                        syncLSMComponentFlushLSN(lsmCompProp, syncTask);
                        updateLSMComponentRemainingFiles(lsmCompProp.getComponentId());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        }

        private void syncLSMComponentFlushLSN(LSMComponentProperties lsmCompProp, LSMComponentLSNSyncTask syncTask)
                throws Exception {
            long remoteLSN = lsmCompProp.getOriginalLSN();
            //LSN=0 (bulkload) does not need to be updated and there is no flush log corresponding to it
            if (remoteLSN == 0) {
                //since this is the first LSM component of this index,
                //then set the mapping in the LSN_MAP to the current log LSN because
                //no other log could've been received for this index since bulkload replication is synchronous.
                lsmCompProp.setReplicaLSN(logManager.getAppendLSN());
                return;
            }

            if (lsmCompProp.getReplicaLSN() == null) {
                if (lsmCompProp.getOpType() == LSMOperationType.FLUSH) {
                    //need to look up LSN mapping from memory
                    RemoteLogMapping remoteLogMap = getRemoteLogMapping(lsmCompProp.getNodeId(), remoteLSN);

                    //wait until flush log arrives
                    while (remoteLogMap == null) {
                        synchronized (localLSN2RemoteLSNMap) {
                            localLSN2RemoteLSNMap.wait();
                        }
                        remoteLogMap = getRemoteLogMapping(lsmCompProp.getNodeId(), remoteLSN);
                    }

                    //wait until the log is flushed locally before updating the disk component LSN
                    synchronized (remoteLogMap) {
                        while (!remoteLogMap.isFlushed()) {
                            remoteLogMap.wait();
                        }
                    }

                    lsmCompProp.setReplicaLSN(remoteLogMap.getLocalLSN());
                } else if (lsmCompProp.getOpType() == LSMOperationType.MERGE) {
                    //need to load the LSN mapping from disk
                    Map<Long, Long> lsmMap = replicaResourcesManager
                            .getReplicaIndexLSNMap(lsmCompProp.getReplicaComponentPath(replicaResourcesManager));
                    Long mappingLSN = lsmMap.get(lsmCompProp.getOriginalLSN());
                    if (mappingLSN == null) {
                        /*
                         * this shouldn't happen unless this node just recovered and the first component it received
                         * is a merged component due to an on-going merge operation while recovery on the remote replica.
                         * In this case, we use the current append LSN since no new records exist for this index,
                         * otherwise they would've been flushed.
                         * This could be prevented by waiting for any IO to finish on the remote replica during recovery.
                         *
                         */
                        mappingLSN = logManager.getAppendLSN();
                    } else {
                        lsmCompProp.setReplicaLSN(mappingLSN);
                    }
                }
            }

            Path path = Paths.get(syncTask.getComponentFilePath());
            if (Files.notExists(path)) {
                /*
                 * This could happen when a merged component arrives and deletes the flushed
                 * component (which we are trying to update) before its flush log arrives since logs and components are received
                 * on different threads.
                 */
                return;
            }

            File destFile = new File(syncTask.getComponentFilePath());
            //prepare local LSN buffer
            ByteBuffer metadataBuffer = ByteBuffer.allocate(Long.BYTES);
            metadataBuffer.putLong(lsmCompProp.getReplicaLSN());
            metadataBuffer.flip();

            //replace the remote LSN value by the local one
            try (RandomAccessFile fileOutputStream = new RandomAccessFile(destFile, "rw");
                    FileChannel fileChannel = fileOutputStream.getChannel()) {
                long lsnStartOffset = syncTask.getLSNByteOffset();
                while (metadataBuffer.hasRemaining()) {
                    lsnStartOffset += fileChannel.write(metadataBuffer, lsnStartOffset);
                }
                fileChannel.force(true);
            }
        }
    }
}
