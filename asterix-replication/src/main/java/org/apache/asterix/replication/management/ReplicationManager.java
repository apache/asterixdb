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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.dataflow.AsterixLSMIndexUtil;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.replication.AsterixReplicationJob;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.replication.Replica;
import org.apache.asterix.common.replication.Replica.ReplicaState;
import org.apache.asterix.common.replication.ReplicaEvent;
import org.apache.asterix.common.replication.ReplicaEvent.ReplicaEventType;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.replication.functions.AsterixReplicationProtocol;
import org.apache.asterix.replication.functions.AsterixReplicationProtocol.ReplicationRequestType;
import org.apache.asterix.replication.functions.ReplicaFilesRequest;
import org.apache.asterix.replication.functions.ReplicaIndexFlushRequest;
import org.apache.asterix.replication.functions.ReplicaLogsRequest;
import org.apache.asterix.replication.logging.ReplicationLogBuffer;
import org.apache.asterix.replication.logging.ReplicationLogFlusher;
import org.apache.asterix.replication.storage.AsterixLSMIndexFileProperties;
import org.apache.asterix.replication.storage.LSMComponentProperties;
import org.apache.asterix.replication.storage.ReplicaResourcesManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.replication.IReplicationJob;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationExecutionType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationJobType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.storage.am.common.api.IMetaDataPageManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexReplicationJob;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;

/**
 * This class is used to process replication jobs and maintain remote replicas states
 */
public class ReplicationManager implements IReplicationManager {

    private static final Logger LOGGER = Logger.getLogger(ReplicationManager.class.getName());
    private final int INITIAL_REPLICATION_FACTOR = 1;
    private final String nodeId;
    private ExecutorService replicationListenerThreads;
    private final Map<Integer, Set<String>> jobCommitAcks;
    private final Map<Integer, ILogRecord> replicationJobsPendingAcks;
    private ByteBuffer dataBuffer;

    private final LinkedBlockingQueue<IReplicationJob> replicationJobsQ;
    private final LinkedBlockingQueue<ReplicaEvent> replicaEventsQ;

    private int replicationFactor = 1;
    private final ReplicaResourcesManager replicaResourcesManager;
    private final ILogManager logManager;
    private final IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider;
    private final AsterixReplicationProperties replicationProperties;
    private final Map<String, Replica> replicas;

    private final AtomicBoolean replicationSuspended;
    private AtomicBoolean terminateJobsReplication;
    private AtomicBoolean jobsReplicationSuspended;
    private final static int INITIAL_BUFFER_SIZE = 4000; //4KB
    private final Set<String> shuttingDownReplicaIds;
    //replication threads
    private ReplicationJobsProccessor replicationJobsProcessor;
    private final ReplicasEventsMonitor replicationMonitor;
    //dummy job used to stop ReplicationJobsProccessor thread.
    private static final IReplicationJob replicationJobPoisonPill = new AsterixReplicationJob(
            ReplicationJobType.METADATA, ReplicationOperation.STOP, ReplicationExecutionType.ASYNC, null);
    //used to identify the correct IP address when the node has multiple network interfaces
    private String hostIPAddressFirstOctet = null;

    private LinkedBlockingQueue<ReplicationLogBuffer> emptyLogBuffersQ;
    private LinkedBlockingQueue<ReplicationLogBuffer> pendingFlushLogBuffersQ;
    protected ReplicationLogBuffer currentTxnLogBuffer;
    private ReplicationLogFlusher txnlogsReplicator;
    private Future<? extends Object> txnLogReplicatorTask;
    private Map<String, SocketChannel> logsReplicaSockets = null;

    public ReplicationManager(String nodeId, AsterixReplicationProperties replicationProperties,
            IReplicaResourcesManager remoteResoucesManager, ILogManager logManager,
            IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider) {
        this.nodeId = nodeId;
        this.replicationProperties = replicationProperties;
        this.replicaResourcesManager = (ReplicaResourcesManager) remoteResoucesManager;
        this.asterixAppRuntimeContextProvider = asterixAppRuntimeContextProvider;
        this.hostIPAddressFirstOctet = replicationProperties.getReplicaIPAddress(nodeId).substring(0, 3);
        this.logManager = logManager;
        replicationJobsQ = new LinkedBlockingQueue<IReplicationJob>();
        replicaEventsQ = new LinkedBlockingQueue<ReplicaEvent>();
        terminateJobsReplication = new AtomicBoolean(false);
        jobsReplicationSuspended = new AtomicBoolean(true);
        replicationSuspended = new AtomicBoolean(true);

        replicas = new HashMap<String, Replica>();
        jobCommitAcks = new ConcurrentHashMap<Integer, Set<String>>();
        replicationJobsPendingAcks = new ConcurrentHashMap<Integer, ILogRecord>();
        shuttingDownReplicaIds = new HashSet<String>();
        dataBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);

        //Used as async listeners from replicas
        replicationListenerThreads = Executors.newCachedThreadPool();
        replicationJobsProcessor = new ReplicationJobsProccessor();
        replicationMonitor = new ReplicasEventsMonitor();

        //add list of replicas from configurations (To be read from another source e.g. Zookeeper)
        Set<Replica> replicaNodes = replicationProperties.getRemoteReplicas(nodeId);
        if (replicaNodes != null) {
            for (Replica replica : replicaNodes) {
                replicas.put(replica.getNode().getId(), replica);
            }
        }
        int numLogBuffers = logManager.getNumLogPages();
        emptyLogBuffersQ = new LinkedBlockingQueue<ReplicationLogBuffer>(numLogBuffers);
        pendingFlushLogBuffersQ = new LinkedBlockingQueue<ReplicationLogBuffer>(numLogBuffers);

        int logBufferSize = logManager.getLogPageSize();
        for (int i = 0; i < numLogBuffers; i++) {
            emptyLogBuffersQ.offer(new ReplicationLogBuffer(this, logBufferSize));
        }
    }

    /**
     * Accepts a replication job. If the job execution type is ASYNC, it is queued.
     * Otherwise, it is processed immediately.
     */
    @Override
    public void submitJob(IReplicationJob job) throws IOException {
        if (job.getExecutionType() == ReplicationExecutionType.ASYNC) {
            replicationJobsQ.offer(job);
        } else {
            //wait until replication is resumed
            while (replicationSuspended.get()) {
                synchronized (replicationSuspended) {
                    try {
                        replicationSuspended.wait();
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
            }
            processJob(job, null, null);
        }
    }

    @Override
    public void replicateLog(ILogRecord logRecord) {
        if (logRecord.getLogType() == LogType.JOB_COMMIT) {
            //if replication is suspended, wait until it is resumed.
            while (replicationSuspended.get()) {
                synchronized (replicationSuspended) {
                    try {
                        replicationSuspended.wait();
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
            }
            Set<String> replicaIds = Collections.synchronizedSet(new HashSet<String>());
            replicaIds.add(nodeId);
            jobCommitAcks.put(logRecord.getJobId(), replicaIds);
        }

        appendToLogBuffer(logRecord);
    }

    protected void getAndInitNewPage() {
        currentTxnLogBuffer = null;
        while (currentTxnLogBuffer == null) {
            try {
                currentTxnLogBuffer = emptyLogBuffersQ.take();
            } catch (InterruptedException e) {
                //ignore
            }
        }
        currentTxnLogBuffer.reset();
        currentTxnLogBuffer.setReplicationSockets(logsReplicaSockets);
        pendingFlushLogBuffersQ.offer(currentTxnLogBuffer);
    }

    private synchronized void appendToLogBuffer(ILogRecord logRecord) {
        if (!currentTxnLogBuffer.hasSpace(logRecord)) {
            currentTxnLogBuffer.isFull(true);
            getAndInitNewPage();
        }

        currentTxnLogBuffer.append(logRecord);
    }

    /**
     * Processes the replication job based on its specifications
     *
     * @param job
     *            The replication job
     * @param replicasSockets
     *            The remote replicas sockets to send the request to.
     * @param requestBuffer
     *            The buffer to use to send the request.
     * @throws IOException
     */
    private void processJob(IReplicationJob job, Map<String, SocketChannel> replicasSockets, ByteBuffer requestBuffer)
            throws IOException {
        boolean isLSMComponentFile;
        ByteBuffer responseBuffer = null;
        AsterixLSMIndexFileProperties asterixFileProperties = new AsterixLSMIndexFileProperties();
        if (requestBuffer == null) {
            requestBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
        }

        isLSMComponentFile = job.getJobType() == ReplicationJobType.LSM_COMPONENT ? true : false;
        try {
            //if there isn't already a connection, establish a new one
            if (replicasSockets == null) {
                replicasSockets = getActiveRemoteReplicasSockets();
            }

            int remainingFiles = job.getJobFiles().size();

            if (job.getOperation() == ReplicationOperation.REPLICATE) {
                try {
                    //if the replication job is an LSM_COMPONENT, its properties are sent first, then its files.
                    ILSMIndexReplicationJob LSMComponentJob = null;
                    if (job.getJobType() == ReplicationJobType.LSM_COMPONENT) {
                        //send LSMComponent properties
                        LSMComponentJob = (ILSMIndexReplicationJob) job;
                        LSMComponentProperties lsmCompProp = new LSMComponentProperties(LSMComponentJob, nodeId);
                        requestBuffer = AsterixReplicationProtocol.writeLSMComponentPropertiesRequest(lsmCompProp,
                                requestBuffer);
                        sendRequest(replicasSockets, requestBuffer);
                    }

                    for (String filePath : job.getJobFiles()) {
                        remainingFiles--;
                        Path path = Paths.get(filePath);
                        if (Files.notExists(path)) {
                            LOGGER.log(Level.SEVERE, "File deleted before replication: " + filePath);
                            continue;
                        }

                        LOGGER.log(Level.INFO, "Replicating file: " + filePath);
                        //open file for reading
                        try (RandomAccessFile fromFile = new RandomAccessFile(filePath, "r");
                                FileChannel fileChannel = fromFile.getChannel();) {

                            long fileSize = fileChannel.size();

                            if (LSMComponentJob != null) {
                                //since this is LSM_COMPONENT REPLICATE job, the job will contain only the component being replicated.
                                ILSMComponent diskComponent = LSMComponentJob.getLSMIndexOperationContext()
                                        .getComponentsToBeReplicated().get(0);
                                long LSNByteOffset = AsterixLSMIndexUtil.getComponentFileLSNOffset(
                                        (AbstractLSMIndex) LSMComponentJob.getLSMIndex(), diskComponent, filePath);
                                asterixFileProperties.initialize(filePath, fileSize, nodeId, isLSMComponentFile,
                                        LSNByteOffset, remainingFiles == 0);
                            } else {
                                asterixFileProperties.initialize(filePath, fileSize, nodeId, isLSMComponentFile,
                                        IMetaDataPageManager.INVALID_LSN_OFFSET, remainingFiles == 0);
                            }

                            requestBuffer = AsterixReplicationProtocol.writeFileReplicationRequest(requestBuffer,
                                    asterixFileProperties, ReplicationRequestType.REPLICATE_FILE);

                            Iterator<Map.Entry<String, SocketChannel>> iterator = replicasSockets.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<String, SocketChannel> entry = iterator.next();
                                SocketChannel socketChannel = entry.getValue();
                                //transfer request header & file
                                try {
                                    NetworkingUtil.transferBufferToChannel(socketChannel, requestBuffer);
                                    NetworkingUtil.sendFile(fileChannel, socketChannel);
                                    if (asterixFileProperties.requiresAck()) {
                                        ReplicationRequestType responseType = waitForResponse(socketChannel,
                                                responseBuffer);
                                        if (responseType != ReplicationRequestType.ACK) {
                                            throw new IOException(
                                                    "Could not receive ACK from replica " + entry.getKey());
                                        }
                                    }
                                } catch (IOException e) {
                                    reportFailedReplica(entry.getKey());
                                    iterator.remove();
                                } finally {
                                    requestBuffer.position(0);
                                }
                            }
                        }
                    }
                } finally {
                    if (job instanceof ILSMIndexReplicationJob) {
                        //exit the replicated LSM components
                        ILSMIndexReplicationJob aJob = (ILSMIndexReplicationJob) job;
                        aJob.endReplication();
                    }
                }
            } else if (job.getOperation() == ReplicationOperation.DELETE) {
                for (String filePath : job.getJobFiles()) {
                    remainingFiles--;
                    asterixFileProperties.initialize(filePath, -1, nodeId, isLSMComponentFile,
                            IMetaDataPageManager.INVALID_LSN_OFFSET, remainingFiles == 0);
                    AsterixReplicationProtocol.writeFileReplicationRequest(requestBuffer, asterixFileProperties,
                            ReplicationRequestType.DELETE_FILE);

                    Iterator<Map.Entry<String, SocketChannel>> iterator = replicasSockets.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, SocketChannel> entry = iterator.next();
                        SocketChannel socketChannel = entry.getValue();
                        try {
                            sendRequest(replicasSockets, requestBuffer);
                            if (asterixFileProperties.requiresAck()) {
                                waitForResponse(socketChannel, responseBuffer);
                            }
                        } catch (IOException e) {
                            reportFailedReplica(entry.getKey());
                            iterator.remove();
                        } finally {
                            requestBuffer.position(0);
                        }
                    }
                }
            }
        } finally {
            //if sync, close sockets with replicas since they wont be reused
            if (job.getExecutionType() == ReplicationExecutionType.SYNC) {
                closeReplicaSockets(replicasSockets);
            }
        }
    }

    /**
     * Waits and reads a response from a remote replica
     *
     * @param socketChannel
     *            The socket to read the response from
     * @param responseBuffer
     *            The response buffer to read the response to.
     * @return The response type.
     * @throws IOException
     */
    private static ReplicationRequestType waitForResponse(SocketChannel socketChannel, ByteBuffer responseBuffer)
            throws IOException {
        if (responseBuffer == null) {
            responseBuffer = ByteBuffer.allocate(AsterixReplicationProtocol.REPLICATION_REQUEST_TYPE_SIZE);
        } else {
            responseBuffer.clear();
        }

        //read response from remote replicas
        ReplicationRequestType responseFunction = AsterixReplicationProtocol.getRequestType(socketChannel,
                responseBuffer);
        return responseFunction;
    }

    @Override
    public boolean isReplicationEnabled() {
        return replicationProperties.isReplicationEnabled();
    }

    @Override
    public synchronized void updateReplicaInfo(Replica replicaNode) {
        Replica replica = replicas.get(replicaNode.getNode().getId());
        //should not update the info of an active replica
        if (replica.getState() == ReplicaState.ACTIVE) {
            return;
        }

        replica.getNode().setClusterIp(replicaNode.getNode().getClusterIp());

        /*
         * This could be used to reconnect to replica without needing the Cluster notifications
        if (replica.getState() == ReplicaState.DEAD) {
            reportFailedReplica(replica.getNode().getId());
        } else if (replica.getState() == ReplicaState.ACTIVE) {
            checkReplicaState(replica.getNode().getId(), true);
        }
        */
    }

    /**
     * Suspends proccessing replication jobs.
     *
     * @param force
     *            a flag indicates if replication should be suspended right away or when the pending jobs are completed.
     */
    private void suspendReplication(boolean force) {
        //suspend replication jobs processing
        if (replicationJobsProcessor != null && replicationJobsProcessor.isAlive()) {
            if (force) {
                terminateJobsReplication.set(true);
            }
            replicationJobsQ.offer(replicationJobPoisonPill);

            //wait until the jobs are suspended
            synchronized (jobsReplicationSuspended) {
                while (!jobsReplicationSuspended.get()) {
                    try {
                        jobsReplicationSuspended.wait();
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
            }
        }

        //suspend logs replication
        if (txnlogsReplicator != null) {
            terminateTxnLogsReplicator();
        }
    }

    /**
     * Opens a new connection with Active remote replicas and starts a listen thread per connection.
     */
    private void establishTxnLogsReplicationConnection() {
        logsReplicaSockets = getActiveRemoteReplicasSockets();
        //start a listener thread per connection
        for (Entry<String, SocketChannel> entry : logsReplicaSockets.entrySet()) {
            replicationListenerThreads
                    .execute(new TxnLogsReplicationResponseListener(entry.getKey(), entry.getValue()));
        }
    }

    /**
     * Stops ReplicationFlusherThread and closes the sockets used to replicate logs.
     */
    private void terminateTxnLogsReplicator() {
        LOGGER.log(Level.INFO, "Terminating ReplicationLogFlusher thread ...");
        txnlogsReplicator.terminate();
        try {
            txnLogReplicatorTask.get();
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.log(Level.WARNING, "RepicationLogFlusher thread terminated abnormally");
            e.printStackTrace();
        }
        LOGGER.log(Level.INFO, "LogFlusher thread is terminated.");

        if (logsReplicaSockets != null) {
            //wait for any ACK to arrive before closing sockets.
            synchronized (jobCommitAcks) {
                while (jobCommitAcks.size() != 0) {
                    try {
                        jobCommitAcks.wait();
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
            }

            //close log replication sockets
            closeReplicaSockets(logsReplicaSockets);
            logsReplicaSockets = null;
        }
    }

    @Override
    public void broadcastNewIPAddress() throws IOException {
        String orignalIPAddress = replicationProperties.getReplicaIPAddress(nodeId);
        String newAddress = NetworkingUtil.getHostAddress(hostIPAddressFirstOctet);

        //IP Address didn't change after failure
        if (orignalIPAddress.equals(newAddress)) {
            return;
        }

        Node node = new Node();
        node.setId(nodeId);
        node.setClusterIp(newAddress);
        Replica replica = new Replica(node);

        ByteBuffer buffer = AsterixReplicationProtocol.writeUpdateReplicaRequest(replica);
        Map<String, SocketChannel> replicaSockets = getActiveRemoteReplicasSockets();
        sendRequest(replicaSockets, buffer);
        closeReplicaSockets(replicaSockets);
    }

    /**
     * Sends a shutdown event to remote replicas notifying them
     * no more logs/files will be sent from this local replica.
     *
     * @throws IOException
     */
    private void sendShutdownNotifiction() throws IOException {
        Node node = new Node();
        node.setId(nodeId);
        node.setClusterIp(NetworkingUtil.getHostAddress(hostIPAddressFirstOctet));
        Replica replica = new Replica(node);
        ReplicaEvent event = new ReplicaEvent(replica, ReplicaEventType.SHUTDOWN);
        ByteBuffer buffer = AsterixReplicationProtocol.writeReplicaEventRequest(event);
        Map<String, SocketChannel> replicaSockets = getActiveRemoteReplicasSockets();
        sendRequest(replicaSockets, buffer);
        closeReplicaSockets(replicaSockets);
    }

    /**
     * Sends a request to remote replicas
     *
     * @param replicaSockets
     *            The sockets to send the request to.
     * @param requestBuffer
     *            The buffer that contains the request.
     */
    private void sendRequest(Map<String, SocketChannel> replicaSockets, ByteBuffer requestBuffer) {
        Iterator<Map.Entry<String, SocketChannel>> iterator = replicaSockets.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, SocketChannel> replicaSocket = iterator.next();
            SocketChannel clientSocket = replicaSocket.getValue();
            try {
                NetworkingUtil.transferBufferToChannel(clientSocket, requestBuffer);
            } catch (IOException e) {
                if (clientSocket.isOpen()) {
                    try {
                        clientSocket.close();
                    } catch (IOException e2) {
                        e2.printStackTrace();
                    }
                }
                reportFailedReplica(replicaSocket.getKey());
                iterator.remove();
            } finally {
                requestBuffer.position(0);
            }
        }
    }

    /**
     * Closes the passed replication sockets by sending GOODBYE request to remote replicas.
     *
     * @param replicaSockets
     */
    private void closeReplicaSockets(Map<String, SocketChannel> replicaSockets) {
        //send goodbye
        ByteBuffer goodbyeBuffer = AsterixReplicationProtocol.getGoodbyeBuffer();
        sendRequest(replicaSockets, goodbyeBuffer);

        Iterator<Map.Entry<String, SocketChannel>> iterator = replicaSockets.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, SocketChannel> replicaSocket = iterator.next();
            SocketChannel clientSocket = replicaSocket.getValue();
            if (clientSocket.isOpen()) {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void initializeReplicasState() {
        for (Replica replica : replicas.values()) {
            checkReplicaState(replica.getNode().getId(), false, false);
        }
    }

    /**
     * Checks the state of a remote replica by trying to ping it.
     *
     * @param replicaId
     *            The replica to check the state for.
     * @param async
     *            a flag indicating whether to wait for the result or not.
     * @param suspendReplication
     *            a flag indicating whether to suspend replication on replica state change or not.
     */
    private void checkReplicaState(String replicaId, boolean async, boolean suspendReplication) {
        Replica replica = replicas.get(replicaId);

        ReplicaStateChecker connector = new ReplicaStateChecker(replica, replicationProperties.getReplicationTimeOut(),
                this, replicationProperties, suspendReplication);
        Future<? extends Object> ft = asterixAppRuntimeContextProvider.getThreadExecutor().submit(connector);

        if (!async) {
            //wait until task is done
            while (!ft.isDone()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Updates the state of a remote replica.
     *
     * @param replicaId
     *            The replica id to update.
     * @param newState
     *            The new state of the replica.
     * @param suspendReplication
     *            a flag indicating whether to suspend replication on state change or not.
     */
    public synchronized void updateReplicaState(String replicaId, ReplicaState newState, boolean suspendReplication) {
        Replica replica = replicas.get(replicaId);

        if (replica.getState() == newState) {
            return;
        }

        if (suspendReplication) {
            //prevent new jobs/logs from coming in
            replicationSuspended.set(true);

            if (newState == ReplicaState.DEAD) {
                //assume the dead replica ACK has been received for all pending jobs
                synchronized (jobCommitAcks) {
                    for (Integer jobId : jobCommitAcks.keySet()) {
                        addAckToJob(jobId, replicaId);
                    }
                }
            }

            //force replication threads to stop in order to change the replication factor
            suspendReplication(true);
        }

        replica.setState(newState);

        if (newState == ReplicaState.ACTIVE) {
            replicationFactor++;
            //TODO Extra check: make sure newly added replica is in sync.
            //Since in the current design the whole cluster becomes UNUSABLE,
            //no new jobs could start before the failed node rejoins
        } else if (newState == ReplicaState.DEAD) {
            if (replicationFactor > INITIAL_REPLICATION_FACTOR) {
                replicationFactor--;
            }
        }

        LOGGER.log(Level.WARNING, "Replica " + replicaId + " state changed to: " + newState.name()
                + ". Replication factor changed to: " + replicationFactor);

        if (suspendReplication) {
            startReplicationThreads();
        }
    }

    /**
     * When an ACK for a JOB_COMMIT is received, it is added to the corresponding job.
     *
     * @param jobId
     * @param replicaId
     *            The remote replica id the ACK received from.
     */
    private void addAckToJob(int jobId, String replicaId) {
        //add ACK to the job
        if (jobCommitAcks.containsKey(jobId)) {
            Set<String> replicaIds = jobCommitAcks.get(jobId);
            replicaIds.add(replicaId);
        } else {
            throw new IllegalStateException("Job ID not found in pending job commits  " + jobId);
        }

        //if got ACKs from all remote replicas, notify pending jobs if any
        if (jobCommitAcks.get(jobId).size() == replicationFactor) {
            synchronized (replicationJobsPendingAcks) {
                if (replicationJobsPendingAcks.containsKey(jobId)) {
                    ILogRecord pendingLog = replicationJobsPendingAcks.get(jobId);
                    synchronized (pendingLog) {
                        pendingLog.notify();
                    }
                }
            }
        }
    }

    @Override
    public boolean hasBeenReplicated(ILogRecord logRecord) {
        if (jobCommitAcks.containsKey(logRecord.getJobId())) {
            //check if all ACKs have been received
            if (jobCommitAcks.get(logRecord.getJobId()).size() == replicationFactor) {
                jobCommitAcks.remove(logRecord.getJobId());

                if (replicationJobsPendingAcks.containsKey(logRecord.getJobId())) {
                    replicationJobsPendingAcks.remove(logRecord);
                }

                //notify any threads waiting for all jobs to finish
                if (jobCommitAcks.size() == 0) {
                    synchronized (jobCommitAcks) {
                        jobCommitAcks.notifyAll();
                    }
                }

                return true;
            } else {
                if (!replicationJobsPendingAcks.containsKey(logRecord.getJobId())) {
                    synchronized (replicationJobsPendingAcks) {
                        replicationJobsPendingAcks.put(logRecord.getJobId(), logRecord);
                    }
                }
                return false;
            }
        }

        //presume replicated
        return true;
    }

    private Map<String, SocketChannel> getActiveRemoteReplicasSockets() {
        Map<String, SocketChannel> replicaNodesSockets = new HashMap<String, SocketChannel>();
        for (Replica replica : replicas.values()) {
            if (replica.getState() == ReplicaState.ACTIVE) {
                try {
                    SocketChannel sc = getReplicaSocket(replica.getId());
                    replicaNodesSockets.put(replica.getId(), sc);
                } catch (IOException e) {
                    reportFailedReplica(replica.getId());
                }
            }
        }
        return replicaNodesSockets;
    }

    /**
     * Establishes a connection with a remote replica.
     *
     * @param replicaId
     *            The replica to connect to.
     * @return The socket of the remote replica
     * @throws IOException
     */
    private SocketChannel getReplicaSocket(String replicaId) throws IOException {
        Replica replica = replicas.get(replicaId);
        SocketChannel sc = SocketChannel.open();
        sc.configureBlocking(true);
        InetSocketAddress address = replica.getAddress(replicationProperties);
        sc.connect(new InetSocketAddress(address.getHostString(), address.getPort()));
        return sc;
    }

    @Override
    public Set<String> getDeadReplicasIds() {
        Set<String> replicasIds = new HashSet<String>();
        for (Replica replica : replicas.values()) {
            if (replica.getState() == ReplicaState.DEAD) {
                replicasIds.add(replica.getNode().getId());
            }
        }
        return replicasIds;
    }

    @Override
    public Set<String> getActiveReplicasIds() {
        Set<String> replicasIds = new HashSet<String>();
        for (Replica replica : replicas.values()) {
            if (replica.getState() == ReplicaState.ACTIVE) {
                replicasIds.add(replica.getNode().getId());
            }
        }
        return replicasIds;
    }

    @Override
    public int getActiveReplicasCount() {
        return getActiveReplicasIds().size();
    }

    @Override
    public void start() {
        //do nothing
    }

    @Override
    public void dumpState(OutputStream os) throws IOException {
        //do nothing
    }

    /**
     * Called during NC shutdown to notify remote replicas about the shutdown
     * and wait for remote replicas shutdown notification then closes the local replication channel.
     */
    @Override
    public void stop(boolean dumpState, OutputStream ouputStream) throws IOException {
        try {
            //stop replication thread afters all jobs/logs have been processed
            suspendReplication(false);
            //send shutdown event to remote replicas
            sendShutdownNotifiction();
            //wait until all shutdown events come from all remote replicas
            synchronized (shuttingDownReplicaIds) {
                while (!shuttingDownReplicaIds.containsAll(getActiveReplicasIds())) {
                    try {
                        shuttingDownReplicaIds.wait(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            LOGGER.log(Level.INFO, "Got shutdown notification from all remote replicas");
            //close replication channel
            asterixAppRuntimeContextProvider.getAppContext().getReplicationChannel().close();

            LOGGER.log(Level.INFO, "Replication manager stopped.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void reportReplicaEvent(ReplicaEvent event) {
        synchronized (replicaEventsQ) {
            replicaEventsQ.offer(event);
        }
    }

    /**
     * Suspends replications and sends a remote replica failure event to ReplicasEventsMonitor.
     *
     * @param replicaId
     *            the failed replica id.
     */
    public void reportFailedReplica(String replicaId) {
        Replica replica = replicas.get(replicaId);
        if (replica.getState() == ReplicaState.DEAD) {
            return;
        }

        //need to stop processing any new logs or jobs
        terminateJobsReplication.set(true);

        ReplicaEvent event = new ReplicaEvent(replica, ReplicaEventType.FAIL);
        reportReplicaEvent(event);
    }

    @Override
    public void startReplicationThreads() {
        replicationJobsProcessor = new ReplicationJobsProccessor();

        //start/continue processing jobs/logs
        if (logsReplicaSockets == null) {
            establishTxnLogsReplicationConnection();
            getAndInitNewPage();
            txnlogsReplicator = new ReplicationLogFlusher(emptyLogBuffersQ, pendingFlushLogBuffersQ);
            txnLogReplicatorTask = asterixAppRuntimeContextProvider.getThreadExecutor().submit(txnlogsReplicator);
        }

        replicationJobsProcessor.start();

        if (!replicationMonitor.isAlive()) {
            replicationMonitor.start();
        }

        //notify any waiting threads that replication has been resumed
        synchronized (replicationSuspended) {
            LOGGER.log(Level.INFO, "Replication started/resumed");
            replicationSuspended.set(false);
            replicationSuspended.notifyAll();
        }
    }

    @Override
    public void requestFlushLaggingReplicaIndexes(long nonSharpCheckpointTargetLSN) throws IOException {
        long startLSN = logManager.getAppendLSN();
        Set<String> replicaIds = getActiveReplicasIds();
        ByteBuffer requestBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
        for (String replicaId : replicaIds) {
            //1. identify replica indexes with LSN less than nonSharpCheckpointTargetLSN.
            HashMap<Long, String> laggingIndexes = replicaResourcesManager.getLaggingReplicaIndexesId2PathMap(replicaId,
                    nonSharpCheckpointTargetLSN);

            if (laggingIndexes.size() > 0) {
                //2. send request to remote replicas that have lagging indexes.
                ReplicaIndexFlushRequest laggingIndexesResponse = null;
                try (SocketChannel socketChannel = getReplicaSocket(replicaId)) {
                    ReplicaIndexFlushRequest laggingIndexesRequest = new ReplicaIndexFlushRequest(
                            laggingIndexes.keySet());
                    requestBuffer = AsterixReplicationProtocol.writeGetReplicaIndexFlushRequest(requestBuffer,
                            laggingIndexesRequest);
                    NetworkingUtil.transferBufferToChannel(socketChannel, requestBuffer);

                    //3. remote replicas will respond with indexes that were not flushed.
                    ReplicationRequestType responseFunction = waitForResponse(socketChannel, requestBuffer);

                    if (responseFunction == ReplicationRequestType.FLUSH_INDEX) {
                        requestBuffer = AsterixReplicationProtocol.readRequest(socketChannel, requestBuffer);
                        //returning the indexes that were not flushed
                        laggingIndexesResponse = AsterixReplicationProtocol.readReplicaIndexFlushRequest(requestBuffer);
                    }
                    //send goodbye
                    AsterixReplicationProtocol.sendGoodbye(socketChannel);
                }

                //4. update the LSN_MAP for indexes that were not flushed to the current append LSN to indicate no operations happend.
                if (laggingIndexesResponse != null) {
                    for (Long resouceId : laggingIndexesResponse.getLaggingRescouresIds()) {
                        String indexPath = laggingIndexes.get(resouceId);
                        HashMap<Long, Long> indexLSNMap = replicaResourcesManager.getReplicaIndexLSNMap(indexPath);
                        indexLSNMap.put(ReplicaResourcesManager.REPLICA_INDEX_CREATION_LSN, startLSN);
                        replicaResourcesManager.updateReplicaIndexLSNMap(indexPath, indexLSNMap);
                    }
                }
            }
        }
    }

    //Recovery Method
    @Override
    public long getMaxRemoteLSN(Set<String> remoteReplicas) throws IOException {
        long maxRemoteLSN = 0;

        AsterixReplicationProtocol.writeGetReplicaMaxLSNRequest(dataBuffer);
        Map<String, SocketChannel> replicaSockets = new HashMap<String, SocketChannel>();
        try {
            for (String replicaId : remoteReplicas) {
                replicaSockets.put(replicaId, getReplicaSocket(replicaId));
            }

            //send request
            Iterator<Map.Entry<String, SocketChannel>> iterator = replicaSockets.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, SocketChannel> replicaSocket = iterator.next();
                SocketChannel clientSocket = replicaSocket.getValue();
                NetworkingUtil.transferBufferToChannel(clientSocket, dataBuffer);
                dataBuffer.position(0);
            }

            iterator = replicaSockets.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, SocketChannel> replicaSocket = iterator.next();
                SocketChannel clientSocket = replicaSocket.getValue();
                //read response
                NetworkingUtil.readBytes(clientSocket, dataBuffer, Long.BYTES);
                maxRemoteLSN = Math.max(maxRemoteLSN, dataBuffer.getLong());
            }
        } finally {
            closeReplicaSockets(replicaSockets);
        }

        return maxRemoteLSN;
    }

    //Recovery Method
    @Override
    public void requestReplicaFiles(String selectedReplicaId, Set<String> replicasDataToRecover) throws IOException {
        ReplicaFilesRequest request = new ReplicaFilesRequest(replicasDataToRecover);
        AsterixReplicationProtocol.writeGetReplicaFilesRequest(dataBuffer, request);

        try (SocketChannel socketChannel = getReplicaSocket(selectedReplicaId)) {

            //transfer request
            NetworkingUtil.transferBufferToChannel(socketChannel, dataBuffer);

            String destFolder;
            String destFilePath;

            ReplicationRequestType responseFunction = AsterixReplicationProtocol.getRequestType(socketChannel,
                    dataBuffer);
            AsterixLSMIndexFileProperties fileProperties;
            while (responseFunction != ReplicationRequestType.GOODBYE) {
                dataBuffer = AsterixReplicationProtocol.readRequest(socketChannel, dataBuffer);

                fileProperties = AsterixReplicationProtocol.readFileReplicationRequest(dataBuffer);
                destFolder = replicaResourcesManager.getIndexPath(fileProperties.getNodeId(),
                        fileProperties.getIoDeviceNum(), fileProperties.getDataverse(), fileProperties.getIdxName());
                destFilePath = destFolder + File.separator + fileProperties.getFileName();

                //create file
                File destFile = new File(destFilePath);
                destFile.createNewFile();

                try (RandomAccessFile fileOutputStream = new RandomAccessFile(destFile, "rw");
                        FileChannel fileChannel = fileOutputStream.getChannel();) {
                    fileOutputStream.setLength(fileProperties.getFileSize());

                    NetworkingUtil.downloadFile(fileChannel, socketChannel);
                    fileChannel.force(true);
                }

                //we need to create LSN map for .metadata files that belong to remote replicas
                if (!fileProperties.isLSMComponentFile() && !fileProperties.getNodeId().equals(nodeId)) {
                    //replica index
                    replicaResourcesManager.initializeReplicaIndexLSNMap(destFolder, logManager.getAppendLSN());
                }

                responseFunction = AsterixReplicationProtocol.getRequestType(socketChannel, dataBuffer);
            }

            //send goodbye
            AsterixReplicationProtocol.sendGoodbye(socketChannel);
        }
    }

    //Recovery Method
    @Override
    public long requestReplicaMinLSN(String selectedReplicaId) throws IOException {
        long minLSN = 0;
        AsterixReplicationProtocol.writeMinLSNRequest(dataBuffer);
        try (SocketChannel socketChannel = getReplicaSocket(selectedReplicaId);) {
            //transfer request
            NetworkingUtil.transferBufferToChannel(socketChannel, dataBuffer);

            //read response
            NetworkingUtil.readBytes(socketChannel, dataBuffer, Long.BYTES);
            minLSN = dataBuffer.getLong();

            //send goodbye
            AsterixReplicationProtocol.sendGoodbye(socketChannel);
        }

        return minLSN;
    }

    //Recovery Method
    @Override
    public ArrayList<ILogRecord> requestReplicaLogs(String remoteNode, Set<String> nodeIdsToRecoverFor, long fromLSN)
            throws IOException, ACIDException {
        ReplicaLogsRequest request = new ReplicaLogsRequest(nodeIdsToRecoverFor, fromLSN);
        dataBuffer = AsterixReplicationProtocol.writeGetReplicaLogsRequest(dataBuffer, request);

        try (SocketChannel socketChannel = getReplicaSocket(remoteNode)) {
            //transfer request
            NetworkingUtil.transferBufferToChannel(socketChannel, dataBuffer);

            //read response type
            ReplicationRequestType responseType = AsterixReplicationProtocol.getRequestType(socketChannel, dataBuffer);

            ArrayList<ILogRecord> recoveryLogs = new ArrayList<ILogRecord>();
            ILogRecord logRecord = new LogRecord();
            while (responseType != ReplicationRequestType.GOODBYE) {
                dataBuffer = AsterixReplicationProtocol.readRequest(socketChannel, dataBuffer);
                logRecord.readRemoteLog(dataBuffer, true, nodeId);

                if (logRecord.getNodeId().equals(nodeId)) {
                    //store log in memory to replay it for recovery
                    recoveryLogs.add(logRecord);
                    //this needs to be a new log object so that it is passed to recovery manager as a different object
                    logRecord = new LogRecord();
                } else {
                    //send log to log manager as a remote recovery log
                    logManager.log(logRecord);
                }

                responseType = AsterixReplicationProtocol.getRequestType(socketChannel, dataBuffer);
            }

            //send goodbye
            AsterixReplicationProtocol.sendGoodbye(socketChannel);
            return recoveryLogs;
        }
    }

    //supporting classes
    /**
     * This class is responsible for processing replica events.
     */
    private class ReplicasEventsMonitor extends Thread {
        ReplicaEvent event;

        @Override
        public void run() {
            while (true) {
                try {
                    event = replicaEventsQ.take();

                    switch (event.getEventType()) {
                        case FAIL:
                            handleReplicaFailure(event.getReplica().getId());
                            break;
                        case JOIN:
                            checkReplicaState(event.getReplica().getId(), false, true);
                            break;
                        case SHUTDOWN:
                            handleShutdownEvent(event.getReplica().getId());
                            break;
                        default:
                            break;
                    }
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        }

        public void handleReplicaFailure(String replicaId) {
            Replica replica = replicas.get(replicaId);

            if (replica.getState() == ReplicaState.DEAD) {
                return;
            }

            updateReplicaState(replicaId, ReplicaState.DEAD, true);

            //delete any invalid LSMComponents for this replica
            try {
                replicaResourcesManager.cleanInvalidLSMComponents(replicaId);
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
        }

        public void handleShutdownEvent(String replicaId) {
            synchronized (shuttingDownReplicaIds) {
                shuttingDownReplicaIds.add(replicaId);
                shuttingDownReplicaIds.notifyAll();
            }
        }
    }

    /**
     * This class process is responsible for processing ASYNC replication job.
     */
    private class ReplicationJobsProccessor extends Thread {
        Map<String, SocketChannel> replicaSockets;
        ByteBuffer reusableBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);

        @Override
        public void run() {
            Thread.currentThread().setName("ReplicationJobsProccessor Thread");
            terminateJobsReplication.set(false);
            jobsReplicationSuspended.set(false);

            while (true) {
                try {
                    if (terminateJobsReplication.get()) {
                        closeSockets();
                        break;
                    }

                    IReplicationJob job = replicationJobsQ.take();
                    if (job.getOperation() != ReplicationOperation.STOP) {
                        //if there isn't already a connection, establish a new one
                        if (replicaSockets == null) {
                            replicaSockets = getActiveRemoteReplicasSockets();
                        }

                        processJob(job, replicaSockets, reusableBuffer);
                    } else {
                        terminateJobsReplication.set(true);
                        continue;
                    }

                    //if no more jobs to process, close sockets
                    if (replicationJobsQ.size() == 0) {
                        LOGGER.log(Level.INFO, "No pending replication jobs. Closing connections to replicas");
                        closeSockets();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            synchronized (jobsReplicationSuspended) {
                jobsReplicationSuspended.set(true);
                jobsReplicationSuspended.notifyAll();
            }
            LOGGER.log(Level.INFO, "ReplicationJobsProccessor stopped. ");
        }

        private void closeSockets() {
            if (replicaSockets != null) {
                closeReplicaSockets(replicaSockets);
                replicaSockets.clear();
                replicaSockets = null;
            }
        }
    }

    /**
     * This class is responsible for listening on sockets that belong to TxnLogsReplicator.
     */
    private class TxnLogsReplicationResponseListener implements Runnable {
        final SocketChannel replicaSocket;
        final String replicaId;

        public TxnLogsReplicationResponseListener(String replicaId, SocketChannel replicaSocket) {
            this.replicaId = replicaId;
            this.replicaSocket = replicaSocket;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("TxnLogs Replication Listener Thread");
            LOGGER.log(Level.INFO, "Started listening on socket: " + replicaSocket.socket().getRemoteSocketAddress());

            try {
                BufferedReader incomingResponse = new BufferedReader(
                        new InputStreamReader(replicaSocket.socket().getInputStream()));
                String responseLine = "";
                while (true) {
                    responseLine = incomingResponse.readLine();
                    if (responseLine == null) {
                        break;
                    }
                    //read ACK for job commit log
                    String replicaId = AsterixReplicationProtocol.getNodeIdFromLogAckMessage(responseLine);
                    int jobId = AsterixReplicationProtocol.getJobIdFromLogAckMessage(responseLine);
                    addAckToJob(jobId, replicaId);
                }
            } catch (AsynchronousCloseException e1) {
                LOGGER.log(Level.INFO, "Replication listener stopped for remote replica: " + replicaId);
            } catch (IOException e2) {
                reportFailedReplica(replicaId);
            }
        }
    }
}
