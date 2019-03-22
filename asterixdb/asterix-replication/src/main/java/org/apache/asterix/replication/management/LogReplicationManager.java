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
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.common.replication.IReplicationDestination;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.replication.api.ReplicationDestination;
import org.apache.asterix.replication.logging.ReplicationLogBuffer;
import org.apache.asterix.replication.logging.TxnAckTracker;
import org.apache.asterix.replication.logging.TxnLogReplicator;
import org.apache.asterix.replication.messaging.ReplicateLogsTask;
import org.apache.asterix.replication.messaging.ReplicationProtocol;
import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LogReplicationManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private final LinkedBlockingQueue<ReplicationLogBuffer> emptyLogBuffersQ;
    private final LinkedBlockingQueue<ReplicationLogBuffer> pendingFlushLogBuffersQ;
    private final ByteBuffer txnLogsBatchSizeBuffer = ByteBuffer.allocate(Integer.BYTES);
    private final Map<ReplicationDestination, ISocketChannel> destinations = new HashMap<>();
    private final IReplicationManager replicationManager;
    private final Executor executor;
    private final TxnAckTracker ackTracker = new TxnAckTracker();
    private final Set<ISocketChannel> failedSockets = new HashSet<>();
    private final Object transferLock = new Object();
    private final INcApplicationContext appCtx;
    private final int logPageSize;
    private final int logBatchSize;
    private ReplicationLogBuffer currentTxnLogBuffer;
    private ISocketChannel[] destSockets;

    public LogReplicationManager(INcApplicationContext appCtx, IReplicationManager replicationManager) {
        this.appCtx = appCtx;
        this.replicationManager = replicationManager;
        final ReplicationProperties replicationProperties = appCtx.getReplicationProperties();
        logPageSize = replicationProperties.getLogBufferPageSize();
        logBatchSize = replicationProperties.getLogBatchSize();
        executor = appCtx.getThreadExecutor();
        emptyLogBuffersQ = new LinkedBlockingQueue<>();
        pendingFlushLogBuffersQ = new LinkedBlockingQueue<>();
        initBuffers(replicationProperties.getLogBufferNumOfPages());
        TxnLogReplicator txnlogReplicator = new TxnLogReplicator(emptyLogBuffersQ, pendingFlushLogBuffersQ);
        ((ExecutorService) executor).submit(txnlogReplicator);
    }

    private void initBuffers(int buffers) {
        for (int i = 0; i < buffers; i++) {
            emptyLogBuffersQ.add(new ReplicationLogBuffer(this, logPageSize, logBatchSize));
        }
        try {
            getAndInitNewPage();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ReplicationException(e);
        }
    }

    public void register(ReplicationDestination dest) {
        synchronized (transferLock) {
            synchronized (destinations) {
                if (destinations.containsKey(dest)) {
                    return;
                }
                LOGGER.info(() -> "register " + dest);
                ISocketChannel socketChannel = dest.getLogReplicationChannel(appCtx);
                handshake(dest, socketChannel);
                destinations.put(dest, socketChannel);
                failedSockets.remove(socketChannel);
                destSockets = destinations.values().toArray(new ISocketChannel[0]);
            }
        }
    }

    public void unregister(IReplicationDestination dest) {
        synchronized (transferLock) {
            synchronized (destinations) {
                if (!destinations.containsKey(dest)) {
                    return;
                }
                LOGGER.info(() -> "unregister " + dest);
                ackTracker.unregister(dest);
                ISocketChannel destSocket = destinations.remove(dest);
                failedSockets.remove(destSocket);
                destSockets = destinations.values().toArray(new ISocketChannel[0]);
                endReplication(destSocket);
            }
        }
    }

    public void replicate(ILogRecord logRecord) throws InterruptedException {
        if (logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT) {
            synchronized (destinations) {
                ackTracker.track(logRecord, new HashSet<>(destinations.keySet()));
            }
        }
        appendToLogBuffer(logRecord);
    }

    public void transferBatch(final ByteBuffer buffer) {
        // prepare the batch size buffer
        txnLogsBatchSizeBuffer.clear();
        txnLogsBatchSizeBuffer.putInt(buffer.remaining());
        txnLogsBatchSizeBuffer.flip();

        buffer.mark();
        synchronized (transferLock) {
            if (destSockets != null) {
                for (ISocketChannel replicaSocket : destSockets) {
                    try {
                        // send batch size then the batch itself
                        NetworkingUtil.transferBufferToChannel(replicaSocket, txnLogsBatchSizeBuffer);
                        NetworkingUtil.transferBufferToChannel(replicaSocket, buffer);
                    } catch (IOException e) {
                        handleFailure(replicaSocket, e);
                    } finally {
                        txnLogsBatchSizeBuffer.position(0);
                        buffer.reset();
                    }
                }
            }
        }
        // move the buffer position to the sent limit
        buffer.position(buffer.limit());
    }

    public int getLogPageSize() {
        return logPageSize;
    }

    private synchronized void appendToLogBuffer(ILogRecord logRecord) throws InterruptedException {
        if (!currentTxnLogBuffer.hasSpace(logRecord)) {
            currentTxnLogBuffer.setFull(true);
            if (logRecord.getLogSize() > logPageSize) {
                getAndInitNewLargePage(logRecord.getLogSize());
            } else {
                getAndInitNewPage();
            }
        }
        currentTxnLogBuffer.append(logRecord);
    }

    private void getAndInitNewPage() throws InterruptedException {
        currentTxnLogBuffer = null;
        while (currentTxnLogBuffer == null) {
            currentTxnLogBuffer = emptyLogBuffersQ.take();
        }
        currentTxnLogBuffer.reset();
        pendingFlushLogBuffersQ.add(currentTxnLogBuffer);
    }

    private void getAndInitNewLargePage(int pageSize) {
        // for now, alloc a new buffer for each large page
        currentTxnLogBuffer = new ReplicationLogBuffer(this, pageSize, logBatchSize);
        pendingFlushLogBuffersQ.add(currentTxnLogBuffer);
    }

    private void handshake(ReplicationDestination dest, ISocketChannel socketChannel) {
        final String nodeId = appCtx.getServiceContext().getNodeId();
        final ReplicateLogsTask task = new ReplicateLogsTask(nodeId);
        ReplicationProtocol.sendTo(socketChannel, task, null);
        executor.execute(new TxnAckListener(dest, socketChannel));
    }

    private void endReplication(ISocketChannel socketChannel) {
        if (socketChannel.getSocketChannel().isConnected()) {
            // end log replication (by sending a dummy log with a single byte)
            final ByteBuffer endLogRepBuffer = ReplicationProtocol.getEndLogReplicationBuffer();
            try {
                NetworkingUtil.transferBufferToChannel(socketChannel, endLogRepBuffer);
            } catch (IOException e) {
                LOGGER.warn("Failed to end txn log", e);
            }
        }
    }

    private synchronized void handleFailure(ISocketChannel replicaSocket, IOException e) {
        if (failedSockets.contains(replicaSocket)) {
            return;
        }
        LOGGER.error("Replica failed", e);
        failedSockets.add(replicaSocket);
        Optional<ReplicationDestination> socketDest = destinations.entrySet().stream()
                .filter(entry -> entry.getValue().equals(replicaSocket)).map(Map.Entry::getKey).findFirst();
        socketDest.ifPresent(dest -> replicationManager.notifyFailure(dest, e));
    }

    private class TxnAckListener implements Runnable {
        private final ReplicationDestination dest;
        private final ISocketChannel replicaSocket;

        TxnAckListener(ReplicationDestination dest, ISocketChannel replicaSocket) {
            this.dest = dest;
            this.replicaSocket = replicaSocket;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("TxnAckListener (" + dest + ")");
            LOGGER.info("Started listening on socket: {}", dest);
            try (BufferedReader incomingResponse = new BufferedReader(
                    new InputStreamReader(replicaSocket.getSocketChannel().socket().getInputStream()))) {
                while (true) {
                    final String response = incomingResponse.readLine();
                    if (response == null) {
                        handleFailure(replicaSocket, new IOException("Unexpected response from replica " + dest));
                        break;
                    }
                    // read ACK
                    final int txnId = ReplicationProtocol.getTxnIdFromLogAckMessage(response);
                    ackTracker.ack(txnId, dest);
                }
            } catch (AsynchronousCloseException e) {
                LOGGER.debug(() -> "Stopped listening on socket:" + dest, e);
            } catch (IOException e) {
                handleFailure(replicaSocket, e);
            }
        }
    }
}
