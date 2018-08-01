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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.replication.api.IReplicaTask;
import org.apache.asterix.replication.api.IReplicationWorker;
import org.apache.asterix.replication.logging.RemoteLogsProcessor;
import org.apache.asterix.replication.messaging.ReplicationProtocol;
import org.apache.asterix.replication.messaging.ReplicationProtocol.ReplicationRequestType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class is used to receive and process replication requests from remote replicas or replica events from CC
 */
public class ReplicationChannel extends Thread implements IReplicationChannel {

    private static final Logger LOGGER = LogManager.getLogger();
    private ServerSocketChannel serverSocketChannel = null;
    private final INcApplicationContext appCtx;
    private final RemoteLogsProcessor logsProcessor;

    public ReplicationChannel(INcApplicationContext appCtx) {
        this.appCtx = appCtx;
        logsProcessor = new RemoteLogsProcessor(appCtx);
    }

    @Override
    public void run() {
        final String nodeId = appCtx.getServiceContext().getNodeId();
        Thread.currentThread().setName(nodeId + " Replication Channel Thread");
        final ReplicationProperties replicationProperties = appCtx.getReplicationProperties();
        final String nodeIP = replicationProperties.getReplicationAddress();
        final int dataPort = replicationProperties.getReplicationPort();
        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(true);
            InetSocketAddress replicationChannelAddress =
                    new InetSocketAddress(InetAddress.getByName(nodeIP), dataPort);
            serverSocketChannel.socket().bind(replicationChannelAddress);
            LOGGER.log(Level.INFO, "opened Replication Channel @ IP Address: " + nodeIP + ":" + dataPort);
            while (serverSocketChannel.isOpen()) {
                SocketChannel socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(true);
                //start a new thread to handle the request
                appCtx.getThreadExecutor().execute(new ReplicationWorker(socketChannel));
            }
        } catch (AsynchronousCloseException e) {
            LOGGER.debug("Replication channel closed", e);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to bind replication channel @ " + nodeIP + ":" + dataPort, e);
        }
    }

    public RemoteLogsProcessor getRemoteLogsProcessor() {
        return logsProcessor;
    }

    @Override
    public void close() throws IOException {
        if (serverSocketChannel != null) {
            serverSocketChannel.close();
            LOGGER.info("Replication channel closed.");
        }
    }

    private class ReplicationWorker implements IReplicationWorker {
        private final SocketChannel socketChannel;
        private final ByteBuffer inBuffer;
        private final ByteBuffer outBuffer;

        public ReplicationWorker(SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
            inBuffer = ByteBuffer.allocate(ReplicationProtocol.INITIAL_BUFFER_SIZE);
            outBuffer = ByteBuffer.allocate(ReplicationProtocol.INITIAL_BUFFER_SIZE);
        }

        @Override
        public void run() {
            Thread.currentThread().setName("Replication Worker");
            try {
                ReplicationRequestType requestType = ReplicationProtocol.getRequestType(socketChannel, inBuffer);
                while (requestType != ReplicationRequestType.GOODBYE) {
                    handle(requestType);
                    requestType = ReplicationProtocol.getRequestType(socketChannel, inBuffer);
                }
            } catch (Exception e) {
                LOGGER.warn("Unexpected error during replication.", e);
            } finally {
                if (socketChannel.isOpen()) {
                    try {
                        socketChannel.close();
                    } catch (IOException e) {
                        LOGGER.warn("Failed to close replication socket.", e);
                    }
                }
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

        private void handle(ReplicationRequestType requestType) throws HyracksDataException {
            final IReplicaTask task =
                    (IReplicaTask) ReplicationProtocol.readMessage(requestType, socketChannel, inBuffer);
            task.perform(appCtx, this);
        }
    }
}
