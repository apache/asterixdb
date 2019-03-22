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
package org.apache.asterix.replication.messaging;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.replication.api.IReplicationMessage;
import org.apache.asterix.replication.api.PartitionReplica;
import org.apache.asterix.replication.management.NetworkingUtil;
import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.hyracks.api.network.ISocketChannelFactory;
import org.apache.hyracks.data.std.util.ExtendedByteArrayOutputStream;
import org.apache.hyracks.util.NetworkUtil;
import org.apache.hyracks.util.StorageUtil;

public class ReplicationProtocol {

    /**
     * All replication messages start with ReplicationRequestType (4 bytes), then the length of the request in bytes
     */
    public static final String LOG_REPLICATION_ACK = "$";
    public static final int INITIAL_BUFFER_SIZE = StorageUtil.getIntSizeInBytes(4, StorageUtil.StorageUnit.KILOBYTE);
    private static final int REPLICATION_REQUEST_TYPE_SIZE = Integer.BYTES;
    private static final int REPLICATION_REQUEST_HEADER_SIZE = REPLICATION_REQUEST_TYPE_SIZE + Integer.BYTES;

    public enum ReplicationRequestType {
        GOODBYE,
        ACK,
        PARTITION_RESOURCES_REQUEST,
        PARTITION_RESOURCES_RESPONSE,
        REPLICATE_RESOURCE_FILE,
        DELETE_RESOURCE_FILE,
        CHECKPOINT_PARTITION,
        LSM_COMPONENT_MASK,
        MARK_COMPONENT_VALID,
        DROP_INDEX,
        REPLICATE_LOGS
    }

    private static final Map<Integer, ReplicationRequestType> TYPES = new HashMap<>();

    static {
        Stream.of(ReplicationRequestType.values()).forEach(type -> TYPES.put(type.ordinal(), type));
    }

    public static ByteBuffer readRequest(ISocketChannel socketChannel, ByteBuffer dataBuffer) throws IOException {
        // read request size
        NetworkingUtil.readBytes(socketChannel, dataBuffer, Integer.BYTES);
        final int requestSize = dataBuffer.getInt();
        final ByteBuffer buf = ensureSize(dataBuffer, requestSize);
        // read request
        NetworkingUtil.readBytes(socketChannel, buf, requestSize);
        return buf;
    }

    public static ReplicationRequestType getRequestType(ISocketChannel socketChannel, ByteBuffer byteBuffer)
            throws IOException {
        // read replication request type
        NetworkingUtil.readBytes(socketChannel, byteBuffer, REPLICATION_REQUEST_TYPE_SIZE);
        return TYPES.get(byteBuffer.getInt());
    }

    private static ByteBuffer getGoodbyeBuffer() {
        ByteBuffer bb = ByteBuffer.allocate(REPLICATION_REQUEST_TYPE_SIZE);
        bb.putInt(ReplicationRequestType.GOODBYE.ordinal());
        bb.flip();
        return bb;
    }

    public static int getTxnIdFromLogAckMessage(String msg) {
        return Integer.parseInt(msg.substring(msg.indexOf(LOG_REPLICATION_ACK) + 1));
    }

    public static void sendGoodbye(ISocketChannel socketChannel) throws IOException {
        ByteBuffer goodbyeBuffer = ReplicationProtocol.getGoodbyeBuffer();
        NetworkingUtil.transferBufferToChannel(socketChannel, goodbyeBuffer);
    }

    public static void sendAck(ISocketChannel socketChannel, ByteBuffer buf) {
        try {
            buf.clear();
            buf.putInt(ReplicationRequestType.ACK.ordinal());
            buf.flip();
            NetworkingUtil.transferBufferToChannel(socketChannel, buf);
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }

    public static void waitForAck(PartitionReplica replica) throws IOException {
        final ISocketChannel channel = replica.getChannel();
        final ByteBuffer buf = replica.getReusableBuffer();
        ReplicationRequestType responseFunction = ReplicationProtocol.getRequestType(channel, buf);
        if (responseFunction != ReplicationRequestType.ACK) {
            throw new IllegalStateException("Unexpected response while waiting for ack.");
        }
    }

    public static void sendTo(PartitionReplica replica, IReplicationMessage task) {
        final ISocketChannel channel = replica.getChannel();
        final ByteBuffer buf = replica.getReusableBuffer();
        sendTo(channel, task, buf);
    }

    public static void sendTo(ISocketChannel channel, IReplicationMessage task, ByteBuffer buf) {
        ExtendedByteArrayOutputStream outputStream = new ExtendedByteArrayOutputStream();
        try (DataOutputStream oos = new DataOutputStream(outputStream)) {
            task.serialize(oos);
            final int requestSize = REPLICATION_REQUEST_HEADER_SIZE + oos.size();
            final ByteBuffer requestBuffer = ensureSize(buf, requestSize);
            requestBuffer.putInt(task.getMessageType().ordinal());
            requestBuffer.putInt(oos.size());
            requestBuffer.put(outputStream.getByteArray(), 0, outputStream.getLength());
            requestBuffer.flip();
            NetworkingUtil.transferBufferToChannel(channel, requestBuffer);
            channel.getSocketChannel().socket().getOutputStream().flush();
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }

    public static IReplicationMessage read(ISocketChannel socketChannel, ByteBuffer buffer) throws IOException {
        final ReplicationRequestType type = getRequestType(socketChannel, buffer);
        return readMessage(type, socketChannel, buffer);
    }

    public static IReplicationMessage readMessage(ReplicationRequestType type, ISocketChannel socketChannel,
            ByteBuffer buffer) {
        try {
            final ByteBuffer requestBuf = ReplicationProtocol.readRequest(socketChannel, buffer);
            final ByteArrayInputStream input =
                    new ByteArrayInputStream(requestBuf.array(), requestBuf.position(), requestBuf.limit());
            try (DataInputStream dis = new DataInputStream(input)) {
                switch (type) {
                    case PARTITION_RESOURCES_REQUEST:
                        return PartitionResourcesListTask.create(dis);
                    case PARTITION_RESOURCES_RESPONSE:
                        return PartitionResourcesListResponse.create(dis);
                    case REPLICATE_RESOURCE_FILE:
                        return ReplicateFileTask.create(dis);
                    case DELETE_RESOURCE_FILE:
                        return DeleteFileTask.create(dis);
                    case CHECKPOINT_PARTITION:
                        return CheckpointPartitionIndexesTask.create(dis);
                    case LSM_COMPONENT_MASK:
                        return ComponentMaskTask.create(dis);
                    case DROP_INDEX:
                        return DropIndexTask.create(dis);
                    case MARK_COMPONENT_VALID:
                        return MarkComponentValidTask.create(dis);
                    case REPLICATE_LOGS:
                        return ReplicateLogsTask.create(dis);
                    default:
                        throw new IllegalStateException("Unrecognized replication message");
                }
            }
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }

    public static ByteBuffer getEndLogReplicationBuffer() {
        final int logsBatchSize = 1;
        final ByteBuffer endLogRepBuffer =
                ByteBuffer.allocate(Integer.BYTES + ReplicateLogsTask.END_REPLICATION_LOG_SIZE);
        endLogRepBuffer.putInt(logsBatchSize);
        endLogRepBuffer.put((byte) 0);
        endLogRepBuffer.flip();
        return endLogRepBuffer;
    }

    public static ISocketChannel establishReplicaConnection(INcApplicationContext appCtx, InetSocketAddress location)
            throws IOException {
        final SocketChannel socketChannel = SocketChannel.open();
        NetworkUtil.configure(socketChannel);
        socketChannel.connect(location);
        // perform handshake in a non-blocking mode
        socketChannel.configureBlocking(false);
        final ISocketChannelFactory socketChannelFactory =
                appCtx.getServiceContext().getControllerService().getNetworkSecurityManager().getSocketChannelFactory();
        final ISocketChannel clientChannel = socketChannelFactory.createClientChannel(socketChannel);
        if (clientChannel.requiresHandshake() && !clientChannel.handshake()) {
            throw new IllegalStateException("handshake failure");
        }
        // switch to blocking mode after handshake success
        socketChannel.configureBlocking(true);
        return clientChannel;
    }

    private static ByteBuffer ensureSize(ByteBuffer buffer, int size) {
        if (buffer == null || buffer.capacity() < size) {
            return ByteBuffer.allocate(size);
        }
        buffer.clear();
        return buffer;
    }
}
