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
package org.apache.asterix.replication.functions;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.asterix.common.replication.ReplicaEvent;
import org.apache.asterix.replication.management.NetworkingUtil;
import org.apache.asterix.replication.storage.LSMComponentProperties;
import org.apache.asterix.replication.storage.LSMIndexFileProperties;
import org.apache.hyracks.data.std.util.ExtendedByteArrayOutputStream;

public class ReplicationProtocol {

    /**
     * All replication messages start with ReplicationRequestType (4 bytes), then the length of the request in bytes
     */
    public static final String JOB_REPLICATION_ACK = "$";

    public final static int REPLICATION_REQUEST_TYPE_SIZE = Integer.BYTES;
    public final static int REPLICATION_REQUEST_HEADER_SIZE = REPLICATION_REQUEST_TYPE_SIZE + Integer.BYTES;

    /*
     * ReplicationRequestType:
     * REPLICATE_LOG: txn log replication
     * REPLICATE_FILE: replicate a file(s)
     * DELETE_FILE: delete a file(s)
     * GET_REPLICA_FILES: used during remote recovery to request lost LSM Components
     * GET_REPLICA_MAX_LSN: used during remote recovery initialize a log manager LSN
     * GOODBYE: used to notify replicas that the replication request has been completed
     * REPLICA_EVENT: used to notify replicas about a remote replica split/merge.
     * LSM_COMPONENT_PROPERTIES: used to send the properties of an LSM Component before its physical files are sent
     * ACK: used to notify the requesting replica that the request has been completed successfully
     * FLUSH_INDEX: request remote replica to flush an LSM component
     */
    public enum ReplicationRequestType {
        REPLICATE_LOG,
        REPLICATE_FILE,
        DELETE_FILE,
        GET_REPLICA_FILES,
        GET_REPLICA_MAX_LSN,
        GOODBYE,
        REPLICA_EVENT,
        LSM_COMPONENT_PROPERTIES,
        ACK,
        FLUSH_INDEX
    }

    public static ByteBuffer readRequest(SocketChannel socketChannel, ByteBuffer dataBuffer) throws IOException {
        //read request size
        NetworkingUtil.readBytes(socketChannel, dataBuffer, Integer.BYTES);
        int requestSize = dataBuffer.getInt();

        if (dataBuffer.capacity() < requestSize) {
            dataBuffer = ByteBuffer.allocate(requestSize);
        }

        //read request
        NetworkingUtil.readBytes(socketChannel, dataBuffer, requestSize);

        return dataBuffer;
    }

    public static ByteBuffer writeLSMComponentPropertiesRequest(LSMComponentProperties lsmCompProp, ByteBuffer buffer)
            throws IOException {
        ExtendedByteArrayOutputStream outputStream = new ExtendedByteArrayOutputStream();
        try (DataOutputStream oos = new DataOutputStream(outputStream)) {
            lsmCompProp.serialize(oos);
            int requestSize = REPLICATION_REQUEST_HEADER_SIZE + oos.size();
            if (buffer.capacity() < requestSize) {
                buffer = ByteBuffer.allocate(requestSize);
            } else {
                buffer.clear();
            }
            buffer.putInt(ReplicationRequestType.LSM_COMPONENT_PROPERTIES.ordinal());
            buffer.putInt(oos.size());
            buffer.put(outputStream.getByteArray(), 0, outputStream.getLength());
            buffer.flip();
            return buffer;
        }
    }

    public static ReplicationRequestType getRequestType(SocketChannel socketChannel, ByteBuffer byteBuffer)
            throws IOException {
        //read replication request type
        NetworkingUtil.readBytes(socketChannel, byteBuffer, REPLICATION_REQUEST_TYPE_SIZE);

        ReplicationRequestType requestType = ReplicationProtocol.ReplicationRequestType.values()[byteBuffer.getInt()];
        return requestType;
    }

    public static LSMComponentProperties readLSMPropertiesRequest(ByteBuffer buffer) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.limit());
        DataInputStream dis = new DataInputStream(bais);
        return LSMComponentProperties.create(dis);
    }

    public static ByteBuffer getGoodbyeBuffer() {
        ByteBuffer bb = ByteBuffer.allocate(REPLICATION_REQUEST_TYPE_SIZE);
        bb.putInt(ReplicationRequestType.GOODBYE.ordinal());
        bb.flip();
        return bb;
    }

    public static ByteBuffer getAckBuffer() {
        ByteBuffer bb = ByteBuffer.allocate(REPLICATION_REQUEST_TYPE_SIZE);
        bb.putInt(ReplicationRequestType.ACK.ordinal());
        bb.flip();
        return bb;
    }

    public static ByteBuffer writeFileReplicationRequest(ByteBuffer requestBuffer, LSMIndexFileProperties afp,
            ReplicationRequestType requestType) throws IOException {
        ExtendedByteArrayOutputStream outputStream = new ExtendedByteArrayOutputStream();
        try (DataOutputStream oos = new DataOutputStream(outputStream)) {
            afp.serialize(oos);
            int requestSize = REPLICATION_REQUEST_HEADER_SIZE + oos.size();
            if (requestBuffer.capacity() < requestSize) {
                requestBuffer = ByteBuffer.allocate(requestSize);
            } else {
                requestBuffer.clear();
            }
            requestBuffer.putInt(requestType.ordinal());
            requestBuffer.putInt(oos.size());
            requestBuffer.put(outputStream.getByteArray(), 0, outputStream.getLength());
            requestBuffer.flip();
            return requestBuffer;
        }
    }

    public static LSMIndexFileProperties readFileReplicationRequest(ByteBuffer buffer) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.limit());
        DataInputStream dis = new DataInputStream(bais);
        return LSMIndexFileProperties.create(dis);
    }

    public static ByteBuffer writeReplicaEventRequest(ReplicaEvent event) throws IOException {
        ExtendedByteArrayOutputStream outputStream = new ExtendedByteArrayOutputStream();
        try (DataOutputStream oos = new DataOutputStream(outputStream)) {
            event.serialize(oos);
            ByteBuffer buffer = ByteBuffer.allocate(REPLICATION_REQUEST_HEADER_SIZE + oos.size());
            buffer.putInt(ReplicationRequestType.REPLICA_EVENT.ordinal());
            buffer.putInt(oos.size());
            buffer.put(outputStream.getByteArray(), 0, outputStream.getLength());
            buffer.flip();
            return buffer;
        }
    }

    public static ReplicaEvent readReplicaEventRequest(ByteBuffer buffer) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.limit());
        DataInputStream dis = new DataInputStream(bais);

        return ReplicaEvent.create(dis);
    }

    public static ByteBuffer writeGetReplicaFilesRequest(ByteBuffer buffer, ReplicaFilesRequest request)
            throws IOException {
        ExtendedByteArrayOutputStream outputStream = new ExtendedByteArrayOutputStream();
        try (DataOutputStream oos = new DataOutputStream(outputStream)) {
            request.serialize(oos);

            int requestSize = REPLICATION_REQUEST_HEADER_SIZE + oos.size();
            if (buffer.capacity() < requestSize) {
                buffer = ByteBuffer.allocate(requestSize);
            } else {
                buffer.clear();
            }
            buffer.putInt(ReplicationRequestType.GET_REPLICA_FILES.ordinal());
            buffer.putInt(oos.size());
            buffer.put(outputStream.getByteArray(), 0, outputStream.getLength());
            buffer.flip();
            return buffer;
        }
    }

    public static ByteBuffer writeGetReplicaIndexFlushRequest(ByteBuffer buffer, ReplicaIndexFlushRequest request)
            throws IOException {
        ExtendedByteArrayOutputStream outputStream = new ExtendedByteArrayOutputStream();
        try (DataOutputStream oos = new DataOutputStream(outputStream)) {
            request.serialize(oos);
            int requestSize = REPLICATION_REQUEST_HEADER_SIZE + oos.size();
            if (buffer.capacity() < requestSize) {
                buffer = ByteBuffer.allocate(requestSize);
            } else {
                buffer.clear();
            }
            buffer.putInt(ReplicationRequestType.FLUSH_INDEX.ordinal());
            buffer.putInt(oos.size());
            buffer.put(outputStream.getByteArray(), 0, outputStream.getLength());
            buffer.flip();
            return buffer;
        }
    }

    public static ReplicaFilesRequest readReplicaFileRequest(ByteBuffer buffer) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.limit());
        DataInputStream dis = new DataInputStream(bais);
        return ReplicaFilesRequest.create(dis);
    }

    public static ReplicaIndexFlushRequest readReplicaIndexFlushRequest(ByteBuffer buffer) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.limit());
        DataInputStream dis = new DataInputStream(bais);
        return ReplicaIndexFlushRequest.create(dis);
    }

    public static void writeGetReplicaMaxLSNRequest(ByteBuffer requestBuffer) {
        requestBuffer.clear();
        requestBuffer.putInt(ReplicationRequestType.GET_REPLICA_MAX_LSN.ordinal());
        requestBuffer.flip();
    }

    public static int getJobIdFromLogAckMessage(String msg) {
        return Integer.parseInt(msg.substring((msg.indexOf(JOB_REPLICATION_ACK) + 1)));
    }

    public static String getNodeIdFromLogAckMessage(String msg) {
        return msg.substring(0, msg.indexOf(JOB_REPLICATION_ACK));
    }

    /**
     * Sends a goodbye request to a remote replica indicating the end of a replication request.
     *
     * @param socketChannel
     *            the remote replica socket.
     * @throws IOException
     */
    public static void sendGoodbye(SocketChannel socketChannel) throws IOException {
        ByteBuffer goodbyeBuffer = ReplicationProtocol.getGoodbyeBuffer();
        NetworkingUtil.transferBufferToChannel(socketChannel, goodbyeBuffer);
    }

    public static void sendAck(SocketChannel socketChannel) throws IOException {
        ByteBuffer ackBuffer = ReplicationProtocol.getAckBuffer();
        NetworkingUtil.transferBufferToChannel(socketChannel, ackBuffer);
    }
}
