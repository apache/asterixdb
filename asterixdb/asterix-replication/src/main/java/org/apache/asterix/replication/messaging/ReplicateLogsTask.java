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

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.replication.api.IReplicaTask;
import org.apache.asterix.replication.api.IReplicationWorker;
import org.apache.asterix.replication.logging.RemoteLogRecord;
import org.apache.asterix.replication.logging.RemoteLogsProcessor;
import org.apache.asterix.replication.management.ReplicationChannel;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.network.ISocketChannel;

/**
 * A task to replicate transaction logs from master replica
 */
public class ReplicateLogsTask implements IReplicaTask {

    public static final int END_REPLICATION_LOG_SIZE = 1;
    private final String nodeId;

    public ReplicateLogsTask(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void perform(INcApplicationContext appCtx, IReplicationWorker worker) {
        final ReplicationChannel replicationChannel = (ReplicationChannel) appCtx.getReplicationChannel();
        final RemoteLogsProcessor logsProcessor = replicationChannel.getRemoteLogsProcessor();
        final ILogManager logManager = appCtx.getTransactionSubsystem().getLogManager();
        final RemoteLogRecord reusableLog = new RemoteLogRecord();
        final ISocketChannel channel = worker.getChannel();
        ByteBuffer logsBuffer = ByteBuffer.allocate(logManager.getLogPageSize());
        try {
            while (true) {
                // read a batch of logs
                logsBuffer = ReplicationProtocol.readRequest(channel, logsBuffer);
                // check if it is end of handshake
                if (logsBuffer.remaining() == END_REPLICATION_LOG_SIZE) {
                    break;
                }
                logsProcessor.process(logsBuffer, reusableLog, worker);
            }
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }

    @Override
    public ReplicationProtocol.ReplicationRequestType getMessageType() {
        return ReplicationProtocol.ReplicationRequestType.REPLICATE_LOGS;
    }

    @Override
    public void serialize(OutputStream out) throws HyracksDataException {
        try {
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeUTF(nodeId);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static ReplicateLogsTask create(DataInput input) throws IOException {
        final String node = input.readUTF();
        return new ReplicateLogsTask(node);
    }
}
