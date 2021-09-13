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

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.replication.api.IReplicaTask;
import org.apache.asterix.replication.api.IReplicationWorker;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeletePartitionTask implements IReplicaTask {

    private static final Logger LOGGER = LogManager.getLogger();
    private final int partitionId;

    public DeletePartitionTask(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public void perform(INcApplicationContext appCtx, IReplicationWorker worker) {
        try {
            PersistentLocalResourceRepository localResourceRepository =
                    (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
            LOGGER.warn("deleting storage partition {}", partitionId);
            localResourceRepository.deletePartition(partitionId);
            ReplicationProtocol.sendAck(worker.getChannel(), worker.getReusableBuffer());
        } catch (Exception e) {
            throw new ReplicationException(e);
        }
    }

    @Override
    public ReplicationProtocol.ReplicationRequestType getMessageType() {
        return ReplicationProtocol.ReplicationRequestType.DELETE_PARTITION;
    }

    @Override
    public void serialize(OutputStream out) throws HyracksDataException {
        try {
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(partitionId);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static DeletePartitionTask create(DataInput input) throws IOException {
        return new DeletePartitionTask(input.readInt());
    }
}
