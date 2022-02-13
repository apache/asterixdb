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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.replication.api.IReplicationMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class PartitionResourcesListResponse implements IReplicationMessage {

    private final int partition;
    private final Map<String, Long> partitionReplicatedResources;
    private final List<String> files;
    private final boolean owner;

    public PartitionResourcesListResponse(int partition, Map<String, Long> partitionReplicatedResources,
            List<String> files, boolean owner) {
        this.partition = partition;
        this.partitionReplicatedResources = partitionReplicatedResources;
        this.files = files;
        this.owner = owner;
    }

    @Override
    public ReplicationProtocol.ReplicationRequestType getMessageType() {
        return ReplicationProtocol.ReplicationRequestType.PARTITION_RESOURCES_RESPONSE;
    }

    @Override
    public void serialize(OutputStream out) throws HyracksDataException {
        try {
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(partition);
            dos.writeInt(files.size());
            for (String file : files) {
                dos.writeUTF(file);
            }
            dos.writeBoolean(owner);
            dos.writeInt(partitionReplicatedResources.size());
            for (Map.Entry<String, Long> stringLongEntry : partitionReplicatedResources.entrySet()) {
                dos.writeUTF(stringLongEntry.getKey());
                dos.writeLong(stringLongEntry.getValue());
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public List<String> getFiles() {
        return files;
    }

    public static PartitionResourcesListResponse create(DataInput input) throws IOException {
        int partition = input.readInt();
        int size = input.readInt();
        List<String> resources = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            resources.add(input.readUTF());
        }
        boolean owner = input.readBoolean();
        int resourceSize = input.readInt();
        Map<String, Long> partitionReplicatedResources = new HashMap<>();
        for (int i = 0; i < resourceSize; i++) {
            partitionReplicatedResources.put(input.readUTF(), input.readLong());
        }
        return new PartitionResourcesListResponse(partition, partitionReplicatedResources, resources, owner);
    }

    public boolean isOrigin() {
        return owner;
    }

    public Map<String, Long> getPartitionReplicatedResources() {
        return partitionReplicatedResources;
    }
}
