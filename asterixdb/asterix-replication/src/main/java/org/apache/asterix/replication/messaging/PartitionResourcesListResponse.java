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
import java.util.List;

import org.apache.asterix.replication.api.IReplicationMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class PartitionResourcesListResponse implements IReplicationMessage {

    private final int partition;
    private final List<String> resources;

    public PartitionResourcesListResponse(int partition, List<String> resources) {
        this.partition = partition;
        this.resources = resources;
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
            dos.writeInt(resources.size());
            for (String file : resources) {
                dos.writeUTF(file);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public List<String> getResources() {
        return resources;
    }

    public static PartitionResourcesListResponse create(DataInput input) throws IOException {
        int partition = input.readInt();
        int size = input.readInt();
        List<String> resources = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            resources.add(input.readUTF());
        }
        return new PartitionResourcesListResponse(partition, resources);
    }
}
