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

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

public class ReplicaFilesRequest {
    private final Set<Integer> partitionIds;
    private final Set<String> existingFiles;

    public ReplicaFilesRequest(Set<Integer> partitionIds, Set<String> existingFiles) {
        this.partitionIds = partitionIds;
        this.existingFiles = existingFiles;
    }

    public void serialize(OutputStream out) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            dos.writeInt(partitionId);
        }
        dos.writeInt(existingFiles.size());
        for (String fileName : existingFiles) {
            dos.writeUTF(fileName);
        }
    }

    public static ReplicaFilesRequest create(DataInput input) throws IOException {
        int size = input.readInt();
        Set<Integer> partitionIds = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            partitionIds.add(input.readInt());
        }
        int filesCount = input.readInt();
        Set<String> existingFiles = new HashSet<>(filesCount);
        for (int i = 0; i < filesCount; i++) {
            existingFiles.add(input.readUTF());
        }
        return new ReplicaFilesRequest(partitionIds, existingFiles);
    }

    public Set<Integer> getPartitionIds() {
        return partitionIds;
    }

    public Set<String> getExistingFiles() {
        return existingFiles;
    }
}
