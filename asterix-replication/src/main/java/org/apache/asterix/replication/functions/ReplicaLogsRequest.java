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

public class ReplicaLogsRequest {
    Set<String> replicaIds;
    long fromLSN;

    public ReplicaLogsRequest(Set<String> replicaIds, long fromLSN) {
        this.replicaIds = replicaIds;
        this.fromLSN = fromLSN;
    }

    public void serialize(OutputStream out) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(replicaIds.size());
        for (String replicaId : replicaIds) {
            dos.writeUTF(replicaId);
        }
        dos.writeLong(fromLSN);
    }

    public static ReplicaLogsRequest create(DataInput input) throws IOException {
        int size = input.readInt();
        Set<String> replicaIds = new HashSet<String>(size);
        for (int i = 0; i < size; i++) {
            replicaIds.add(input.readUTF());
        }
        long fromLSN = input.readLong();
        return new ReplicaLogsRequest(replicaIds, fromLSN);
    }

    public Set<String> getReplicaIds() {
        return replicaIds;
    }

    public void setReplicaIds(Set<String> replicaIds) {
        this.replicaIds = replicaIds;
    }

    public long getFromLSN() {
        return fromLSN;
    }

    public void setFromLSN(long fromLSN) {
        this.fromLSN = fromLSN;
    }
}
