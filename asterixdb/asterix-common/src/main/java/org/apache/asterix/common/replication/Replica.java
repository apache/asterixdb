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
package org.apache.asterix.common.replication;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class Replica {

    public enum ReplicaState {
        ACTIVE,
        DEAD,
        UNKNOWN
    }

    private ReplicaState state = ReplicaState.UNKNOWN;
    String nodeId;
    String ipAddr;
    int port;

    public Replica(String id, String ip, int port) {
        nodeId = id;
        ipAddr = ip;
        this.port = port;
    }

    public ReplicaState getState() {
        return state;
    }

    public void setState(ReplicaState state) {
        this.state = state;
    }

    public static Replica create(DataInput input) throws IOException {
        Replica replica = new Replica(null, null, -1);
        replica.readFields(input);
        return replica;
    }

    public String getId() {
        return nodeId;
    }

    public void writeFields(DataOutput output) throws IOException {
        output.writeUTF(nodeId);
        output.writeUTF(ipAddr);
        output.writeInt(port);
        output.writeInt(state.ordinal());
    }

    public void readFields(DataInput input) throws IOException {
        this.nodeId = input.readUTF();
        this.ipAddr = input.readUTF();
        this.port = input.readInt();
        this.state = ReplicaState.values()[input.readInt()];
    }

    public String getClusterIp() {
        return ipAddr;
    }

    public void setClusterIp(String ip) {
        ipAddr = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void serialize(OutputStream out) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        writeFields(dos);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Replica)) {
            return false;
        }
        Replica other = (Replica) o;
        return nodeId.equals(other.getId());
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }
}
