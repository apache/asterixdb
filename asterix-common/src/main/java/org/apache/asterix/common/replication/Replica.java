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
import java.net.InetSocketAddress;

import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.event.schema.cluster.Node;

public class Replica {

    public enum ReplicaState {
        ACTIVE,
        DEAD,
        UNKNOWN
    }

    final Node node;
    private ReplicaState state = ReplicaState.UNKNOWN;

    public Replica(Node node) {
        this.node = node;
    }

    public ReplicaState getState() {
        return state;
    }

    public void setState(ReplicaState state) {
        this.state = state;
    }

    public Node getNode() {
        return node;
    }

    public String getId() {
        return node.getId();
    }

    public InetSocketAddress getAddress(AsterixReplicationProperties asterixReplicationProperties) {
        String replicaIPAddress = node.getClusterIp();
        int replicationPort = asterixReplicationProperties.getDataReplicationPort(node.getId());
        InetSocketAddress replicaAddress = InetSocketAddress.createUnresolved(replicaIPAddress, replicationPort);
        return replicaAddress;
    }

    public static Replica create(DataInput input) throws IOException {
        Node node = new Node();
        Replica replica = new Replica(node);
        replica.readFields(input);
        return replica;
    }

    public void writeFields(DataOutput output) throws IOException {
        output.writeUTF(node.getId());
        output.writeUTF(node.getClusterIp());
        output.writeInt(state.ordinal());
    }

    public void readFields(DataInput input) throws IOException {
        this.node.setId(input.readUTF());
        this.node.setClusterIp(input.readUTF());
        this.state = ReplicaState.values()[input.readInt()];
    }

    public void serialize(OutputStream out) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        writeFields(dos);
    }
}
