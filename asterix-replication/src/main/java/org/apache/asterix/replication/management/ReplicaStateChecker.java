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
package org.apache.asterix.replication.management;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.concurrent.Callable;

import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.replication.Replica;
import org.apache.asterix.common.replication.Replica.ReplicaState;
import org.apache.asterix.replication.functions.AsterixReplicationProtocol;

public class ReplicaStateChecker implements Callable<Void> {

    private final int WAIT_TIME = 2000;
    private final Replica replica;
    private final int replicationTimeOut;
    private final ReplicationManager replicationManager;
    private final AsterixReplicationProperties asterixReplicationProperties;
    private final boolean suspendReplication;

    public ReplicaStateChecker(Replica replica, int replicationTimeOut, ReplicationManager replicationManager,
            AsterixReplicationProperties asterixReplicationProperties, boolean suspendReplication) {
        this.replica = replica;
        this.replicationTimeOut = replicationTimeOut;
        this.replicationManager = replicationManager;
        this.asterixReplicationProperties = asterixReplicationProperties;
        this.suspendReplication = suspendReplication;
    }

    @Override
    public Void call() throws Exception {
        Thread.currentThread().setName("ReplicaConnector Thread");

        long startTime = System.currentTimeMillis();
        InetSocketAddress replicaAddress = replica.getAddress(asterixReplicationProperties);
        SocketChannel connection = null;

        while (true) {
            try {
                connection = SocketChannel.open();
                connection.configureBlocking(true);
                connection.connect(new InetSocketAddress(replicaAddress.getHostString(), replicaAddress.getPort()));
                ByteBuffer buffer = AsterixReplicationProtocol.getGoodbyeBuffer();
                connection.write(buffer);
                replicationManager.updateReplicaState(replica.getId(), ReplicaState.ACTIVE, suspendReplication);
                return null;
            } catch (IOException | UnresolvedAddressException e) {
                Thread.sleep(WAIT_TIME);

                //check if connection to replica timed out
                if (((System.currentTimeMillis() - startTime) / 1000) >= replicationTimeOut) {
                    replicationManager.updateReplicaState(replica.getId(), ReplicaState.DEAD, suspendReplication);
                    return null;
                } else {
                    continue;
                }
            } finally {
                if (connection.isOpen()) {
                    connection.close();
                }
            }
        }
    }

}
