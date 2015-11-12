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
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.replication.Replica;
import org.apache.asterix.common.replication.ReplicaEvent;
import org.apache.asterix.replication.functions.AsterixReplicationProtocol;

public class ReplicaEventNotifier implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(ReplicaEventNotifier.class.getName());

    final int WAIT_TIME = 2000;
    final Set<Replica> notifyReplicaNodes;

    int notificationTimeOut;

    final ReplicaEvent event;
    final AsterixReplicationProperties asterixReplicationProperties;

    public ReplicaEventNotifier(ReplicaEvent event, AsterixReplicationProperties asterixReplicationProperties) {
        this.event = event;
        this.asterixReplicationProperties = asterixReplicationProperties;
        notificationTimeOut = asterixReplicationProperties.getReplicationTimeOut();
        notifyReplicaNodes = asterixReplicationProperties.getRemoteReplicas(event.getReplica().getId());
    }

    @Override
    public void run() {
        Thread.currentThread().setName("ReplicaEventNotifier Thread");

        if (notifyReplicaNodes == null) {
            return;
        }

        ByteBuffer buffer = null;
        try {
            buffer = AsterixReplicationProtocol.writeReplicaEventRequest(event);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Replica replica : notifyReplicaNodes) {
            long startTime = System.currentTimeMillis();
            InetSocketAddress replicaAddress = replica.getAddress(asterixReplicationProperties);
            SocketChannel connection = null;

            while (true) {
                try {
                    connection = SocketChannel.open();
                    connection.configureBlocking(true);
                    connection.connect(new InetSocketAddress(replicaAddress.getHostString(), replicaAddress.getPort()));
                    //send replica event
                    connection.write(buffer);
                    //send goodbye
                    connection.write(AsterixReplicationProtocol.getGoodbyeBuffer());
                    break;
                } catch (IOException | UnresolvedAddressException e) {
                    try {
                        Thread.sleep(WAIT_TIME);
                    } catch (InterruptedException e1) {
                        //ignore
                    }

                    //check if connection to replica timed out
                    if (((System.currentTimeMillis() - startTime) / 1000) >= notificationTimeOut) {
                        LOGGER.log(Level.WARNING, "Could not send ReplicaEvent to " + replica);
                        break;
                    }
                } finally {
                    if (connection.isOpen()) {
                        try {
                            connection.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    buffer.position(0);
                }
            }
        }
    }
}
