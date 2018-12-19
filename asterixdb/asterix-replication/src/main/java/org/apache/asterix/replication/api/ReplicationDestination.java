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
package org.apache.asterix.replication.api;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.common.replication.IPartitionReplica;
import org.apache.asterix.common.replication.IReplicationDestination;
import org.apache.asterix.replication.management.NetworkingUtil;
import org.apache.asterix.replication.messaging.ReplicationProtocol;
import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReplicationDestination implements IReplicationDestination {

    private static final Logger LOGGER = LogManager.getLogger();
    private final Set<IPartitionReplica> replicas = new HashSet<>();
    private final InetSocketAddress location;
    private ISocketChannel logRepChannel;

    private ReplicationDestination(InetSocketAddress location) {
        this.location = location;
    }

    public static ReplicationDestination at(InetSocketAddress location) {
        return new ReplicationDestination(location);
    }

    @Override
    public synchronized void add(IPartitionReplica replica) {
        replicas.add(replica);
    }

    @Override
    public synchronized void remove(IPartitionReplica replica) {
        replicas.remove(replica);
    }

    @Override
    public synchronized void notifyFailure(Exception failure) {
        replicas.forEach(replica -> replica.notifyFailure(failure));
        closeLogReplicationChannel();
    }

    @Override
    public Set<IPartitionReplica> getReplicas() {
        return new HashSet<>(replicas);
    }

    public synchronized Optional<IPartitionReplica> getPartitionReplica(int partition) {
        return replicas.stream().filter(replica -> replica.getIdentifier().getPartition() == partition
                && replica.getStatus() == IPartitionReplica.PartitionReplicaStatus.IN_SYNC).findAny();
    }

    public synchronized ISocketChannel getLogReplicationChannel(INcApplicationContext appCtx) {
        try {
            if (!NetworkingUtil.isHealthy(logRepChannel)) {
                logRepChannel = ReplicationProtocol.establishReplicaConnection(appCtx, location);
            }
            return logRepChannel;
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }

    private synchronized void closeLogReplicationChannel() {
        try {
            if (logRepChannel != null && logRepChannel.getSocketChannel().isOpen()) {
                ReplicationProtocol.sendGoodbye(logRepChannel);
                logRepChannel.close();
                logRepChannel = null;
            }
        } catch (IOException e) {
            LOGGER.warn("Exception while closing socket", e);
        }
    }

    @Override
    public InetSocketAddress getLocation() {
        return location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReplicationDestination that = (ReplicationDestination) o;
        return Objects.equals(location, that.location);
    }

    @Override
    public String toString() {
        return location.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(location);
    }
}
