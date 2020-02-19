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
import org.apache.hyracks.util.NetworkUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReplicationDestination implements IReplicationDestination {

    private static final Logger LOGGER = LogManager.getLogger();
    private final Set<IPartitionReplica> replicas = new HashSet<>();
    private final InetSocketAddress inputLocation;
    private InetSocketAddress resolvedLocation;
    private ISocketChannel logRepChannel;

    private ReplicationDestination(InetSocketAddress location) {
        this.inputLocation = location;
        this.resolvedLocation = NetworkUtil.ensureResolved(location);
    }

    public static ReplicationDestination at(InetSocketAddress location) {
        if (!location.isUnresolved()) {
            throw new IllegalArgumentException("only unresolved addresses are allowed!");
        }
        return new ReplicationDestination(new InetSocketAddress(location.getHostString(), location.getPort()));
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
                establishReplicaConnection(appCtx);
            }
            return logRepChannel;
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }

    protected void establishReplicaConnection(INcApplicationContext appCtx) throws IOException {
        // try to re-resolve the address, in case our replica has had his IP address updated, and that is why
        // the connection is unhealthy...
        resolvedLocation = NetworkUtil.refresh(resolvedLocation);
        logRepChannel = ReplicationProtocol.establishReplicaConnection(appCtx, resolvedLocation);
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
        return resolvedLocation;
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
        return Objects.equals(inputLocation, that.inputLocation);
    }

    @Override
    public String toString() {
        return resolvedLocation.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputLocation);
    }
}
