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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.replication.IPartitionReplica;
import org.apache.asterix.common.replication.IReplicationDestination;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.replication.ReplicationStrategyFactory;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.replication.api.ReplicationDestination;
import org.apache.hyracks.api.replication.IReplicationJob;
import org.apache.hyracks.util.annotations.ThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@ThreadSafe
public class ReplicationManager implements IReplicationManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<InetSocketAddress, ReplicationDestination> dests = new HashMap<>();
    private final ReplicationProperties replicationProperties;
    private final IReplicationStrategy strategy;
    private final INcApplicationContext appCtx;
    private final LogReplicationManager logReplicationManager;
    private final IndexReplicationManager lsnIndexReplicationManager;

    public ReplicationManager(INcApplicationContext appCtx, ReplicationProperties replicationProperties) {
        this.replicationProperties = replicationProperties;
        this.appCtx = appCtx;
        strategy = ReplicationStrategyFactory.create(replicationProperties.getReplicationStrategy());
        logReplicationManager = new LogReplicationManager(appCtx, this);
        lsnIndexReplicationManager = new IndexReplicationManager(appCtx, this);
    }

    @Override
    public void register(IPartitionReplica replica) {
        synchronized (dests) {
            final InetSocketAddress location = replica.getIdentifier().getLocation();
            final ReplicationDestination replicationDest = dests.computeIfAbsent(location, ReplicationDestination::at);
            replicationDest.add(replica);
            logReplicationManager.register(replicationDest);
            lsnIndexReplicationManager.register(replicationDest);
        }
    }

    @Override
    public void unregister(IPartitionReplica replica) {
        synchronized (dests) {
            final InetSocketAddress location = replica.getIdentifier().getLocation();
            final ReplicationDestination dest = dests.get(location);
            if (dest == null) {
                LOGGER.warn(() -> "Asked to unregister unknown replica " + replica);
                return;
            }
            LOGGER.info(() -> "unregister " + replica);
            dest.remove(replica);
            if (dest.getReplicas().isEmpty()) {
                LOGGER.info(() -> "Removing destination with no replicas " + dest);
                logReplicationManager.unregister(dest);
                lsnIndexReplicationManager.unregister(dest);
                dests.remove(location);
            }
        }
    }

    @Override
    public void notifyFailure(IReplicationDestination dest, Exception failure) {
        LOGGER.info(() -> "processing failure for " + dest);
        appCtx.getThreadExecutor().execute(() -> {
            logReplicationManager.unregister(dest);
            lsnIndexReplicationManager.unregister(dest);
            dest.notifyFailure(failure);
        });
    }

    @Override
    public void replicate(ILogRecord logRecord) throws InterruptedException {
        logReplicationManager.replicate(logRecord);
    }

    @Override
    public IReplicationStrategy getReplicationStrategy() {
        return strategy;
    }

    @Override
    public void submitJob(IReplicationJob job) {
        lsnIndexReplicationManager.accept(job);
    }

    @Override
    public boolean isReplicationEnabled() {
        return replicationProperties.isReplicationEnabled();
    }

    @Override
    public void start() {
        // no op
    }

    @Override
    public void dumpState(OutputStream os) {
        // no op
    }

    @Override
    public void stop(boolean dumpState, OutputStream ouputStream) throws IOException {
        LOGGER.info("Closing replication channel");
        appCtx.getReplicationChannel().close();
        LOGGER.info("Replication manager stopped");
    }
}
