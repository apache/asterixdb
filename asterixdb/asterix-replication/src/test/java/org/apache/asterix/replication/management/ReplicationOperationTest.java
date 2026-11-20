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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.replication.IPartitionReplica;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.storage.ReplicaIdentifier;
import org.apache.asterix.replication.api.PartitionReplica;
import org.apache.asterix.replication.api.ReplicationDestination;
import org.apache.hyracks.api.replication.IReplicationJob;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReplicationOperationTest {

    private static final int PARTITION = 6;
    private static final int METADATA_PARTITION = -1;
    private static final InetSocketAddress REPLICA_LOCATION = InetSocketAddress.createUnresolved("localhost", 9120);

    private INcApplicationContext appCtx;
    private IndexReplicationManager indexReplicationManager;
    private ReplicationDestination dest;
    private IPartitionReplica partitionReplica;
    private IPartitionReplica metadataReplica;
    private DatasetResourceReference indexRef;
    private IReplicationJob job;

    @Before
    public void setUp() {
        appCtx = mock(INcApplicationContext.class);
        dest = ReplicationDestination.at(REPLICA_LOCATION);
        partitionReplica = mockReplica(PARTITION);
        metadataReplica = mockReplica(METADATA_PARTITION);
        dest.add(partitionReplica);
        dest.add(metadataReplica);

        indexReplicationManager = mock(IndexReplicationManager.class);
        when(indexReplicationManager.getDestinations()).thenReturn(Set.of(dest));
        when(indexReplicationManager.skip(any())).thenReturn(false);

        indexRef = mock(DatasetResourceReference.class);
        when(indexRef.getPartitionId()).thenReturn(PARTITION);
        when(indexRef.getRelativePath()).thenReturn(Paths.get("storage", "partition_" + PARTITION, "idx"));

        job = mock(IReplicationJob.class);
        when(job.getJobFiles()).thenReturn(Collections.emptySet());
    }

    /**
     * A replica that is removed while a replication to it is in flight (e.g. due to a topology change) may delete its
     * partition files causing the replication to fail. Such a failure must not fail the other replicas on the same
     * destination (e.g. the metadata partition replica).
     */
    @Test
    public void testFailureOfRemovedReplicaIsIgnored() {
        when(job.getJobType()).thenAnswer(invocation -> {
            // simulate the replica being removed while the replication is in flight, then failing
            dest.remove(partitionReplica);
            throw new IllegalStateException("simulated replication failure");
        });
        new ReplicationOperation(appCtx, indexRef, job, indexReplicationManager).call();
        verify(indexReplicationManager, never()).handleFailure(any(), any());
        verify(indexReplicationManager).afterReplication(job);
        Assert.assertTrue(dest.isRegistered(metadataReplica));
    }

    @Test
    public void testFailureOfRegisteredReplicaIsReported() {
        when(job.getJobType()).thenThrow(new IllegalStateException("simulated replication failure"));
        new ReplicationOperation(appCtx, indexRef, job, indexReplicationManager).call();
        verify(indexReplicationManager).handleFailure(eq(dest), any(IllegalStateException.class));
        verify(indexReplicationManager).afterReplication(job);
    }

    @Test
    public void testConnectionOfRemovedReplicaIsClosedOnRecycle() {
        PartitionReplica connection = spy(dest.getPartitionReplicaConnection(partitionReplica.getIdentifier(), appCtx));
        dest.remove(partitionReplica);
        dest.recycleConnection(connection);
        verify(connection).close();
    }

    @Test
    public void testConnectionTakenAfterRemovalIsClosedOnRecycle() {
        dest.remove(partitionReplica);
        PartitionReplica connection = spy(dest.getPartitionReplicaConnection(partitionReplica.getIdentifier(), appCtx));
        dest.recycleConnection(connection);
        verify(connection).close();
    }

    @Test
    public void testConnectionOfRegisteredReplicaIsRecycled() {
        PartitionReplica connection = spy(dest.getPartitionReplicaConnection(partitionReplica.getIdentifier(), appCtx));
        dest.recycleConnection(connection);
        verify(connection, never()).close();
        Assert.assertSame(connection, dest.getPartitionReplicaConnection(partitionReplica.getIdentifier(), appCtx));
    }

    private static IPartitionReplica mockReplica(int partition) {
        IPartitionReplica replica = mock(IPartitionReplica.class);
        when(replica.getIdentifier()).thenReturn(ReplicaIdentifier.of(partition, "nc2", REPLICA_LOCATION));
        when(replica.getStatus()).thenReturn(IPartitionReplica.PartitionReplicaStatus.IN_SYNC);
        return replica;
    }
}
