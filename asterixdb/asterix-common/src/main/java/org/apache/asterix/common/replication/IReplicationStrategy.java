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

import java.util.Set;

import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IReplicationStrategy {

    /**
     * @param datasetId
     * @return True, if the dataset should be replicated. Otherwise false.
     */
    boolean isMatch(int datasetId);

    /**
     * @param nodeId
     * @return The set of nodes that replicate data on {@code nodeId}.
     */
    Set<Replica> getRemotePrimaryReplicas(String nodeId);

    /**
     * @param node
     * @return The set of nodes that {@code nodeId} replicates data to.
     */
    Set<Replica> getRemoteReplicas(String node);

    /**
     * @param nodeId
     * @return true if {@code nodeId} has any remote primary replica or remote replica. Otherwise false.
     */
    default boolean isParticipant(String nodeId) {
        return !getRemoteReplicas(nodeId).isEmpty() || !getRemotePrimaryReplicas(nodeId).isEmpty();
    }

    /**
     * @param cluster
     * @return A replication strategy based on the passed configurations.
     */
    IReplicationStrategy from(Cluster cluster) throws HyracksDataException;
}
