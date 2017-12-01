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
package org.apache.asterix.common.storage;

import java.util.List;
import java.util.Set;

public interface IStorageSubsystem {

    /**
     * Adds a replica with the specified {@code id}
     *
     * @param id
     */
    void addReplica(ReplicaIdentifier id);

    /**
     * Removes the replica with the specified {@code id}
     *
     * @param id
     */
    void removeReplica(ReplicaIdentifier id);

    /**
     * The existing replicas of the partition {@code partition}
     *
     * @param partition
     * @return The list of replicas
     */
    List<PartitionReplica> getReplicas(int partition);

    /**
     * Gets the list of partition to which the current node is
     * the master of.
     *
     * @return The list of partition
     */
    Set<Integer> getPartitions();
}
