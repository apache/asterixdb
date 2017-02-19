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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IRemoteRecoveryManager {

    /**
     * Performs the partitions takeover process from the {@code failedNode}
     *
     * @param failedNode
     * @param partitions
     * @throws IOException
     * @throws ACIDException
     */
    public void takeoverPartitons(Integer[] partitions) throws IOException, ACIDException;

    /**
     * Construct failback plan and requests LSM disk components from active remote replicas.
     */
    public void startFailbackProcess();

    /**
     * Requests the remaining LSM disk components files from active remote replicas.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void completeFailbackProcess() throws IOException, InterruptedException;

    /**
     * Replays all committed jobs logs for {@code partitions}. Optionally, flushes all datasets
     * to convert the replayed logs into LSM Components.
     *
     * @param partitions
     * @param flush
     * @throws HyracksDataException
     */
    void replayReplicaPartitionLogs(Set<Integer> partitions, boolean flush) throws HyracksDataException;

    /**
     * Performs the remote recovery plan by requesting data from each specified node
     * for each partitions specified.
     *
     * @param recoveryPlan
     * @throws HyracksDataException
     */
    void doRemoteRecoveryPlan(Map<String, Set<Integer>> recoveryPlan) throws HyracksDataException;
}
