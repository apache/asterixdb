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
import java.util.ArrayList;
import java.util.Set;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.hyracks.api.replication.IIOReplicationManager;

public interface IReplicationManager extends IIOReplicationManager {

    /**
     * Asynchronously sends a serialized version of the record to remote replicas.
     * 
     * @param logRecord
     *            The log record to be replicated,
     */
    public void replicateLog(ILogRecord logRecord);

    /**
     * Checks whether a log record has been replicated
     * 
     * @param logRecord
     *            the log to check for.
     * @return true, if all ACKs were received from remote replicas.
     */
    public boolean hasBeenReplicated(ILogRecord logRecord);

    /**
     * Requests txns logs from a remote replica.
     * 
     * @param remoteReplicaId
     *            The replica id to send the request to.
     * @param replicasDataToRecover
     *            Get logs that belong to those replicas.
     * @param fromLSN
     *            Low water mark for logs to be requested.
     * @return The logs received that belong to the local node.
     * @throws IOException
     * @throws ACIDException
     */
    public ArrayList<ILogRecord> requestReplicaLogs(String remoteReplicaId, Set<String> replicasDataToRecover,
            long fromLSN) throws IOException, ACIDException;

    /**
     * Requests LSM components files from a remote replica.
     * 
     * @param remoteReplicaId
     *            The replica id to send the request to.
     * @param replicasDataToRecover
     *            Get files that belong to those replicas.
     * @throws IOException
     */
    public void requestReplicaFiles(String remoteReplicaId, Set<String> replicasDataToRecover) throws IOException;

    /**
     * Requests current maximum LSN from remote replicas.
     * 
     * @param remoteReplicaIds
     *            remote replicas to send the request to.
     * @return The maximum of the received maximum LSNs.
     * @throws IOException
     */
    public long getMaxRemoteLSN(Set<String> remoteReplicaIds) throws IOException;

    /**
     * Sends the IP address of the local replica to all remote replicas.
     * 
     * @throws IOException
     */
    public void broadcastNewIPAddress() throws IOException;

    /**
     * @return The number of remote replicas that are in ACTIVE state.
     */
    public int getActiveReplicasCount();

    /**
     * @return The IDs of the remote replicas that are in DEAD state.
     */
    public Set<String> getDeadReplicasIds();

    /**
     * Starts processing of ASYNC replication jobs as well as Txn logs.
     */
    public void startReplicationThreads();

    /**
     * Checks and sets each remote replica state.
     */
    public void initializeReplicasState();

    /**
     * Updates remote replica (in-memory) information.
     * 
     * @param replica
     *            the replica to update.
     */
    public void updateReplicaInfo(Replica replica);

    /**
     * @return The IDs of the remote replicas that are in ACTIVE state.
     */
    public Set<String> getActiveReplicasIds();

    /**
     * Submits a ReplicaEvent to ReplicationEventsMonitor thread.
     * 
     * @param event
     */
    public void reportReplicaEvent(ReplicaEvent event);

    /**
     * Requests the current minimum LSN of a remote replica.
     * 
     * @param replicaId
     *            The replica to send the request to.
     * @return The returned minimum LSN from the remote replica.
     * @throws IOException
     */
    public long requestReplicaMinLSN(String replicaId) throws IOException;

    /**
     * Sends a request to remote replicas to flush indexes that have LSN less than nonSharpCheckpointTargetLSN
     * 
     * @param nonSharpCheckpointTargetLSN
     * @throws IOException
     */
    public void requestFlushLaggingReplicaIndexes(long nonSharpCheckpointTargetLSN) throws IOException;

}
