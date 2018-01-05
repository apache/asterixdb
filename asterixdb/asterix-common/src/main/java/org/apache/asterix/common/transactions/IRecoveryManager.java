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
package org.apache.asterix.common.transactions;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Provides API for failure recovery. Failure could be at application level and
 * require a roll back or could be at system level (crash) and require a more
 * sophisticated mechanism of replaying logs and bringing the system to a
 * consistent state ensuring durability.
 */
public interface IRecoveryManager {

    enum SystemState {
        BOOTSTRAPPING, // The first time the NC is bootstrapped.
        PERMANENT_DATA_LOSS, // No checkpoint files found on NC and it is not BOOTSTRAPPING (data loss).
        RECOVERING, // Recovery process is on-going.
        HEALTHY, // All txn logs effects are on disk (no need to perform recovery).
        CORRUPTED // Some txn logs need to be replayed (need to perform recover).
    }

    class ResourceType {
        private ResourceType() {
        }

        public static final byte LSM_BTREE = 0;
        public static final byte LSM_RTREE = 1;
        public static final byte LSM_INVERTED_INDEX = 2;
    }

    /**
     * Returns the state of the system.
     * Health state of the system could be any one of the following. RECOVERING:
     * The system is recovering HEALTHY: The system is in healthy state
     * CORRUPTEED: The system is in corrupted state. This happens when a
     * rollback or recovery task fails. In this state the system is unusable.
     *
     * @see SystemState
     * @return SystemState The state of the system
     * @throws ACIDException
     */
    SystemState getSystemState() throws ACIDException;

    /**
     * Rolls back a transaction.
     *
     * @param txnContext
     *            the transaction context associated with the transaction
     * @throws ACIDException
     */
    void rollbackTransaction(ITransactionContext txnContext) throws ACIDException;

    /**
     * @return min first LSN of the open indexes (including remote indexes if replication is enabled)
     * @throws HyracksDataException
     */
    long getMinFirstLSN() throws HyracksDataException;

    /**
     * @return min first LSN of the open indexes
     * @throws HyracksDataException
     */
    long getLocalMinFirstLSN() throws HyracksDataException;

    /**
     * Replay the logs that belong to the passed {@code partitions} starting from the {@code lowWaterMarkLSN}
     *
     * @param partitions
     * @param lowWaterMarkLSN
     * @throws IOException
     * @throws ACIDException
     */
    void replayPartitionsLogs(Set<Integer> partitions, ILogReader logReader, long lowWaterMarkLSN)
            throws IOException, ACIDException;

    /**
     * Creates a temporary file to be used during recovery
     *
     * @param txnId
     * @param fileName
     * @return A file to the created temporary file
     * @throws IOException
     *             if the file for the specified {@code txnId} with the {@code fileName} already exists
     */
    File createJobRecoveryFile(long txnId, String fileName) throws IOException;

    /**
     * Deletes all temporary recovery files
     */
    void deleteRecoveryTemporaryFiles();

    /**
     * Performs the local recovery process on {@code partitions}
     *
     * @param partitions
     * @throws IOException
     * @throws ACIDException
     */
    void startLocalRecovery(Set<Integer> partitions) throws IOException, ACIDException;

    /**
     * Replay the commited transactions' logs belonging to {@code partitions}. if {@code flush} is true,
     * all datasets are flushed after the logs are replayed.
     *
     * @param partitions
     * @param flush
     * @throws HyracksDataException
     */
    void replayReplicaPartitionLogs(Set<Integer> partitions, boolean flush) throws HyracksDataException;

}
