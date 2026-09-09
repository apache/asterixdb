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

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.cluster.IGlobalTxManager;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;

public interface IGlobalTransactionContext {

    /**
     * The round of acknowledgements the transaction is currently collecting. The transaction status cannot
     * stand in for this: it reads {@code PREPARED} throughout both the commit and the rollback phase, so an
     * acknowledgement belonging to an abandoned phase is otherwise indistinguishable from a current one.
     */
    enum TxnPhase {
        PREPARE,
        COMMIT,
        ROLLBACK
    }

    JobId getJobId();

    int incrementAndGetAcksReceived();

    int getAcksReceived();

    int getNumNodes();

    int getNumPartitions();

    /**
     * Opens a round of acknowledgements: records which phase is collecting them and how many it waits for,
     * captured from the set of nodes actually messaged, and discards the previous phase's count. Must be
     * called before the first message of the phase is sent, so that a straggler from the phase before is
     * rejected rather than counted towards this one.
     */
    void beginPhase(TxnPhase phase, int expectedAcks);

    TxnPhase getPhase();

    int getExpectedAcks();

    void setTxnStatus(IGlobalTxManager.TransactionStatus status);

    IGlobalTxManager.TransactionStatus getTxnStatus();

    List<Integer> getDatasetIds();

    Map<String, Map<String, ILSMComponentId>> getNodeResourceMap();

    /**
     * Records the resources a participating partition reported in its prepared message. Called concurrently,
     * once per participating partition, so implementations must accumulate atomically.
     *
     * @param nodeId the node the prepared message came from
     * @param componentIdMap the flushed component of each resource of the reporting partition; may be empty
     */
    void addPreparedNodeResources(String nodeId, Map<String, ILSMComponentId> componentIdMap);

    void persist(IOManager ioManager);

    void delete(IOManager ioManager);

}
