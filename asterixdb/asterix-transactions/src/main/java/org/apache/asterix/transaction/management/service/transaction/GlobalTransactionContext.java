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
package org.apache.asterix.transaction.management.service.transaction;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.cluster.IGlobalTxManager.TransactionStatus;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.IGlobalTransactionContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.hyracks.util.annotations.ThreadSafe;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@ThreadSafe
public class GlobalTransactionContext implements IGlobalTransactionContext {

    protected final JobId jobId;
    private final AtomicInteger acksReceived;
    private final int numNodes;
    private volatile TransactionStatus status;
    private final List<Integer> datasetIds;
    private final int numPartitions;
    private volatile int expectedAcks;
    private volatile TxnPhase phase = TxnPhase.PREPARE;

    /**
     * Concurrent because the prepared messages of the participating partitions are handled by several CC
     * executor threads at once. A plain {@link java.util.HashMap} loses entries under that concurrency, which
     * drops a node from the commit broadcast and silently leaves its partitions uncommitted.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Made the node resource map concurrent; prepared messages are accumulated from several threads")
    private final Map<String, Map<String, ILSMComponentId>> nodeResourceMap;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public GlobalTransactionContext(JobId jobId, List<Integer> datasetIds, int numNodes, int numPartitions) {
        this.jobId = jobId;
        this.datasetIds = datasetIds;
        this.numNodes = numNodes;
        this.numPartitions = numPartitions;
        this.acksReceived = new AtomicInteger(0);
        this.nodeResourceMap = new ConcurrentHashMap<>();
        this.status = TransactionStatus.ACTIVE;
    }

    public GlobalTransactionContext(FileReference txnLogFileRef, IOManager ioManager) {
        try {
            AtomicTransactionLog txnLog = OBJECT_MAPPER.readValue(new String(ioManager.readAllBytes(txnLogFileRef)),
                    AtomicTransactionLog.class);
            this.jobId = txnLog.getJobId();
            this.datasetIds = txnLog.getDatasetIds();
            this.nodeResourceMap = concurrentCopy(txnLog.getNodeResourceMap());
            this.numNodes = nodeResourceMap.keySet().size();
            this.numPartitions = txnLog.getNumPartitions();
            this.acksReceived = new AtomicInteger(0);
        } catch (JsonProcessingException | HyracksDataException e) {
            throw new ACIDException(e);
        }
    }

    @Override
    public void setTxnStatus(TransactionStatus status) {
        this.status = status;
    }

    @Override
    public TransactionStatus getTxnStatus() {
        return status;
    }

    @Override
    public JobId getJobId() {
        return jobId;
    }

    @Override
    public int incrementAndGetAcksReceived() {
        return acksReceived.incrementAndGet();
    }

    @Override
    public int getAcksReceived() {
        return acksReceived.get();
    }

    /**
     * The phase is published last: an acknowledgement racing with this call is then either rejected against
     * the previous phase, or counted against a target that is already in place.
     */
    @Override
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Open a phase, its acknowledgement target and its counter in one step")
    public void beginPhase(TxnPhase phase, int expectedAcks) {
        acksReceived.set(0);
        this.expectedAcks = expectedAcks;
        this.phase = phase;
    }

    @Override
    public TxnPhase getPhase() {
        return phase;
    }

    @Override
    public int getExpectedAcks() {
        return expectedAcks;
    }

    @Override
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Atomic accumulation of a partition's prepared resources")
    public void addPreparedNodeResources(String nodeId, Map<String, ILSMComponentId> componentIdMap) {
        nodeResourceMap.computeIfAbsent(nodeId, k -> new ConcurrentHashMap<>()).putAll(componentIdMap);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Keep the recovered map concurrent so rollback shares the live map's guarantees")
    private static Map<String, Map<String, ILSMComponentId>> concurrentCopy(
            Map<String, Map<String, ILSMComponentId>> source) {
        Map<String, Map<String, ILSMComponentId>> copy = new ConcurrentHashMap<>();
        if (source != null) {
            source.forEach((nodeId, resources) -> copy.put(nodeId, new ConcurrentHashMap<>(resources)));
        }
        return copy;
    }

    public int getNumNodes() {
        return numNodes;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public List<Integer> getDatasetIds() {
        return datasetIds;
    }

    public Map<String, Map<String, ILSMComponentId>> getNodeResourceMap() {
        return nodeResourceMap;
    }

    @Override
    public void persist(IOManager ioManager) {
        try {
            FileReference fref = ioManager.resolve(Paths.get(String.format("%s.log", jobId)).toString());
            AtomicTransactionLog txnLog = new AtomicTransactionLog(jobId, datasetIds, nodeResourceMap.keySet(),
                    nodeResourceMap, numPartitions);
            ioManager.overwrite(fref,
                    OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(txnLog).getBytes());
        } catch (HyracksDataException | JsonProcessingException e) {
            throw new ACIDException(e);
        }
    }

    @Override
    public void delete(IOManager ioManager) {
        try {
            FileReference fref = ioManager.resolve(Paths.get(String.format("%s.log", jobId)).toString());
            ioManager.delete(fref);
        } catch (HyracksDataException e) {
            throw new RuntimeException(e);
        }
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n" + jobId + "\n");
        sb.append("TransactionState: " + status + "\n");
        return sb.toString();
    }
}
