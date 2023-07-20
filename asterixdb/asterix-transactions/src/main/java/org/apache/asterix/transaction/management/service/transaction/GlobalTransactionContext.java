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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.cluster.IGlobalTxManager.TransactionStatus;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.IGlobalTransactionContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.util.annotations.ThreadSafe;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@ThreadSafe
public class GlobalTransactionContext implements IGlobalTransactionContext {

    protected final JobId jobId;
    private AtomicInteger acksReceived;
    private final int numNodes;
    private TransactionStatus status;
    private final List<Integer> datasetIds;
    private final int numPartitions;
    private final Map<String, Map<String, ILSMComponentId>> nodeResourceMap;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public GlobalTransactionContext(JobId jobId, List<Integer> datasetIds, int numNodes, int numPartitions) {
        this.jobId = jobId;
        this.datasetIds = datasetIds;
        this.numNodes = numNodes;
        this.numPartitions = numPartitions;
        this.acksReceived = new AtomicInteger(0);
        this.nodeResourceMap = new HashMap<>();
        this.status = TransactionStatus.ACTIVE;
    }

    public GlobalTransactionContext(FileReference txnLogFileRef, IOManager ioManager) {
        try {
            AtomicTransactionLog txnLog = OBJECT_MAPPER.readValue(new String(ioManager.readAllBytes(txnLogFileRef)),
                    AtomicTransactionLog.class);
            this.jobId = txnLog.getJobId();
            this.datasetIds = txnLog.getDatasetIds();
            this.nodeResourceMap = txnLog.getNodeResourceMap();
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

    @Override
    public void resetAcksReceived() {
        acksReceived = new AtomicInteger(0);
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
