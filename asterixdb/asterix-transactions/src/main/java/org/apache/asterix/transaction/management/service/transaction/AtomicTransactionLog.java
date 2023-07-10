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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AtomicTransactionLog {

    private JobId jobId;
    private List<Integer> datasetIds;
    private Set<String> nodeIds;
    private Map<String, Map<String, ILSMComponentId>> nodeResourceMap;
    private int numPartitions;

    @JsonCreator
    public AtomicTransactionLog(@JsonProperty("jobId") JobId jobId,
            @JsonProperty("datasetIds") List<Integer> datasetIds, @JsonProperty("nodeIds") Set<String> nodeIds,
            @JsonProperty("nodeResourceMap") Map<String, Map<String, ILSMComponentId>> nodeResourceMap,
            @JsonProperty("numPartitions") int numPartitions) {
        this.jobId = jobId;
        this.datasetIds = datasetIds;
        this.nodeIds = nodeIds;
        this.nodeResourceMap = nodeResourceMap;
        this.numPartitions = numPartitions;
    }

    public JobId getJobId() {
        return jobId;
    }

    public List<Integer> getDatasetIds() {
        return datasetIds;
    }

    public Set<String> getNodeIds() {
        return nodeIds;
    }

    public Map<String, Map<String, ILSMComponentId>> getNodeResourceMap() {
        return nodeResourceMap;
    }

    public int getNumPartitions() {
        return numPartitions;
    }
}
