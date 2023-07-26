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

import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadataAtomicTransactionLog {

    private TxnId txnId;
    private List<Integer> datasetIds;
    private String nodeId;
    private Map<String, ILSMComponentId> resourceMap;

    @JsonCreator
    public MetadataAtomicTransactionLog(@JsonProperty("txnId") TxnId txnId,
            @JsonProperty("datasetIds") List<Integer> datasetIds, @JsonProperty("nodeId") String nodeId,
            @JsonProperty("resourceMap") Map<String, ILSMComponentId> resourceMap) {
        this.txnId = txnId;
        this.datasetIds = datasetIds;
        this.nodeId = nodeId;
        this.resourceMap = resourceMap;
    }

    public TxnId getTxnId() {
        return txnId;
    }

    public List<Integer> getDatasetIds() {
        return datasetIds;
    }

    public String getNodeId() {
        return nodeId;
    }

    public Map<String, ILSMComponentId> getResourceMap() {
        return resourceMap;
    }
}
