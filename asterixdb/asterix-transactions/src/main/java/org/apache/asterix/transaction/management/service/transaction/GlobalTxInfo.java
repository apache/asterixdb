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

import java.io.Serializable;
import java.util.List;

public class GlobalTxInfo implements Serializable {

    private final int numNodes;
    private final List<Integer> datasetIds;
    private final int numPartitions;
    private static final long serialVersionUID = 5235019091652601411L;

    public GlobalTxInfo(List<Integer> datasetIds, int numNodes, int numPartitions) {
        this.datasetIds = datasetIds;
        this.numNodes = numNodes;
        this.numPartitions = numPartitions;
    }

    public int getNumNodes() {
        return numNodes;
    }

    public List<Integer> getDatasetIds() {
        return datasetIds;
    }

    public int getNumPartitions() {
        return numPartitions;
    }
}
