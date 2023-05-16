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
package org.apache.asterix.common.cluster;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class PartitioningProperties {
    private final IFileSplitProvider splitsProvider;
    private final AlgebricksPartitionConstraint constraints;
    private final int[][] computeStorageMap;

    private PartitioningProperties(IFileSplitProvider splitsProvider, AlgebricksPartitionConstraint constraints,
            int[][] computeStorageMap) {
        this.splitsProvider = splitsProvider;
        this.constraints = constraints;
        this.computeStorageMap = computeStorageMap;
    }

    public static PartitioningProperties of(IFileSplitProvider splitsProvider,
            AlgebricksPartitionConstraint constraints, int[][] computeStorageMap) {
        return new PartitioningProperties(splitsProvider, constraints, computeStorageMap);
    }

    public IFileSplitProvider getSplitsProvider() {
        return splitsProvider;
    }

    public AlgebricksPartitionConstraint getConstraints() {
        return constraints;
    }

    public int[][] getComputeStorageMap() {
        return computeStorageMap;
    }

    public int getNumberOfPartitions() {
        return splitsProvider.getFileSplits().length;
    }
}
