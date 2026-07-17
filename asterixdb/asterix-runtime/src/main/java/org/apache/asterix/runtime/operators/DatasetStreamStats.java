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

package org.apache.asterix.runtime.operators;

import java.util.Map;

import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IndexStats;

/**
 * Helper method to access stats produced by {@link DatasetStreamStatsOperatorDescriptor}
 */
public final class DatasetStreamStats {

    private final long cardinality;

    private final int avgTupleSize;

    private final long pinnedPages;

    private final long coldReads;
    private final long cloudPageReads;

    private final Map<String, IndexStats> indexesStats;

    public DatasetStreamStats(IOperatorStats opStats) {
        long sampledTupleCount = opStats.getInputTupleCounter().get();
        long sampledTupleSize = opStats.getTupleBytes().get();
        this.cardinality = opStats.getTupleCounter().get();
        this.avgTupleSize = sampledTupleCount > 0 ? (int) (sampledTupleSize / sampledTupleCount) : 0;
        this.pinnedPages = opStats.getPageReadCounter().get();
        this.coldReads = opStats.coldReadCounter().get();
        this.cloudPageReads = opStats.cloudReadRequestCounter().get();
        this.indexesStats = opStats.getIndexesStats();
    }

    static void update(IOperatorStats opStats, long sampledTupleCount, long estimatedCardinality, long totalTupleLength,
            long pinnedPages, long coldReads, long cloudReadCount, Map<String, IndexStats> indexStats) {
        opStats.getTupleCounter().update(estimatedCardinality);
        opStats.getInputTupleCounter().update(sampledTupleCount);
        opStats.getTupleBytes().update(totalTupleLength);
        opStats.cloudReadRequestCounter().update(cloudReadCount);
        opStats.coldReadCounter().update(coldReads);
        opStats.getPageReadCounter().update(pinnedPages);
        opStats.updateIndexesStats(indexStats);
    }

    public long getCardinality() {
        return cardinality;
    }

    public int getAvgTupleSize() {
        return avgTupleSize;
    }

    public long getPinnedPages() {
        return pinnedPages;
    }

    public long getColdReads() {
        return coldReads;
    }

    public long getCloudPageReads() {
        return cloudPageReads;
    }

    public Map<String, IndexStats> getIndexesStats() {
        return indexesStats;
    }
}
