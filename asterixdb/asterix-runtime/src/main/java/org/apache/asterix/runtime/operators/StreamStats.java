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

import org.apache.hyracks.api.job.profiling.IOperatorStats;

/**
 * Helper method to access stats produced by {@link org.apache.asterix.runtime.operators.StreamStatsOperatorDescriptor}
 */
public final class StreamStats {

    private final long cardinality;

    private final int avgTupleSize;

    public StreamStats(IOperatorStats opStats) {
        this.cardinality = opStats.getTupleCounter().get();
        long totalTupleSize = opStats.getPageReads().get();
        this.avgTupleSize = cardinality > 0 ? (int) (totalTupleSize / cardinality) : 0;
    }

    static void update(IOperatorStats opStats, long tupleCount, long tupleSize) {
        opStats.getTupleCounter().update(tupleCount);
        opStats.getPageReads().update(tupleSize);
    }

    public long getCardinality() {
        return cardinality;
    }

    public int getAvgTupleSize() {
        return avgTupleSize;
    }
}
