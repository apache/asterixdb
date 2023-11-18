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
package org.apache.hyracks.control.common.job.profiling;

import static org.apache.hyracks.api.job.profiling.NoOpOperatorStats.INVALID_ODID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.NoOpOperatorStats;
import org.apache.hyracks.api.job.profiling.OperatorStats;

public class StatsCollector implements IStatsCollector {
    private static final long serialVersionUID = 6858817639895434379L;

    private final Map<String, IOperatorStats> operatorStatsMap = new LinkedHashMap<>();

    @Override
    public void add(IOperatorStats operatorStats) {
        if (operatorStatsMap.containsKey(operatorStats.getName())) {
            throw new IllegalArgumentException("Operator with the same name already exists");
        }
        operatorStatsMap.put(operatorStats.getName(), operatorStats);
    }

    @Override
    public IOperatorStats getOperatorStats(String operatorName) {
        return operatorStatsMap.getOrDefault(operatorName, NoOpOperatorStats.INSTANCE);
    }

    @Override
    public Map<String, IOperatorStats> getAllOperatorStats() {
        return Collections.unmodifiableMap(operatorStatsMap);
    }

    public static StatsCollector create(DataInput input) throws IOException {
        StatsCollector statsCollector = new StatsCollector();
        statsCollector.readFields(input);
        return statsCollector;
    }

    @Override
    public IOperatorStats getAggregatedStats() {
        IOperatorStats aggregatedStats = new OperatorStats("aggregated", INVALID_ODID);
        for (IOperatorStats stats : operatorStatsMap.values()) {
            aggregatedStats.getInputTupleCounter().update(stats.getInputTupleCounter().get());
            aggregatedStats.getTimeCounter().update(stats.getTimeCounter().get());
            aggregatedStats.getPageReads().update(stats.getPageReads().get());
            aggregatedStats.coldReadCounter().update(stats.coldReadCounter().get());
        }
        return aggregatedStats;
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeInt(operatorStatsMap.size());
        for (IOperatorStats stats : operatorStatsMap.values()) {
            stats.writeFields(output);
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        int operatorCount = input.readInt();
        for (int i = 0; i < operatorCount; i++) {
            IOperatorStats opStats = IOperatorStats.create(input);
            operatorStatsMap.put(opStats.getName(), opStats);
        }
    }

}
