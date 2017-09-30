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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;

public class StatsCollector implements IStatsCollector {

    private final Map<String, IOperatorStats> operatorStatsMap = new HashMap<>();

    @Override
    public void add(IOperatorStats operatorStats) throws HyracksDataException {
        if (operatorStatsMap.containsKey(operatorStats.getName())) {
            throw new IllegalArgumentException("Operator with the same name already exists");
        }
        operatorStatsMap.put(operatorStats.getName(), operatorStats);
    }

    @Override
    public IOperatorStats getOperatorStats(String operatorName) {
        return operatorStatsMap.get(operatorName);
    }

    public static StatsCollector create(DataInput input) throws IOException {
        StatsCollector statsCollector = new StatsCollector();
        statsCollector.readFields(input);
        return statsCollector;
    }

    @Override
    public IOperatorStats getAggregatedStats() {
        IOperatorStats aggregatedStats = new OperatorStats("aggregated");
        for (IOperatorStats stats : operatorStatsMap.values()) {
            aggregatedStats.getTupleCounter().update(stats.getTupleCounter().get());
            aggregatedStats.getTimeCounter().update(stats.getTupleCounter().get());
        }
        return aggregatedStats;
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeInt(operatorStatsMap.size());
        for (IOperatorStats operatorStats : operatorStatsMap.values()) {
            operatorStats.writeFields(output);
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        int operatorCount = input.readInt();
        for (int i = 0; i < operatorCount; i++) {
            IOperatorStats opStats = OperatorStats.create(input);
            operatorStatsMap.put(opStats.getName(), opStats);
        }
    }
}
