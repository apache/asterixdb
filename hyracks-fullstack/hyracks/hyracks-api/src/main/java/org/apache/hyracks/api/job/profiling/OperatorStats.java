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
package org.apache.hyracks.api.job.profiling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.com.job.profiling.counters.Counter;
import org.apache.hyracks.api.job.profiling.counters.ICounter;

public class OperatorStats implements IOperatorStats {
    private static final long serialVersionUID = 6401830963367567167L;

    public final String operatorName;
    public final ICounter tupleCounter;
    public final ICounter timeCounter;
    public final ICounter diskIoCounter;
    private final Map<String, IndexStats> indexesStats;

    public OperatorStats(String operatorName) {
        if (operatorName == null || operatorName.isEmpty()) {
            throw new IllegalArgumentException("operatorName must not be null or empty");
        }
        this.operatorName = operatorName;
        tupleCounter = new Counter("tupleCounter");
        timeCounter = new Counter("timeCounter");
        diskIoCounter = new Counter("diskIoCounter");
        indexesStats = new HashMap<>();
    }

    public static IOperatorStats create(DataInput input) throws IOException {
        String name = input.readUTF();
        OperatorStats operatorStats = new OperatorStats(name);
        operatorStats.readFields(input);
        return operatorStats;
    }

    @Override
    public String getName() {
        return operatorName;
    }

    @Override
    public ICounter getTupleCounter() {
        return tupleCounter;
    }

    @Override
    public ICounter getTimeCounter() {
        return timeCounter;
    }

    @Override
    public ICounter getDiskIoCounter() {
        return diskIoCounter;
    }

    @Override
    public void updateIndexesStats(Map<String, IndexStats> stats) {
        if (stats == null) {
            return;
        }
        for (Map.Entry<String, IndexStats> stat : stats.entrySet()) {
            String indexName = stat.getKey();
            IndexStats indexStat = stat.getValue();
            IndexStats existingIndexStat = indexesStats.get(indexName);
            if (existingIndexStat == null) {
                indexesStats.put(indexName, new IndexStats(indexName, indexStat.getNumPages()));
            } else {
                existingIndexStat.updateNumPages(indexStat.getNumPages());
            }
        }
    }

    @Override
    public Map<String, IndexStats> getIndexesStats() {
        return indexesStats;
    }

    @Override
    public void updateFrom(IOperatorStats stats) {
        tupleCounter.update(stats.getTupleCounter().get());
        timeCounter.update(stats.getTimeCounter().get());
        diskIoCounter.update(stats.getDiskIoCounter().get());
        updateIndexesStats(stats.getIndexesStats());
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeUTF(operatorName);
        output.writeLong(tupleCounter.get());
        output.writeLong(timeCounter.get());
        output.writeLong(diskIoCounter.get());
        writeIndexesStats(output);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        tupleCounter.set(input.readLong());
        timeCounter.set(input.readLong());
        diskIoCounter.set(input.readLong());
        readIndexesStats(input);
    }

    private void writeIndexesStats(DataOutput output) throws IOException {
        output.writeInt(indexesStats.size());
        for (Map.Entry<String, IndexStats> indexStat : indexesStats.entrySet()) {
            output.writeUTF(indexStat.getKey());
            indexStat.getValue().writeFields(output);
        }
    }

    private void readIndexesStats(DataInput input) throws IOException {
        int numIndexes = input.readInt();
        for (int i = 0; i < numIndexes; i++) {
            String indexName = input.readUTF();
            IndexStats indexStats = IndexStats.create(input);
            indexesStats.put(indexName, indexStats);
        }
    }

    @Override
    public String toString() {
        return "{ " + "\"operatorName\": \"" + operatorName + "\", " + "\"" + tupleCounter.getName() + "\": "
                + tupleCounter.get() + ", \"" + timeCounter.getName() + "\": " + timeCounter.get() + ", \""
                + ", \"indexesStats\": \"" + indexesStats + "\" }";
    }
}
