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
    private static final long serialVersionUID = 6401830963361567126L;
    public final String operatorName;

    public final String operatorId;
    public final ICounter tupleCounter;
    public final ICounter timeCounter;
    public final ICounter pageReads;
    public final ICounter coldReadCounter;
    public final ICounter avgTupleSz;
    public final ICounter minTupleSz;
    public final ICounter maxTupleSz;
    public final ICounter inputTupleCounter;
    public final ICounter level;
    public final ICounter bytesRead;
    public final ICounter bytesWritten;
    private final Map<String, IndexStats> indexesStats;

    //TODO: this is quickly becoming gross it should just be a map where the value is obliged to be a Counter

    public OperatorStats(String operatorName, String operatorId) {
        if (operatorName == null || operatorName.isEmpty()) {
            throw new IllegalArgumentException("operatorName must not be null or empty");
        }
        this.operatorName = operatorName;
        this.operatorId = operatorId;
        tupleCounter = new Counter("tupleCounter");
        timeCounter = new Counter("timeCounter");
        pageReads = new Counter("diskIoCounter");
        coldReadCounter = new Counter("coldReadCounter");
        avgTupleSz = new Counter("avgTupleSz");
        minTupleSz = new Counter("minTupleSz");
        maxTupleSz = new Counter("maxTupleSz");
        inputTupleCounter = new Counter("inputTupleCounter");
        level = new Counter("level");
        bytesRead = new Counter("bytesRead");
        bytesWritten = new Counter("bytesWritten");
        level.set(-1);
        indexesStats = new HashMap<>();
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
    public ICounter getPageReads() {
        return pageReads;
    }

    @Override
    public ICounter coldReadCounter() {
        return coldReadCounter;
    }

    @Override
    public ICounter getAverageTupleSz() {
        return avgTupleSz;
    }

    @Override
    public ICounter getMaxTupleSz() {
        return maxTupleSz;
    }

    @Override
    public ICounter getMinTupleSz() {
        return minTupleSz;
    }

    @Override
    public ICounter getInputTupleCounter() {
        return inputTupleCounter;
    }

    @Override
    public ICounter getLevel() {
        return level;
    }

    @Override
    public ICounter getBytesRead() {
        return bytesRead;
    }

    @Override
    public ICounter getBytesWritten() {
        return bytesWritten;
    }

    @Override
    public String getOperatorId() {
        return operatorId;
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
        pageReads.update(stats.getPageReads().get());
        updateIndexesStats(stats.getIndexesStats());
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeUTF(operatorName);
        output.writeUTF(operatorId);
        output.writeLong(tupleCounter.get());
        output.writeLong(timeCounter.get());
        output.writeLong(pageReads.get());
        output.writeLong(coldReadCounter.get());
        output.writeLong(avgTupleSz.get());
        output.writeLong(minTupleSz.get());
        output.writeLong(maxTupleSz.get());
        output.writeLong(inputTupleCounter.get());
        output.writeLong(level.get());
        output.writeLong(bytesRead.get());
        output.writeLong(bytesWritten.get());
        writeIndexesStats(output);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        tupleCounter.set(input.readLong());
        timeCounter.set(input.readLong());
        pageReads.set(input.readLong());
        coldReadCounter.set(input.readLong());
        avgTupleSz.set(input.readLong());
        minTupleSz.set(input.readLong());
        maxTupleSz.set(input.readLong());
        inputTupleCounter.set(input.readLong());
        level.set(input.readLong());
        bytesRead.set(input.readLong());
        bytesWritten.set(input.readLong());
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
        return "{ " + "\"operatorName\": \"" + operatorName + "\", " + "\"id\": \"" + operatorId + "\", " + "\""
                + tupleCounter.getName() + "\": " + tupleCounter.get() + ", \"" + timeCounter.getName() + "\": "
                + timeCounter.get() + ", \"" + coldReadCounter.getName() + "\": " + coldReadCounter.get()
                + avgTupleSz.getName() + "\": " + avgTupleSz.get() + ", \"" + minTupleSz.getName() + "\": "
                + minTupleSz.get() + ", \"" + minTupleSz.getName() + "\": " + timeCounter.get() + ", \""
                + inputTupleCounter.getName() + "\": " + bytesRead.get() + ", \"" + bytesRead.getName() + "\": "
                + bytesWritten.get() + ", \"" + bytesWritten.getName() + "\": " + inputTupleCounter.get() + ", \""
                + level.getName() + "\": " + level.get() + ", \"indexStats\": \"" + indexesStats + "\" }";
    }
}
