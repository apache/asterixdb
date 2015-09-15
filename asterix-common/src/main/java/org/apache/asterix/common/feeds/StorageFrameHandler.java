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
package org.apache.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.asterix.common.feeds.FeedConstants.StatisticsConstants;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class StorageFrameHandler {

    private final Map<Integer, Map<Integer, IntakePartitionStatistics>> intakeStatistics;
    private long avgDelayPersistence;

    public StorageFrameHandler() {
        intakeStatistics = new HashMap<Integer, Map<Integer, IntakePartitionStatistics>>();
        avgDelayPersistence = 0L;
    }

    public synchronized void updateTrackingInformation(ByteBuffer frame, FrameTupleAccessor frameAccessor) {
        int nTuples = frameAccessor.getTupleCount();
        long delay = 0;
        long intakeTimestamp;
        long currentTime = System.currentTimeMillis();
        int partition = 0;
        int recordId = 0;
        for (int i = 0; i < nTuples; i++) {
            int recordStart = frameAccessor.getTupleStartOffset(i) + frameAccessor.getFieldSlotsLength();
            int openPartOffsetOrig = frame.getInt(recordStart + 6);
            int numOpenFields = frame.getInt(recordStart + openPartOffsetOrig);

            int recordIdOffset = openPartOffsetOrig + 4 + 8 * numOpenFields
                    + (StatisticsConstants.INTAKE_TUPLEID.length() + 2) + 1;
            recordId = frame.getInt(recordStart + recordIdOffset);

            int partitionOffset = recordIdOffset + 4 + (StatisticsConstants.INTAKE_PARTITION.length() + 2) + 1;
            partition = frame.getInt(recordStart + partitionOffset);

            ackRecordId(partition, recordId);
            int intakeTimestampValueOffset = partitionOffset + 4 + (StatisticsConstants.INTAKE_TIMESTAMP.length() + 2)
                    + 1;
            intakeTimestamp = frame.getLong(recordStart + intakeTimestampValueOffset);

            int storeTimestampValueOffset = intakeTimestampValueOffset + 8
                    + (StatisticsConstants.STORE_TIMESTAMP.length() + 2) + 1;
            frame.putLong(recordStart + storeTimestampValueOffset, System.currentTimeMillis());
            delay += currentTime - intakeTimestamp;
        }
        avgDelayPersistence = delay / nTuples;
    }

    private void ackRecordId(int partition, int recordId) {
        Map<Integer, IntakePartitionStatistics> map = intakeStatistics.get(partition);
        if (map == null) {
            map = new HashMap<Integer, IntakePartitionStatistics>();
            intakeStatistics.put(partition, map);
        }
        int base = (int) Math.ceil(recordId * 1.0 / IntakePartitionStatistics.ACK_WINDOW_SIZE);
        IntakePartitionStatistics intakeStatsForBaseOfPartition = map.get(base);
        if (intakeStatsForBaseOfPartition == null) {
            intakeStatsForBaseOfPartition = new IntakePartitionStatistics(partition, base);
            map.put(base, intakeStatsForBaseOfPartition);
        }
        intakeStatsForBaseOfPartition.ackRecordId(recordId);
    }

    public byte[] getAckData(int partition, int base) {
        Map<Integer, IntakePartitionStatistics> intakeStats = intakeStatistics.get(partition);
        if (intakeStats != null) {
            IntakePartitionStatistics intakePartitionStats = intakeStats.get(base);
            if (intakePartitionStats != null) {
                return intakePartitionStats.getAckInfo();
            }
        }
        return null;
    }

    public synchronized Map<Integer, IntakePartitionStatistics> getBaseAcksForPartition(int partition) {
        Map<Integer, IntakePartitionStatistics> intakeStatsForPartition = intakeStatistics.get(partition);
        Map<Integer, IntakePartitionStatistics> clone = new HashMap<Integer, IntakePartitionStatistics>();
        for (Entry<Integer, IntakePartitionStatistics> entry : intakeStatsForPartition.entrySet()) {
            clone.put(entry.getKey(), entry.getValue());
        }
        return intakeStatsForPartition;
    }

    public long getAvgDelayPersistence() {
        return avgDelayPersistence;
    }

    public void setAvgDelayPersistence(long avgDelayPersistence) {
        this.avgDelayPersistence = avgDelayPersistence;
    }

    public Set<Integer> getPartitionsWithStats() {
        return intakeStatistics.keySet();
    }
}
