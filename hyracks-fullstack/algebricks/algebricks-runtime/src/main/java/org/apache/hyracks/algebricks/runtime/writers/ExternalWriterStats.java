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
package org.apache.hyracks.algebricks.runtime.writers;

import java.util.UUID;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.OperatorStats;

/**
 * Counts the records an external sink writes, on the task's stats collector, so that the count reaches the cluster
 * controller with the task's profile whether or not the job is profiled. A sink's own stats are separate from the
 * stats every operator reports, whose input tuple counts are a job's processed objects, so that writing a record does
 * not count as processing it a second time.
 */
public final class ExternalWriterStats {

    /** The operator id of every sink's stats, by which {@link #getWrittenRecords} finds them among a task's stats */
    public static final String OPERATOR_ID = "external-writer";

    private ExternalWriterStats() {
    }

    /**
     * @return new stats for a sink running in the given task, on which the sink counts each record written with
     *         {@link IOperatorStats#getTupleCounter()}
     */
    public static IOperatorStats register(IHyracksTaskContext ctx) throws HyracksDataException {
        // each sink of a task needs a name of its own, as the collector refuses a second stats of the same name
        IOperatorStats stats = new OperatorStats(OPERATOR_ID + "-" + UUID.randomUUID(), OPERATOR_ID);
        ctx.getStatsCollector().add(stats);
        return stats;
    }

    /** @return the records written by the sinks whose stats the given task collected */
    public static long getWrittenRecords(IStatsCollector statsCollector) {
        long written = 0;
        for (IOperatorStats stats : statsCollector.getAllOperatorStats().values()) {
            if (OPERATOR_ID.equals(stats.getOperatorId())) {
                written += stats.getTupleCounter().get();
            }
        }
        return written;
    }
}
