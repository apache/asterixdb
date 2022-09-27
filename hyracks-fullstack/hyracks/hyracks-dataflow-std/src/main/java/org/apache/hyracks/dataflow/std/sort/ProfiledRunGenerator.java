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
package org.apache.hyracks.dataflow.std.sort;

import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.ProfiledFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.OperatorStats;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;

public class ProfiledRunGenerator extends ProfiledFrameWriter implements IRunGenerator {

    private final IRunGenerator runGenerator;

    private ProfiledRunGenerator(IRunGenerator runGenerator, IStatsCollector collector, String name,
            IOperatorStats stats, ActivityId root) {
        super(runGenerator, collector, name, stats, null);
        this.runGenerator = runGenerator;
    }

    @Override
    public List<GeneratedRunFileReader> getRuns() {
        return runGenerator.getRuns();
    }

    @Override
    public ISorter getSorter() {
        return runGenerator.getSorter();
    }

    public static IRunGenerator time(IRunGenerator runGenerator, IHyracksTaskContext ctx, String name, ActivityId root)
            throws HyracksDataException {
        if (!(runGenerator instanceof ProfiledRunGenerator)) {
            String statName = root.toString() + " - " + name;
            IStatsCollector statsCollector = ctx.getStatsCollector();
            IOperatorStats stats = new OperatorStats(statName);
            statsCollector.add(stats);
            return new ProfiledRunGenerator(runGenerator, ctx.getStatsCollector(), name, stats, root);
        }
        return runGenerator;
    }
}
