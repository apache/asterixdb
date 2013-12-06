/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.pregelix.api.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.job.IIterationCompleteReporterHook;
import edu.uci.ics.pregelix.api.job.PregelixJob;

/**
 * Hook that aggregates a mapreduce.Counters object across all
 * iterations of a job, saving to HDFS
 * 
 * @author wbiesing
 */
public class HadoopCountersGlobalAggregateHook implements IIterationCompleteReporterHook {

    @Override
    public void completeIteration(int superstep, PregelixJob job) throws HyracksDataException {
        Configuration conf = job.getConfiguration();
        String jobId = BspUtils.getJobId(conf);
        Class<?> aggClass = conf.getClass(PregelixJob.COUNTERS_AGGREGATOR_CLASS, null);
        if (aggClass == null)
            throw new HyracksDataException(
                    "A subclass of HadoopCountersAggregator must active for GlobalAggregateCountersHook to operate!");
        Counters curIterCounters;
        try {
            curIterCounters = (Counters) BspUtils.readGlobalAggregateValue(conf, jobId, aggClass.getName());
        } catch (IllegalStateException e) {
            throw new HyracksDataException(
                    "A subclass of HadoopCountersAggregator must active for GlobalAggregateCountersHook to operate!", e);
        }
        if (superstep > 1) {
            Counters prevCounters = BspUtils.readCounters(superstep - 1, conf, jobId); // the counters from the previous iterations, all aggregated together
            curIterCounters.incrAllCounters(prevCounters); // add my counters to previous ones
        }
        BspUtils.writeCounters(curIterCounters, superstep, conf, jobId);
        BspUtils.writeCountersLastIteration(superstep, conf, jobId);
    }
}
