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
package edu.uci.ics.pregelix.dataflow.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.job.IIterationCompleteReporterHook;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;

/**
 * After each iteration, this hook keeps a record of all the global aggregates in each iteration
 * 
 * @author wbiesing
 */
public class PerIterationGlobalAggregatesHook implements IIterationCompleteReporterHook {
    private ArrayList<HashMap<String, Writable>> aggregatesByIteration = new ArrayList<>();

    public List<HashMap<String, Writable>> getAggregatesByIteration() {
        return aggregatesByIteration;
    }

    @Override
    public void completeIteration(int superstep, PregelixJob job) {
        Configuration conf = job.getConfiguration();
        HashMap<String, Writable> aggValues;
        try {
            aggValues = IterationUtils
                    .readAllGlobalAggregateValues(conf, BspUtils.getJobId(conf));
        } catch (HyracksDataException e) {
            throw new RuntimeException(e);
        }
        // forget aggregates that happened later than current iteration (e.g., restarting 
        // after checkpointing) by trimming the array to size() == superstep - 1
        while (aggregatesByIteration.size() > superstep) {
            aggregatesByIteration.remove(aggregatesByIteration.size() - 1);
        }
        aggregatesByIteration.add(aggValues);
    }
}
