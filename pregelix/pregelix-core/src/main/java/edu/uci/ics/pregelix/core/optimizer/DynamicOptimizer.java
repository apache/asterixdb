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

package edu.uci.ics.pregelix.core.optimizer;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.client.stats.Counters;
import edu.uci.ics.hyracks.client.stats.IClusterCounterContext;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.pregelix.core.jobgen.JobGen;
import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;

public class DynamicOptimizer implements IOptimizer {

    private IClusterCounterContext counterContext;
    private Map<String, IntWritable> machineToDegreeOfParallelism = new TreeMap<String, IntWritable>();
    private int dop = 0;

    public DynamicOptimizer(IClusterCounterContext counterContext) {
        this.counterContext = counterContext;
    }

    @Override
    public JobGen optimize(JobGen jobGen, int iteration) {
        try {
            if (iteration == 0) {
                initializeLoadPerMachine();
            }
            return jobGen;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void setOptimizedLocationConstraints(JobSpecification spec, IOperatorDescriptor operator) {
        try {
            String[] constraints = new String[dop];
            int index = 0;
            for (Entry<String, IntWritable> entry : machineToDegreeOfParallelism.entrySet()) {
                String loc = entry.getKey();
                IntWritable count = entry.getValue();
                for (int j = 0; j < count.get(); j++) {
                    constraints[index++] = loc;
                }
            }
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, operator, constraints);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public IFileSplitProvider getOptimizedFileSplitProvider(String jobId, String indexName) {
        FileSplit[] fileSplits = new FileSplit[dop];
        String[] stores = ClusterConfig.getStores();
        int splitIndex = 0;
        for (Entry<String, IntWritable> entry : machineToDegreeOfParallelism.entrySet()) {
            String ncName = entry.getKey();
            IntWritable count = entry.getValue();
            for (int j = 0; j < count.get(); j++) {
                //cycles stores, each machine has the number of stores = the number of cores
                int storeCursor = j % stores.length;
                String st = stores[storeCursor];
                FileSplit split = new FileSplit(ncName, st + File.separator + ncName + "-data" + File.separator + jobId
                        + File.separator + indexName + (j / stores.length));
                fileSplits[splitIndex++] = split;
            }
        }
        return new ConstantFileSplitProvider(fileSplits);
    }

    /**
     * initialize the load-per-machine map
     * 
     * @return the degree of parallelism
     * @throws HyracksException
     */
    private int initializeLoadPerMachine() throws HyracksException {
        machineToDegreeOfParallelism.clear();
        String[] locationConstraints = ClusterConfig.getLocationConstraint();
        for (String loc : locationConstraints) {
            machineToDegreeOfParallelism.put(loc, new IntWritable(0));
        }
        dop = 0;
        for (Entry<String, IntWritable> entry : machineToDegreeOfParallelism.entrySet()) {
            String loc = entry.getKey();
            //reserve one core for heartbeat
            int load = (int) counterContext.getCounter(Counters.NUM_PROCESSOR, false).get();
            //load = load > 3 ? load - 2 : load;
            IntWritable count = machineToDegreeOfParallelism.get(loc);
            count.set(load);
            dop += load;
        }
        return dop;
    }

}
