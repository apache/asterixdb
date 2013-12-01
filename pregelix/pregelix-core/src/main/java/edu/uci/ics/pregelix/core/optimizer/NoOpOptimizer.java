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

import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.pregelix.core.jobgen.JobGen;
import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;

public class NoOpOptimizer implements IOptimizer {

    @Override
    public JobGen optimize(JobGen jobGen, int iteration) {
        return jobGen;
    }

    @Override
    public void setOptimizedLocationConstraints(JobSpecification spec, IOperatorDescriptor operator) {
        try {
            ClusterConfig.setLocationConstraint(spec, operator);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public IFileSplitProvider getOptimizedFileSplitProvider(String jobId, String indexName) {
        try {
           return ClusterConfig.getFileSplitProvider(jobId, indexName);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
