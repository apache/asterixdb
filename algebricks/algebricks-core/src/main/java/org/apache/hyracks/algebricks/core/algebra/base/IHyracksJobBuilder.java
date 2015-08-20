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
package edu.uci.ics.hyracks.algebricks.core.algebra.base;

import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public interface IHyracksJobBuilder {
    public enum TargetConstraint {
        ONE,
        SAME_COUNT
    }

    public void contributeHyracksOperator(ILogicalOperator op, IOperatorDescriptor opDesc);

    public void contributeAlgebricksPartitionConstraint(IOperatorDescriptor opDesc, AlgebricksPartitionConstraint apc);

    public void contributeMicroOperator(ILogicalOperator op, IPushRuntimeFactory runtime, RecordDescriptor recDesc);

    public void contributeMicroOperator(ILogicalOperator op, IPushRuntimeFactory runtime, RecordDescriptor recDesc,
            AlgebricksPartitionConstraint pc);

    /**
     * inputs are numbered starting from 0
     */
    public void contributeGraphEdge(ILogicalOperator src, int srcOutputIndex, ILogicalOperator dest, int destInputIndex);

    public void contributeConnector(ILogicalOperator exchgOp, IConnectorDescriptor conn);

    public void contributeConnectorWithTargetConstraint(ILogicalOperator exchgOp, IConnectorDescriptor conn,
            TargetConstraint numberOfTargetPartitions);

    public JobSpecification getJobSpec();

    /**
     * to be called only after all the graph information is added
     */
    public void buildSpec(List<ILogicalOperator> roots) throws AlgebricksException;
}
