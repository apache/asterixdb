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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.NestedPlansAccumulatingAggregatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.NestedPlansRunningAggregatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.group.AbstractAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupOperatorDescriptor;

public class PreclusteredGroupByPOperator extends AbstractPreclusteredGroupByPOperator {

    private final boolean groupAll;

    public PreclusteredGroupByPOperator(List<LogicalVariable> columnList, boolean groupAll) {
        super(columnList);
        this.groupAll = groupAll;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.PRE_CLUSTERED_GROUP_BY;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        GroupByOperator gby = (GroupByOperator) op;
        checkGroupAll(gby);
        int keys[] = JobGenHelper.variablesToFieldIndexes(columnList, inputSchemas[0]);
        int fdColumns[] = getFdColumns(gby, inputSchemas[0]);
        // compile subplans and set the gby op. schema accordingly
        AlgebricksPipeline[] subplans = compileSubplans(inputSchemas[0], gby, opSchema, context);
        AbstractAggregatorDescriptorFactory aggregatorFactory;

        List<ILogicalPlan> nestedPlans = gby.getNestedPlans();
        if (!nestedPlans.isEmpty() && nestedPlans.get(0).getRoots().get(0).getValue()
                .getOperatorTag() == LogicalOperatorTag.RUNNINGAGGREGATE) {
            aggregatorFactory = new NestedPlansRunningAggregatorFactory(subplans, keys, fdColumns);
        } else {
            aggregatorFactory = new NestedPlansAccumulatingAggregatorFactory(subplans, keys, fdColumns);
        }
        aggregatorFactory.setSourceLocation(gby.getSourceLocation());

        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        IBinaryComparatorFactory[] comparatorFactories = JobGenHelper
                .variablesToAscBinaryComparatorFactories(columnList, context.getTypeEnvironment(op), context);
        RecordDescriptor recordDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);

        int framesLimit = localMemoryRequirements.getMemoryBudgetInFrames();
        PreclusteredGroupOperatorDescriptor opDesc = new PreclusteredGroupOperatorDescriptor(spec, keys,
                comparatorFactories, aggregatorFactory, recordDescriptor, groupAll, framesLimit);
        opDesc.setSourceLocation(gby.getSourceLocation());

        contributeOpDesc(builder, gby, opDesc);

        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + (groupAll ? "(ALL)" : "") + columnList;
    }
}
