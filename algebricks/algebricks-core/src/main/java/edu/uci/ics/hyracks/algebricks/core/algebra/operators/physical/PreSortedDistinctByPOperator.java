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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.ListSet;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.OrderColumn;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.aggreg.SimpleAlgebricksAccumulatingAggregatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.preclustered.PreclusteredGroupOperatorDescriptor;

public class PreSortedDistinctByPOperator extends AbstractPhysicalOperator {

    private List<LogicalVariable> columnList;

    public PreSortedDistinctByPOperator(List<LogicalVariable> columnList) {
        this.columnList = columnList;
    }

    public void setDistinctByColumns(List<LogicalVariable> distinctByColumns) {
        this.columnList = distinctByColumns;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        IPartitioningProperty pp = op2.getDeliveredPhysicalProperties().getPartitioningProperty();
        List<ILocalStructuralProperty> propsLocal = op2.getDeliveredPhysicalProperties().getLocalProperties();
        deliveredProperties = new StructuralPropertiesVector(pp, propsLocal);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
        List<ILocalStructuralProperty> localProps = new ArrayList<ILocalStructuralProperty>(columnList.size());
        for (LogicalVariable column : columnList) {
            localProps.add(new LocalOrderProperty(new OrderColumn(column, OrderKind.ASC)));
        }
        IPartitioningProperty pp = null;
        AbstractLogicalOperator aop = (AbstractLogicalOperator) op;
        if (aop.getExecutionMode() == ExecutionMode.PARTITIONED) {
            pp = new UnorderedPartitionedProperty(new ListSet<LogicalVariable>(columnList), null);
        }
        pv[0] = new StructuralPropertiesVector(pp, localProps);
        return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {

        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        int keys[] = JobGenHelper.variablesToFieldIndexes(columnList, inputSchemas[0]);
        int sz = inputSchemas[0].getSize();
        int fdSz = sz - columnList.size();
        int[] fdColumns = new int[fdSz];
        int j = 0;
        for (LogicalVariable v : inputSchemas[0]) {
            if (!columnList.contains(v)) {
                fdColumns[j++] = inputSchemas[0].findVariable(v);
            }
        }
        int[] keysAndDecs = new int[keys.length + fdColumns.length];
        for (int i = 0; i < keys.length; i++) {
            keysAndDecs[i] = keys[i];
        }
        for (int i = 0; i < fdColumns.length; i++) {
            keysAndDecs[i + keys.length] = fdColumns[i];
        }

        IBinaryComparatorFactory[] comparatorFactories = JobGenHelper.variablesToAscBinaryComparatorFactories(
                columnList, context.getTypeEnvironment(op), context);
        IAggregateEvaluatorFactory[] aggFactories = new IAggregateEvaluatorFactory[] {};
        IAggregatorDescriptorFactory aggregatorFactory = new SimpleAlgebricksAccumulatingAggregatorFactory(
                aggFactories, keysAndDecs);

        RecordDescriptor recordDescriptor = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema,
                context);
        /** make fd columns part of the key but the comparator only compares the distinct key columns */
        PreclusteredGroupOperatorDescriptor opDesc = new PreclusteredGroupOperatorDescriptor(spec, keysAndDecs,
                comparatorFactories, aggregatorFactory, recordDescriptor);

        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);

        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.PRE_SORTED_DISTINCT_BY;
    }

}
