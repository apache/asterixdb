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

import static org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractWindowPOperator.getOrderRequirement;

import java.util.Collections;
import java.util.List;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.metadata.IWriteDataSink;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;

public class SinkWritePOperator extends AbstractPhysicalOperator {
    private final LogicalVariable sourceVariable;
    private final List<LogicalVariable> partitionVariables;
    private final List<OrderColumn> orderColumns;

    public SinkWritePOperator(LogicalVariable sourceVariable, List<LogicalVariable> partitionVariables,
            List<OrderColumn> orderColumns) {
        this.sourceVariable = sourceVariable;
        this.partitionVariables = partitionVariables;
        this.orderColumns = orderColumns;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.SINK_WRITE;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        ILogicalOperator op2 = op.getInputs().get(0).getValue();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqByParent, IOptimizationContext context) throws AlgebricksException {
        if (partitionVariables.isEmpty()) {
            return emptyUnaryRequirements();
        }
        IPartitioningProperty pp;
        switch (op.getExecutionMode()) {
            case PARTITIONED:
                pp = UnorderedPartitionedProperty.of(new ListSet<>(partitionVariables),
                        context.getComputationNodeDomain());
                break;
            case UNPARTITIONED:
                pp = IPartitioningProperty.UNPARTITIONED;
                break;
            case LOCAL:
                pp = null;
                break;
            default:
                throw new IllegalStateException(op.getExecutionMode().name());
        }

        List<OrderColumn> finalOrderColumns =
                getOrderRequirement(op, ErrorCode.UNSUPPORTED_WRITE_SPEC, partitionVariables, orderColumns);

        List<ILocalStructuralProperty> localProps =
                Collections.singletonList(new LocalOrderProperty(finalOrderColumns));
        return new PhysicalRequirements(
                new StructuralPropertiesVector[] { new StructuralPropertiesVector(pp, localProps) },
                IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        WriteOperator write = (WriteOperator) op;
        IExpressionRuntimeProvider runtimeProvider = context.getExpressionRuntimeProvider();
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(op);
        IOperatorSchema schema = inputSchemas[0];
        IWriteDataSink writeDataSink = write.getWriteDataSink();

        // Source evaluator column
        int sourceColumn = schema.findVariable(sourceVariable);

        // Path expression
        IScalarEvaluatorFactory dynamicPathEvalFactory = null;
        ILogicalExpression staticPathExpr = null;
        ILogicalExpression pathExpr = write.getPathExpression().getValue();
        if (pathExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            dynamicPathEvalFactory = runtimeProvider.createEvaluatorFactory(pathExpr, typeEnv, inputSchemas, context);
        } else {
            staticPathExpr = pathExpr;
        }

        // Partition columns
        int[] partitionColumns = JobGenHelper.projectVariables(schema, partitionVariables);
        IBinaryComparatorFactory[] partitionComparatorFactories =
                JobGenHelper.variablesToAscBinaryComparatorFactories(partitionVariables, typeEnv, context);

        RecordDescriptor recDesc =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);
        RecordDescriptor inputDesc = JobGenHelper.mkRecordDescriptor(
                context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas[0], context);

        IMetadataProvider<?, ?> mp = context.getMetadataProvider();

        Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> runtimeAndConstraints = mp.getWriteFileRuntime(
                sourceColumn, partitionColumns, partitionComparatorFactories, dynamicPathEvalFactory, staticPathExpr,
                pathExpr.getSourceLocation(), writeDataSink, inputDesc, typeEnv.getVarType(sourceVariable));
        IPushRuntimeFactory runtime = runtimeAndConstraints.first;
        runtime.setSourceLocation(write.getSourceLocation());

        builder.contributeMicroOperator(write, runtime, recDesc, runtimeAndConstraints.second);
        ILogicalOperator src = write.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, write, 0);
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }
}
