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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamLimitRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class StreamLimitPOperator extends AbstractPhysicalOperator {

    public StreamLimitPOperator() {

    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.STREAM_LIMIT;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator limitOp = (AbstractLogicalOperator) op;
        ILogicalOperator op2 = op.getInputs().get(0).getValue();
        if (limitOp.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.UNPARTITIONED) {
            //partitioning property: unpartitioned;  local property: whatever from the child
            deliveredProperties = new StructuralPropertiesVector(IPartitioningProperty.UNPARTITIONED,
                    op2.getDeliveredPhysicalProperties().getLocalProperties());
        } else {
            deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
        }
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        AbstractLogicalOperator limitOp = (AbstractLogicalOperator) op;
        if (limitOp.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.UNPARTITIONED) {
            StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
            pv[0] = new StructuralPropertiesVector(IPartitioningProperty.UNPARTITIONED, null);
            return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
        } else {
            return emptyUnaryRequirements();
        }
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        LimitOperator limit = (LimitOperator) op;
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        IScalarEvaluatorFactory maxObjectsFact = expressionRuntimeProvider
                .createEvaluatorFactory(limit.getMaxObjects().getValue(), env, inputSchemas, context);
        ILogicalExpression offsetExpr = limit.getOffset().getValue();
        IScalarEvaluatorFactory offsetFact = (offsetExpr == null) ? null
                : expressionRuntimeProvider.createEvaluatorFactory(offsetExpr, env, inputSchemas, context);
        RecordDescriptor recDesc =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);
        StreamLimitRuntimeFactory runtime = new StreamLimitRuntimeFactory(maxObjectsFact, offsetFact, null,
                context.getBinaryIntegerInspectorFactory());
        runtime.setSourceLocation(limit.getSourceLocation());
        builder.contributeMicroOperator(limit, runtime, recDesc);
        // and contribute one edge from its child
        ILogicalOperator src = limit.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, limit, 0);
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }
}
