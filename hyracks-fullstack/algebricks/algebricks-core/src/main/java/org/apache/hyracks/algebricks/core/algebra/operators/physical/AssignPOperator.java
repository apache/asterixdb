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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class AssignPOperator extends AbstractPhysicalOperator {

    private boolean flushFramesRapidly;
    private String[] locations;

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.ASSIGN;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AssignOperator assignOp = (AssignOperator) op;
        ILogicalOperator op2 = op.getInputs().get(0).getValue();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
        if (assignOp.getExplicitOrderingProperty() != null) {
            deliveredProperties.getLocalProperties().add(assignOp.getExplicitOrderingProperty());
        }
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        return emptyUnaryRequirements();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        AssignOperator assign = (AssignOperator) op;
        List<LogicalVariable> variables = assign.getVariables();
        List<Mutable<ILogicalExpression>> expressions = assign.getExpressions();
        int[] outColumns = new int[variables.size()];
        for (int i = 0; i < outColumns.length; i++) {
            outColumns[i] = opSchema.findVariable(variables.get(i));
        }
        IScalarEvaluatorFactory[] evalFactories = new IScalarEvaluatorFactory[expressions.size()];
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        for (int i = 0; i < evalFactories.length; i++) {
            evalFactories[i] = expressionRuntimeProvider.createEvaluatorFactory(expressions.get(i).getValue(),
                    context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas, context);
        }

        // TODO push projections into the operator
        int[] projectionList = JobGenHelper.projectAllVariables(opSchema);

        AssignRuntimeFactory runtime =
                new AssignRuntimeFactory(outColumns, evalFactories, projectionList, flushFramesRapidly);
        runtime.setSourceLocation(assign.getSourceLocation());

        // contribute one Asterix framewriter
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);
        if (locations != null && locations.length > 0) {
            AlgebricksAbsolutePartitionConstraint locationConstraint =
                    new AlgebricksAbsolutePartitionConstraint(locations);
            builder.contributeMicroOperator(assign, runtime, recDesc, locationConstraint);
        } else {
            builder.contributeMicroOperator(assign, runtime, recDesc);
        }
        // and contribute one edge from its child
        ILogicalOperator src = assign.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, assign, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    public void setRapidFrameFlush(boolean flushFramesRapidly) {
        this.flushFramesRapidly = flushFramesRapidly;
    }

    public void setLocationConstraint(String[] locations) {
        this.locations = locations;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }
}
