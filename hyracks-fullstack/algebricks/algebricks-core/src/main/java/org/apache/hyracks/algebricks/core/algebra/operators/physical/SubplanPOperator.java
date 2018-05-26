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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.operators.meta.SubplanRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class SubplanPOperator extends AbstractPhysicalOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.SUBPLAN;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        IPhysicalPropertiesVector childsProperties = op2.getPhysicalOperator().getDeliveredProperties();
        List<ILocalStructuralProperty> propsLocal = new ArrayList<>();
        if (childsProperties.getLocalProperties() != null) {
            propsLocal.addAll(childsProperties.getLocalProperties());
        }
        // ... get local properties for newly created variables...
        SubplanOperator subplan = (SubplanOperator) op;
        for (ILogicalPlan plan : subplan.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : plan.getRoots()) {
                AbstractLogicalOperator rOp = (AbstractLogicalOperator) r.getValue();
                propsLocal.addAll(rOp.getPhysicalOperator().getDeliveredProperties().getLocalProperties());
            }
        }

        deliveredProperties = new StructuralPropertiesVector(childsProperties.getPartitioningProperty(), propsLocal);
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
        SubplanOperator subplan = (SubplanOperator) op;
        if (subplan.getNestedPlans().size() != 1) {
            throw new NotImplementedException("Subplan currently works only for one nested plan with one root.");
        }
        List<List<AlgebricksPipeline>> subplans = compileSubplansImpl(inputSchemas[0], subplan, opSchema, context);
        assert subplans.size() == 1;
        List<AlgebricksPipeline> np = subplans.get(0);
        RecordDescriptor inputRecordDesc = JobGenHelper.mkRecordDescriptor(
                context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas[0], context);
        IMissingWriterFactory[] missingWriterFactories = new IMissingWriterFactory[np.get(0).getOutputWidth()];
        for (int i = 0; i < missingWriterFactories.length; i++) {
            missingWriterFactories[i] = context.getMissingWriterFactory();
        }
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);
        SubplanRuntimeFactory runtime =
                new SubplanRuntimeFactory(np, missingWriterFactories, inputRecordDesc, recDesc, null);
        runtime.setSourceLocation(subplan.getSourceLocation());
        builder.contributeMicroOperator(subplan, runtime, recDesc);

        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }
}
