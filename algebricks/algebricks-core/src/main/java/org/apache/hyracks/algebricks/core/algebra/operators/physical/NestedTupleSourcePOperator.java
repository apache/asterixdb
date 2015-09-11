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
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.operators.std.NestedTupleSourceRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class NestedTupleSourcePOperator extends AbstractPhysicalOperator {

    public NestedTupleSourcePOperator() {
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.NESTED_TUPLE_SOURCE;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        Mutable<ILogicalOperator> dataSource = ((NestedTupleSourceOperator) op).getDataSourceReference();
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) dataSource.getValue().getInputs().get(0).getValue();
        IPhysicalPropertiesVector inheritedProps = op2.getDeliveredPhysicalProperties();
        AbstractLogicalOperator parent = (AbstractLogicalOperator) dataSource.getValue();
        if (parent.getOperatorTag() == LogicalOperatorTag.GROUP) {
            // The following part computes the data property regarding to each particular group.
            // TODO(buyingyi): we need to add the original data property as well. But currently
            // there are places assuming there is only one LocalOrderProperty and one
            // LocalGroupingProperty delivered by an operator.
            GroupByOperator gby = (GroupByOperator) parent;
            List<ILocalStructuralProperty> originalLocalProperties = inheritedProps.getLocalProperties();
            List<ILocalStructuralProperty> newLocalProperties = null;
            if (originalLocalProperties != null) {
                newLocalProperties = new ArrayList<ILocalStructuralProperty>();
                for (ILocalStructuralProperty lsp : inheritedProps.getLocalProperties()) {
                    ILocalStructuralProperty newLsp = lsp.regardToGroup(gby.getGbyVarList());
                    if (newLsp != null) {
                        newLocalProperties.add(newLsp);
                    }
                }
            }
            deliveredProperties = new StructuralPropertiesVector(inheritedProps.getPartitioningProperty(),
                    newLocalProperties);
        } else {
            deliveredProperties = inheritedProps.clone();
        }
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        return null;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        propagatedSchema.addAllVariables(outerPlanSchema);
        NestedTupleSourceRuntimeFactory runtime = new NestedTupleSourceRuntimeFactory();
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema,
                context);
        builder.contributeMicroOperator(op, runtime, recDesc);
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }
}
