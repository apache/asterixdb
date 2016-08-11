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
package org.apache.asterix.algebra.operators.physical;

import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.RangeId;

public class IntervalLocalRangeSplitterPOperator extends AbstractPhysicalOperator {

    private List<LogicalVariable> intervalFields;
    private RangeId rangeId;

    public IntervalLocalRangeSplitterPOperator(List<LogicalVariable> intervalFields, RangeId rangeId) {
        this.intervalFields = intervalFields;
        this.rangeId = rangeId;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.EXTENSION_OPERATOR;
    }

    @Override
    public String toString() {
        return "IntervalLocalRangeSplitterPOperator " + intervalFields;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        return emptyUnaryRequirements();
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = (StructuralPropertiesVector) op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        RecordDescriptor recDescriptor = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op),
                propagatedSchema, context);

        int[] keys = JobGenHelper.variablesToFieldIndexes(intervalFields, inputSchemas[0]);

        IOperatorDescriptor opDesc = new IntervalLocalRangeOperatorDescriptor(spec, keys, recDescriptor, rangeId);
        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);
        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    @Override
    public Pair<int[], int[]> getInputOutputDependencyLabels(ILogicalOperator op) {
        ReplicateOperator rop = (ReplicateOperator) op;
        int[] inputDependencyLabels = new int[] { 0 };
        int[] outputDependencyLabels = new int[rop.getOutputArity()]; // filled with 0's
        return new Pair<>(inputDependencyLabels, outputDependencyLabels);
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }
}
