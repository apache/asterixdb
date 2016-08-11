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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.value.IRangeMap;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.RangeId;
import org.apache.hyracks.dataflow.std.misc.RangeForwardOperatorDescriptor;

public class RangeForwardPOperator extends AbstractPhysicalOperator {

    private RangeId rangeId;
    private IRangeMap rangeMap;

    public RangeForwardPOperator(RangeId rangeId, IRangeMap rangeMap) {
        // Use when a range hint is provided.
        this.rangeId = rangeId;
        this.rangeMap = rangeMap;
    }

    public RangeForwardPOperator(RangeId rangeId) {
        this(rangeId, null);
    }

    public IRangeMap getRangeMap() {
        return rangeMap;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.RANGE_FORWARD;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = (StructuralPropertiesVector) op2.getDeliveredPhysicalProperties().clone();
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
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        RecordDescriptor recordDescriptor = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema,
                context);
        RangeForwardOperatorDescriptor opDesc = new RangeForwardOperatorDescriptor(spec, rangeId, rangeMap,
                recordDescriptor);
        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);
        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + " " + rangeId;
    }

}
