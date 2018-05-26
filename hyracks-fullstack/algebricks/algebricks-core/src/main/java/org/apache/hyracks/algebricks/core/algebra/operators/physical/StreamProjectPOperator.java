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
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamProjectRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class StreamProjectPOperator extends AbstractPropagatePropertiesForUsedVariablesPOperator {

    private boolean flushFramesRapidly;

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.STREAM_PROJECT;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        return emptyUnaryRequirements();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        ProjectOperator project = (ProjectOperator) op;
        int[] projectionList = new int[project.getVariables().size()];
        int i = 0;
        for (LogicalVariable v : project.getVariables()) {
            int pos = inputSchemas[0].findVariable(v);
            if (pos < 0) {
                throw new AlgebricksException("Could not find variable " + v + ".");
            }
            projectionList[i++] = pos;
        }
        StreamProjectRuntimeFactory runtime = new StreamProjectRuntimeFactory(projectionList, flushFramesRapidly);
        runtime.setSourceLocation(project.getSourceLocation());
        RecordDescriptor recDesc =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);
        builder.contributeMicroOperator(project, runtime, recDesc);
        ILogicalOperator src = project.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, project, 0);
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        ProjectOperator p = (ProjectOperator) op;
        computeDeliveredPropertiesForUsedVariables(p, p.getVariables());
    }

    public void setRapidFrameFlush(boolean flushFramesRapidly) {
        this.flushFramesRapidly = flushFramesRapidly;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }
}
