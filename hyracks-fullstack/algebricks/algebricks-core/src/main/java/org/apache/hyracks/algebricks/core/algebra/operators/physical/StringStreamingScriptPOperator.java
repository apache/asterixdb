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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.scripting.IScriptDescription;
import org.apache.hyracks.algebricks.core.algebra.scripting.IScriptDescription.ScriptKind;
import org.apache.hyracks.algebricks.core.algebra.scripting.StringStreamingScriptDescription;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.operators.std.StringStreamingRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class StringStreamingScriptPOperator extends AbstractPropagatePropertiesForUsedVariablesPOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.STRING_STREAM_SCRIPT;
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
        ScriptOperator scriptOp = (ScriptOperator) op;
        IScriptDescription scriptDesc = scriptOp.getScriptDescription();
        if (scriptDesc.getKind() != ScriptKind.STRING_STREAMING) {
            throw new IllegalStateException();
        }
        StringStreamingScriptDescription sssd = (StringStreamingScriptDescription) scriptDesc;
        StringStreamingRuntimeFactory runtime = new StringStreamingRuntimeFactory(sssd.getCommand(),
                sssd.getPrinterFactories(), sssd.getFieldDelimiter(), sssd.getParserFactory());
        runtime.setSourceLocation(scriptOp.getSourceLocation());
        RecordDescriptor recDesc =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);
        builder.contributeMicroOperator(scriptOp, runtime, recDesc);
        // and contribute one edge from its child
        ILogicalOperator src = scriptOp.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, scriptOp, 0);
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        ScriptOperator s = (ScriptOperator) op;
        computeDeliveredPropertiesForUsedVariables(s, s.getInputVariables());
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }
}
