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

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.scripting.IScriptDescription;
import edu.uci.ics.hyracks.algebricks.core.algebra.scripting.IScriptDescription.ScriptKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.scripting.StringStreamingScriptDescription;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.StringStreamingRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

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
            IPhysicalPropertiesVector reqdByParent) {
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
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema,
                context);
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

}
