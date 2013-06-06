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
package edu.uci.ics.hivesterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.StreamProjectPOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class InsertProjectBeforeWriteRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    /**
     * When the input schema to WriteOperator is different from the output
     * schema in terms of variable order, add a project operator to get the
     * write order
     */
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.WRITE) {
            return false;
        }
        WriteOperator opWrite = (WriteOperator) op;
        ArrayList<LogicalVariable> finalSchema = new ArrayList<LogicalVariable>();
        VariableUtilities.getUsedVariables(opWrite, finalSchema);
        ArrayList<LogicalVariable> inputSchema = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(opWrite, inputSchema);
        if (!isIdentical(finalSchema, inputSchema)) {
            ProjectOperator projectOp = new ProjectOperator(finalSchema);
            Mutable<ILogicalOperator> parentOpRef = opWrite.getInputs().get(0);
            projectOp.getInputs().add(parentOpRef);
            opWrite.getInputs().clear();
            opWrite.getInputs().add(new MutableObject<ILogicalOperator>(projectOp));
            projectOp.setPhysicalOperator(new StreamProjectPOperator());
            projectOp.setExecutionMode(ExecutionMode.PARTITIONED);

            AbstractLogicalOperator op2 = (AbstractLogicalOperator) parentOpRef.getValue();
            if (op2.getOperatorTag() == LogicalOperatorTag.PROJECT) {
                ProjectOperator pi2 = (ProjectOperator) op2;
                parentOpRef.setValue(pi2.getInputs().get(0).getValue());
            }
            context.computeAndSetTypeEnvironmentForOperator(projectOp);
            return true;
        } else
            return false;

    }

    private boolean isIdentical(List<LogicalVariable> finalSchema, List<LogicalVariable> inputSchema) {
        int finalSchemaSize = finalSchema.size();
        int inputSchemaSize = inputSchema.size();
        if (finalSchemaSize != inputSchemaSize)
            throw new IllegalStateException("final output schema variables missing!");
        for (int i = 0; i < finalSchemaSize; i++) {
            LogicalVariable var1 = finalSchema.get(i);
            LogicalVariable var2 = inputSchema.get(i);
            if (!var1.equals(var2))
                return false;
        }
        return true;
    }
}
