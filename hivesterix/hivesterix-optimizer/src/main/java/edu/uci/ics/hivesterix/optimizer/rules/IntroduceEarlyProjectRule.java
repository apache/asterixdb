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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroduceEarlyProjectRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.PROJECT) {
            return false;
        }
        AbstractLogicalOperator middleOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        List<LogicalVariable> deliveredVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> usedVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> producedVars = new ArrayList<LogicalVariable>();

        VariableUtilities.getUsedVariables(op, deliveredVars);
        VariableUtilities.getUsedVariables(middleOp, usedVars);
        VariableUtilities.getProducedVariables(middleOp, producedVars);

        Set<LogicalVariable> requiredVariables = new HashSet<LogicalVariable>();
        requiredVariables.addAll(deliveredVars);
        requiredVariables.addAll(usedVars);
        requiredVariables.removeAll(producedVars);

        if (middleOp.getInputs().size() <= 0 || middleOp.getInputs().size() > 1)
            return false;

        AbstractLogicalOperator targetOp = (AbstractLogicalOperator) middleOp.getInputs().get(0).getValue();
        if (targetOp.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN)
            return false;

        Set<LogicalVariable> deliveredEarlyVars = new HashSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(targetOp, deliveredEarlyVars);

        deliveredEarlyVars.removeAll(requiredVariables);
        if (deliveredEarlyVars.size() > 0) {
            ArrayList<LogicalVariable> requiredVars = new ArrayList<LogicalVariable>();
            requiredVars.addAll(requiredVariables);
            ILogicalOperator earlyProjectOp = new ProjectOperator(requiredVars);
            Mutable<ILogicalOperator> earlyProjectOpRef = new MutableObject<ILogicalOperator>(earlyProjectOp);
            Mutable<ILogicalOperator> targetRef = middleOp.getInputs().get(0);
            middleOp.getInputs().set(0, earlyProjectOpRef);
            earlyProjectOp.getInputs().add(targetRef);
            context.computeAndSetTypeEnvironmentForOperator(earlyProjectOp);
            return true;
        }
        return false;
    }
}
