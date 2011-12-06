/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import java.util.HashSet;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ComplexJoinInferenceRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator op = opRef.getOperator();
        if (!(op instanceof AbstractScanOperator)) {
            return false;
        }

        LogicalOperatorReference opRef2 = op.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getOperator();
        if (op2.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op2;

        LogicalOperatorReference opRef3 = subplan.getInputs().get(0);
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) opRef3.getOperator();

        if (op3.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE
                || op3.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return false;
        }

        if (subplanHasFreeVariables(subplan)) {
            return false;
        }

        HashSet<LogicalVariable> varsUsedInUnnest = new HashSet<LogicalVariable>();
        VariableUtilities.getUsedVariables(op, varsUsedInUnnest);

        HashSet<LogicalVariable> producedInSubplan = new HashSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(subplan, producedInSubplan);

        if (!producedInSubplan.containsAll(varsUsedInUnnest)) {
            return false;
        }

        ntsToEtsInSubplan(subplan, context);
        InnerJoinOperator join = new InnerJoinOperator(new LogicalExpressionReference(ConstantExpression.TRUE));
        join.getInputs().add(opRef3);
        opRef2.setOperator(OperatorManipulationUtil.eliminateSingleSubplanOverEts(subplan));
        join.getInputs().add(new LogicalOperatorReference(op));
        opRef.setOperator(join);
        context.computeAndSetTypeEnvironmentForOperator(join);
        return true;
    }

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) {
        return false;
    }

    private static void ntsToEtsInSubplan(SubplanOperator s, IOptimizationContext context) throws AlgebricksException {
        for (ILogicalPlan p : s.getNestedPlans()) {
            for (LogicalOperatorReference r : p.getRoots()) {
                OperatorManipulationUtil.ntsToEts(r, context);
            }
        }
    }

    private static boolean subplanHasFreeVariables(SubplanOperator s) throws AlgebricksException {
        for (ILogicalPlan p : s.getNestedPlans()) {
            for (LogicalOperatorReference r : p.getRoots()) {
                if (OperatorPropertiesUtil.hasFreeVariablesInSelfOrDesc((AbstractLogicalOperator) r.getOperator())) {
                    return true;
                }
            }
        }
        return false;
    }

}
