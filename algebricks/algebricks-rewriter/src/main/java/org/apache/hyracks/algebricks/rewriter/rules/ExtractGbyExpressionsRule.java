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
package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;

/**
 * Needed only bc. current Hyrax operators require keys to be fields.
 */
public class ExtractGbyExpressionsRule extends AbstractExtractExprRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }

        if (context.checkIfInDontApplySet(this, op1)) {
            return false;
        }
        context.addToDontApplySet(this, op1);
        GroupByOperator g = (GroupByOperator) op1;
        boolean r1 = gbyExprWasRewritten(g, context);
        boolean r2 = decorExprWasRewritten(g, context);
        boolean fired = r1 || r2;
        if (fired) {
            context.computeAndSetTypeEnvironmentForOperator(g);
        }
        return fired;
    }

    private boolean gbyExprWasRewritten(GroupByOperator g, IOptimizationContext context) throws AlgebricksException {
        if (!gbyHasComplexExpr(g)) {
            return false;
        }
        Mutable<ILogicalOperator> opRef2 = g.getInputs().get(0);
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> gbyPair : g.getGroupByList()) {
            ILogicalExpression expr = gbyPair.second.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                LogicalVariable v = extractExprIntoAssignOpRef(expr, opRef2, context);
                gbyPair.second.setValue(new VariableReferenceExpression(v));
            }
        }
        return true;
    }

    private boolean decorExprWasRewritten(GroupByOperator g, IOptimizationContext context) throws AlgebricksException {
        if (!decorHasComplexExpr(g)) {
            return false;
        }
        Mutable<ILogicalOperator> opRef2 = g.getInputs().get(0);
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> decorPair : g.getDecorList()) {
            ILogicalExpression expr = decorPair.second.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                LogicalVariable v = extractExprIntoAssignOpRef(expr, opRef2, context);
                decorPair.second.setValue(new VariableReferenceExpression(v));
            }
        }
        return true;
    }

    private boolean gbyHasComplexExpr(GroupByOperator g) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> gbyPair : g.getGroupByList()) {
            if (gbyPair.second.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return true;
            }
        }
        return false;
    }

    private boolean decorHasComplexExpr(GroupByOperator g) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> gbyPair : g.getDecorList()) {
            if (gbyPair.second.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return true;
            }
        }
        return false;
    }

}
