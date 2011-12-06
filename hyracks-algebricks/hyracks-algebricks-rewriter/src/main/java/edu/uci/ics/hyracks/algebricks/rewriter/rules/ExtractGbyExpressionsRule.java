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

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;

/**
 * Needed only bc. current Hyrax operators require keys to be fields.
 * 
 */
public class ExtractGbyExpressionsRule extends AbstractExtractExprRule {

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getOperator();
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
        LogicalOperatorReference opRef2 = g.getInputs().get(0);
        for (Pair<LogicalVariable, LogicalExpressionReference> gbyPair : g.getGroupByList()) {
            ILogicalExpression expr = gbyPair.second.getExpression();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                LogicalVariable v = extractExprIntoAssignOpRef(expr, opRef2, context);
                gbyPair.second.setExpression(new VariableReferenceExpression(v));
            }
        }
        return true;
    }

    private boolean decorExprWasRewritten(GroupByOperator g, IOptimizationContext context) throws AlgebricksException {
        if (!decorHasComplexExpr(g)) {
            return false;
        }
        LogicalOperatorReference opRef2 = g.getInputs().get(0);
        for (Pair<LogicalVariable, LogicalExpressionReference> decorPair : g.getDecorList()) {
            ILogicalExpression expr = decorPair.second.getExpression();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                LogicalVariable v = extractExprIntoAssignOpRef(expr, opRef2, context);
                decorPair.second.setExpression(new VariableReferenceExpression(v));
            }
        }
        return true;
    }

    private boolean gbyHasComplexExpr(GroupByOperator g) {
        for (Pair<LogicalVariable, LogicalExpressionReference> gbyPair : g.getGroupByList()) {
            if (gbyPair.second.getExpression().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return true;
            }
        }
        return false;
    }

    private boolean decorHasComplexExpr(GroupByOperator g) {
        for (Pair<LogicalVariable, LogicalExpressionReference> gbyPair : g.getDecorList()) {
            if (gbyPair.second.getExpression().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return true;
            }
        }
        return false;
    }

}
