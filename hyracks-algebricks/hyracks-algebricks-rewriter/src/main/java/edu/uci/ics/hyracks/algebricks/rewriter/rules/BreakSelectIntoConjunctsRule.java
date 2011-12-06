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

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class BreakSelectIntoConjunctsRule implements IAlgebraicRewriteRule {

    private List<LogicalExpressionReference> conjs = new ArrayList<LogicalExpressionReference>();

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator select = (SelectOperator) op;

        ILogicalExpression cond = select.getCondition().getExpression();

        conjs.clear();
        if (!cond.splitIntoConjuncts(conjs)) {
            return false;
        }

        LogicalOperatorReference childOfSelect = select.getInputs().get(0);
        boolean fst = true;
        ILogicalOperator botOp = select;
        ILogicalExpression firstExpr = null;
        for (LogicalExpressionReference eRef : conjs) {
            ILogicalExpression e = eRef.getExpression();
            if (fst) {
                fst = false;
                firstExpr = e;
            } else {
                SelectOperator newSelect = new SelectOperator(new LogicalExpressionReference(e));
                List<LogicalOperatorReference> botInpList = botOp.getInputs();
                botInpList.clear();
                botInpList.add(new LogicalOperatorReference(newSelect));
                context.computeAndSetTypeEnvironmentForOperator(botOp);
                botOp = newSelect;
            }
        }
        botOp.getInputs().add(childOfSelect);
        select.getCondition().setExpression(firstExpr);
        context.computeAndSetTypeEnvironmentForOperator(botOp);
        context.computeAndSetTypeEnvironmentForOperator(select);

        return true;
    }
}
