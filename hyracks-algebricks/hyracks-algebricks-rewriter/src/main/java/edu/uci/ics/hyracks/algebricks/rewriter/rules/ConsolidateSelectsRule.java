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

import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ConsolidateSelectsRule implements IAlgebraicRewriteRule {

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

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) select.getInputs().get(0).getOperator();
        if (op2.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }

        AbstractFunctionCallExpression conj = new ScalarFunctionCallExpression(AlgebricksBuiltinFunctions
                .getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND));
        conj.getArguments().add(new LogicalExpressionReference(select.getCondition().getExpression()));
        conj.getArguments().add(((SelectOperator) op2).getCondition());

        LogicalOperatorReference botOpRef = select.getInputs().get(0);
        boolean more = true;
        while (more) {
            botOpRef = botOpRef.getOperator().getInputs().get(0);
            AbstractLogicalOperator botOp = (AbstractLogicalOperator) botOpRef.getOperator();
            if (botOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                conj.getArguments().add(((SelectOperator) botOp).getCondition());
            } else {
                more = false;
            }
        }
        select.getCondition().setExpression(conj);
        List<LogicalOperatorReference> selInptList = select.getInputs();
        selInptList.clear();
        selInptList.add(botOpRef);
        context.computeAndSetTypeEnvironmentForOperator(select);
        return true;
    }

}
