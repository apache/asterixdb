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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ConsolidateSelectsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator select = (SelectOperator) op;

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) select.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }

        IFunctionInfo andFn = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.AND);
        AbstractFunctionCallExpression conj = new ScalarFunctionCallExpression(andFn);
        conj.getArguments().add(new MutableObject<ILogicalExpression>(select.getCondition().getValue()));
        conj.getArguments().add(((SelectOperator) op2).getCondition());

        Mutable<ILogicalOperator> botOpRef = select.getInputs().get(0);
        boolean more = true;
        while (more) {
            botOpRef = botOpRef.getValue().getInputs().get(0);
            AbstractLogicalOperator botOp = (AbstractLogicalOperator) botOpRef.getValue();
            if (botOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                conj.getArguments().add(((SelectOperator) botOp).getCondition());
            } else {
                more = false;
            }
        }
        select.getCondition().setValue(conj);
        List<Mutable<ILogicalOperator>> selInptList = select.getInputs();
        selInptList.clear();
        selInptList.add(botOpRef);
        context.computeAndSetTypeEnvironmentForOperator(select);
        return true;
    }

}
