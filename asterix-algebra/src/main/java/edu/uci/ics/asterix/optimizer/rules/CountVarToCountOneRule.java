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
package edu.uci.ics.asterix.optimizer.rules;


import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class CountVarToCountOneRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    // It is only for a group-by having just one aggregate which is a count.
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        GroupByOperator g = (GroupByOperator) op1;
        if (g.getNestedPlans().size() != 1) {
            return false;
        }
        ILogicalPlan p = g.getNestedPlans().get(0);
        if (p.getRoots().size() != 1) {
            return false;
        }
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) p.getRoots().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator agg = (AggregateOperator) op2;
        if (agg.getExpressions().size() != 1) {
            return false;
        }
        ILogicalExpression exp2 = agg.getExpressions().get(0).getValue();
        if (exp2.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression fun = (AbstractFunctionCallExpression) exp2;
        if (fun.getFunctionIdentifier() != AsterixBuiltinFunctions.COUNT) {
            return false;
        }
        ILogicalExpression exp3 = fun.getArguments().get(0).getValue();
        if (exp3.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        if (((AbstractLogicalOperator) agg.getInputs().get(0).getValue()).getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return false;
        }
        fun.getArguments().get(0).setValue(new ConstantExpression(new AsterixConstantValue(new AInt64(1L))));
        return true;
    }

}
