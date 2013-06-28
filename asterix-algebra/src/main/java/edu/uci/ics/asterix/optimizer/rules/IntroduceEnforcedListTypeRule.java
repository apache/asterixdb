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

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.om.typecomputer.base.TypeComputerUtilities;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.optimizer.rules.typecast.StaticTypeCastUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractAssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This class is to enforce types for function expressions which contain list constructor function calls.
 * The List constructor is very special because a nested list is of type List<ANY>.
 * However, the bottom-up type inference (InferTypeRule in algebricks) did not infer that so we need this method to enforce the type.
 * We do not want to break the generality of algebricks so this method is called in an ASTERIX rule: @ IntroduceEnforcedListTypeRule} .
 */
public class IntroduceEnforcedListTypeRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue()))
            return false;
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        context.addToDontApplySet(this, opRef.getValue());

        /**
         * rewrite list constructor types for list constructor functions
         */
        List<Mutable<ILogicalExpression>> expressions;
        switch (op.getOperatorTag()) {
            case ASSIGN:
                AbstractAssignOperator assignOp = (AbstractAssignOperator) op;
                expressions = assignOp.getExpressions();
                break;
            case UNNEST:
                AbstractUnnestOperator unnestOp = (AbstractUnnestOperator) op;
                expressions = Collections.singletonList(unnestOp.getExpressionRef());
                break;
            default:
                return false;
        }
        IVariableTypeEnvironment env = op.computeOutputTypeEnvironment(context);
        return rewriteExpressions(expressions, env);
    }

    private boolean rewriteExpressions(List<Mutable<ILogicalExpression>> expressions, IVariableTypeEnvironment env)
            throws AlgebricksException {
        boolean changed = false;
        for (Mutable<ILogicalExpression> exprRef : expressions) {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) expr;
                IAType exprType = (IAType) env.getType(argFuncExpr);
                if (StaticTypeCastUtil.rewriteListExpr(argFuncExpr, exprType, exprType, env)) {
                    TypeComputerUtilities.resetRequiredAndInputTypes(argFuncExpr);
                    changed = true;
                }
            }
        }
        return changed;
    }

}
