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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractAssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class FeedScanCollectionToUnnest implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            context.addToDontApplySet(this, op);
            return false;
        }
        UnnestOperator unnest = (UnnestOperator) op;
        ILogicalExpression unnestExpr = unnest.getExpressionRef().getValue();
        if (needsScanCollection(unnestExpr, op)) {
            ILogicalExpression newExpr = new UnnestingFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.SCAN_COLLECTION),
                    new MutableObject<ILogicalExpression>(unnestExpr));
            unnest.getExpressionRef().setValue(newExpr);
            context.addToDontApplySet(this, op);
            return true;
        }
        context.addToDontApplySet(this, op);
        return false;
    }

    private ILogicalExpression findVarOriginExpression(LogicalVariable v, ILogicalOperator op)
            throws AlgebricksException {
        boolean searchInputs = false;
        if (!(op instanceof AbstractAssignOperator)) {
            searchInputs = true;
        } else {
            AbstractAssignOperator aao = (AbstractAssignOperator) op;
            List<LogicalVariable> producedVars = new ArrayList<>();
            VariableUtilities.getProducedVariables(op, producedVars);
            int exprIndex = producedVars.indexOf(v);
            if (exprIndex == -1) {
                searchInputs = true;
            } else {
                ILogicalExpression originalCandidate = aao.getExpressions().get(exprIndex).getValue();
                if (originalCandidate.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    searchInputs = true;
                } else {
                    return originalCandidate;
                }
            }
        }

        if (searchInputs) {
            for (Mutable<ILogicalOperator> childOp : op.getInputs()) {
                ILogicalExpression ret = findVarOriginExpression(v, childOp.getValue());
                if (ret != null) {
                    return ret;
                }
            }
        }

        throw new IllegalStateException("Unable to find the original expression that produced variable " + v);
    }

    private boolean needsScanCollection(ILogicalExpression unnestExpr, ILogicalOperator op) throws AlgebricksException {
        switch (unnestExpr.getExpressionTag()) {
            case VARIABLE: {
                LogicalVariable v = ((VariableReferenceExpression) unnestExpr).getVariableReference();
                ILogicalExpression originalExpr = findVarOriginExpression(v, op);
                if (originalExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    return false;
                } else {
                    return !isUnnestingFunction(originalExpr);
                }
            }
            case FUNCTION_CALL: {
                return !isUnnestingFunction(unnestExpr);
            }
            default: {
                return false;
            }
        }
    }

    private boolean isUnnestingFunction(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            return fce.getKind() == FunctionKind.UNNEST;
        }
        return false;
    }
}
