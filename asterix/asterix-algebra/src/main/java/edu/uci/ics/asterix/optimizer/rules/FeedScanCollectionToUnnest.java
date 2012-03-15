package edu.uci.ics.asterix.optimizer.rules;


import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class FeedScanCollectionToUnnest implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
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
        if (needsScanCollection(unnestExpr)) {
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

    private boolean needsScanCollection(ILogicalExpression unnestExpr) {
        switch (unnestExpr.getExpressionTag()) {
            case VARIABLE: {
                return true;
            }
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) unnestExpr;
                return fce.getKind() != FunctionKind.UNNEST;
            }
            default: {
                return false;
            }
        }
    }

}
