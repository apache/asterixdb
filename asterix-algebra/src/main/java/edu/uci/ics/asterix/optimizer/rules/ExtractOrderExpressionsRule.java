package edu.uci.ics.asterix.optimizer.rules;



import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.rewriter.rules.*;
import edu.uci.ics.asterix.optimizer.base.AnalysisUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;

public class ExtractOrderExpressionsRule extends  AbstractExtractExprRule  {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {

        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.ORDER) {
            return false;
        }

        if (context.checkIfInDontApplySet(this, op1)) {
            return false;
        }
        context.addToDontApplySet(this, op1);
        OrderOperator oo = (OrderOperator) op1;

        if (!orderHasComplexExpr(oo)) {
            return false;
        }
        Mutable<ILogicalOperator> opRef2 = oo.getInputs().get(0);
        for (Pair<IOrder, Mutable<ILogicalExpression>> orderPair : oo.getOrderExpressions()) {
            ILogicalExpression expr = orderPair.second.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE && !AnalysisUtil.isAccessToFieldRecord(expr)) {
                LogicalVariable v = extractExprIntoAssignOpRef(expr, opRef2, context);
                orderPair.second.setValue(new VariableReferenceExpression(v));
            }
        }
        context.computeAndSetTypeEnvironmentForOperator(oo);
        return true;
    }

    private boolean orderHasComplexExpr(OrderOperator oo) {
        for (Pair<IOrder, Mutable<ILogicalExpression>> orderPair : oo.getOrderExpressions()) {
            if (orderPair.second.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return true;
            }
        }
        return false;
    }

}
