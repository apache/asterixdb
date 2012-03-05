package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.rewriter.rules.AbstractExtractExprRule;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

/**
 * Needed only bc. current Hyracks operators require keys to be fields.
 */
public class ExtractDistinctByExpressionsRule extends AbstractExtractExprRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.DISTINCT) {
            return false;
        }

        if (context.checkIfInDontApplySet(this, op1)) {
            return false;
        }
        context.addToDontApplySet(this, op1);
        DistinctOperator d = (DistinctOperator) op1;
        boolean changed = false;
        Mutable<ILogicalOperator> opRef2 = d.getInputs().get(0);
        List<Mutable<ILogicalExpression>> newExprList = new ArrayList<Mutable<ILogicalExpression>>();
        for (Mutable<ILogicalExpression> expr : d.getExpressions()) {
            LogicalExpressionTag tag = expr.getValue().getExpressionTag();
            if (tag == LogicalExpressionTag.VARIABLE || tag == LogicalExpressionTag.CONSTANT) {
                newExprList.add(expr);
                continue;
            }
            LogicalVariable v = extractExprIntoAssignOpRef(expr.getValue(), opRef2, context);
            ILogicalExpression newExpr = new VariableReferenceExpression(v);
            newExprList.add(new MutableObject<ILogicalExpression>(newExpr));
            changed = true;
        }
        if (changed) {
            d.getExpressions().clear();
            d.getExpressions().addAll(newExprList);
            context.computeAndSetTypeEnvironmentForOperator(d);
        }
        return changed;
    }

}
