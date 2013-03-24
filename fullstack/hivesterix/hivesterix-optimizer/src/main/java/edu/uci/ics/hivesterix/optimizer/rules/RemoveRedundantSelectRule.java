package edu.uci.ics.hivesterix.optimizer.rules;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class RemoveRedundantSelectRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        AbstractLogicalOperator inputOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        if (inputOp.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator selectOp = (SelectOperator) op;
        SelectOperator inputSelectOp = (SelectOperator) inputOp;
        ILogicalExpression expr1 = selectOp.getCondition().getValue();
        ILogicalExpression expr2 = inputSelectOp.getCondition().getValue();

        if (expr1.equals(expr2)) {
            selectOp.getInputs().set(0, inputSelectOp.getInputs().get(0));
            return true;
        }
        return false;
    }

}
