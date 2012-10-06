package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushDieUpRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getValue();
        if (op0.getInputs().size() == 0)
            return false;
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) op0.getInputs().get(0).getValue();

        if (op1.getInputs().size() == 0)
            return false;
        LogicalOperatorTag tag = op1.getOperatorTag();
        if (tag == LogicalOperatorTag.SINK || tag == LogicalOperatorTag.WRITE
                || tag == LogicalOperatorTag.INSERT_DELETE || tag == LogicalOperatorTag.WRITE_RESULT)
            return false;

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() == LogicalOperatorTag.DIE) {
            op0.getInputs().get(0).setValue(op2);
            op1.getInputs().clear();
            for (Mutable<ILogicalOperator> ref : op2.getInputs())
                op1.getInputs().add(ref);
            op2.getInputs().clear();
            op2.getInputs().add(new MutableObject<ILogicalOperator>(op1));

            context.computeAndSetTypeEnvironmentForOperator(op0);
            context.computeAndSetTypeEnvironmentForOperator(op1);
            context.computeAndSetTypeEnvironmentForOperator(op2);
            return true;
        } else {
            return false;
        }
    }
}
