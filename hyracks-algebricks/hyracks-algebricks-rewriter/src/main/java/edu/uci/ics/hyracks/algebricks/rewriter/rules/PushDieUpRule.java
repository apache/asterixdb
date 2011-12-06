package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushDieUpRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getOperator();
        if (op0.getInputs().size() == 0)
            return false;
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) op0.getInputs().get(0).getOperator();

        if (op1.getInputs().size() == 0)
            return false;
        LogicalOperatorTag tag = op1.getOperatorTag();
        if (tag == LogicalOperatorTag.SINK || tag == LogicalOperatorTag.WRITE
                || tag == LogicalOperatorTag.INSERT_DELETE || tag == LogicalOperatorTag.WRITE_RESULT)
            return false;

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getOperator();
        if (op2.getOperatorTag() == LogicalOperatorTag.DIE) {
            op0.getInputs().get(0).setOperator(op2);
            op1.getInputs().clear();
            for (LogicalOperatorReference ref : op2.getInputs())
                op1.getInputs().add(ref);
            op2.getInputs().clear();
            op2.getInputs().add(new LogicalOperatorReference(op1));
            
            context.computeAndSetTypeEnvironmentForOperator(op0);
            context.computeAndSetTypeEnvironmentForOperator(op1);
            context.computeAndSetTypeEnvironmentForOperator(op2);
            return true;
        } else {
            return false;
        }
    }
}
