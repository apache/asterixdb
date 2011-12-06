package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ReinferAllTypesRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getOperator())) {
            return false;
        }
        typeOpRec(opRef, context);
        return true;
    }

    private void typePlan(ILogicalPlan p, IOptimizationContext context) throws AlgebricksException {
        for (LogicalOperatorReference r : p.getRoots()) {
            typeOpRec(r, context);
        }
    }

    private void typeOpRec(LogicalOperatorReference r, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) r.getOperator();
        for (LogicalOperatorReference i : op.getInputs()) {
            typeOpRec(i, context);
        }
        if (op.hasNestedPlans()) {
            for (ILogicalPlan p : ((AbstractOperatorWithNestedPlans) op).getNestedPlans()) {
                typePlan(p, context);
            }
        }
        context.computeAndSetTypeEnvironmentForOperator(op);
        context.addToDontApplySet(this, op);
    }

}
