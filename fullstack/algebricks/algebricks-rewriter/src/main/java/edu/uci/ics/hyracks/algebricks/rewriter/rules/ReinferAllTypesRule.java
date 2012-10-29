package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ReinferAllTypesRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        typeOpRec(opRef, context);
        return true;
    }

    private void typePlan(ILogicalPlan p, IOptimizationContext context) throws AlgebricksException {
        for (Mutable<ILogicalOperator> r : p.getRoots()) {
            typeOpRec(r, context);
        }
    }

    private void typeOpRec(Mutable<ILogicalOperator> r, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) r.getValue();
        for (Mutable<ILogicalOperator> i : op.getInputs()) {
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
