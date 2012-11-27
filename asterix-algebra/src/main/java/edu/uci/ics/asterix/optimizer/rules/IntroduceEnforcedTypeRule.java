package edu.uci.ics.asterix.optimizer.rules;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.optimizer.rules.typecast.StaticTypeCastUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractAssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroduceEnforcedTypeRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue()))
            return false;
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        context.addToDontApplySet(this, opRef.getValue());
        boolean changed = false;
        if (op1.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AbstractAssignOperator assignOp = (AbstractAssignOperator) op1;
            List<Mutable<ILogicalExpression>> expressions = assignOp.getExpressions();
            IVariableTypeEnvironment env = assignOp.computeOutputTypeEnvironment(context);
            changed = rewriteExpressions(expressions, env);
        }
        if (op1.getOperatorTag() == LogicalOperatorTag.UNNEST) {
            AbstractUnnestOperator unnestOp = (AbstractUnnestOperator) op1;
            List<Mutable<ILogicalExpression>> expressions = Collections.singletonList(unnestOp.getExpressionRef());
            IVariableTypeEnvironment env = unnestOp.computeOutputTypeEnvironment(context);
            changed = rewriteExpressions(expressions, env);
        }
        return changed;
    }

    private boolean rewriteExpressions(List<Mutable<ILogicalExpression>> expressions, IVariableTypeEnvironment env)
            throws AlgebricksException {
        boolean changed = false;
        for (Mutable<ILogicalExpression> exprRef : expressions) {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) expr;
                IAType exprType = (IAType) env.getType(argFuncExpr);
                changed = changed || StaticTypeCastUtil.rewriteListExpr(argFuncExpr, exprType, exprType, env);
            }
        }
        return changed;
    }

}
