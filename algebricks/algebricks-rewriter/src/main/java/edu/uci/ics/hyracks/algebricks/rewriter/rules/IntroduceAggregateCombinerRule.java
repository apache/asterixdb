package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;

public class IntroduceAggregateCombinerRule extends AbstractIntroduceCombinerRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        context.addToDontApplySet(this, op);
        if (op.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggOp = (AggregateOperator) op;
        if (aggOp.getExecutionMode() != ExecutionMode.PARTITIONED || aggOp.getPartitioningVariable() == null) {
            return false;
        }
        Map<AggregateFunctionCallExpression, SimilarAggregatesInfo> toReplaceMap = new HashMap<AggregateFunctionCallExpression, SimilarAggregatesInfo>();
        Pair<Boolean, Mutable<ILogicalOperator>> result = tryToPushAgg(aggOp, null, toReplaceMap, context);
        if (!result.first || result.second == null) {
            return false;
        }
        replaceOriginalAggFuncs(toReplaceMap);
        context.computeAndSetTypeEnvironmentForOperator(aggOp);
        return true;
    }
}
