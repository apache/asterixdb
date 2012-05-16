package edu.uci.ics.hyracks.algebricks.core.algebra.expressions;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;

public interface IMergeAggregationExpressionFactory {
    ILogicalExpression createMergeAggregation(ILogicalExpression expr, IOptimizationContext env)
            throws AlgebricksException;
}
