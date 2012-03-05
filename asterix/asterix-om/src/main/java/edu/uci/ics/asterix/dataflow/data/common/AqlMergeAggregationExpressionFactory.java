package edu.uci.ics.asterix.dataflow.data.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class AqlMergeAggregationExpressionFactory implements IMergeAggregationExpressionFactory {

    @Override
    public ILogicalExpression createMergeAggregation(ILogicalExpression expr, IOptimizationContext env)
            throws AlgebricksException {
        AggregateFunctionCallExpression agg = (AggregateFunctionCallExpression) expr;
        FunctionIdentifier fid = agg.getFunctionIdentifier();
        int var = env.getVarCounter() + 1;
        env.setVarCounter(var);
        LogicalVariable tempVar = new LogicalVariable(var);
        VariableReferenceExpression tempVarExpr = new VariableReferenceExpression(tempVar);
        List<Mutable<ILogicalExpression>> arguments = new ArrayList<Mutable<ILogicalExpression>>();
        Mutable<ILogicalExpression> mutableExpression = new MutableObject<ILogicalExpression>(tempVarExpr);
        arguments.add(mutableExpression);
        ILogicalExpression aggExpr = AsterixBuiltinFunctions.makeAggregateFunctionExpression(fid, arguments);
        return aggExpr;
    }
}
