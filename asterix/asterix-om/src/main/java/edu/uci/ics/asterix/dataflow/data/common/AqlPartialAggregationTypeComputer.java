package edu.uci.ics.asterix.dataflow.data.common;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IPartialAggregationTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class AqlPartialAggregationTypeComputer implements IPartialAggregationTypeComputer {

    @Override
    public Object getType(ILogicalExpression expr, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AggregateFunctionCallExpression agg = (AggregateFunctionCallExpression) expr;
        FunctionIdentifier partialFid = agg.getFunctionIdentifier();
        if (partialFid.equals(AsterixBuiltinFunctions.SERIAL_GLOBAL_AVG)) {
            partialFid = AsterixBuiltinFunctions.SERIAL_LOCAL_AVG;
        }
        AggregateFunctionCallExpression partialAgg = AsterixBuiltinFunctions.makeAggregateFunctionExpression(
                partialFid, agg.getArguments());
        return getTypeForFunction((AbstractFunctionCallExpression) partialAgg, env, metadataProvider);
    }

    private Object getTypeForFunction(AbstractFunctionCallExpression expr, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        FunctionIdentifier fi = expr.getFunctionIdentifier();
        ComparisonKind ck = AlgebricksBuiltinFunctions.getComparisonType(fi);
        if (ck != null) {
            List<IAType> unionList = new ArrayList<IAType>();
            unionList.add(BuiltinType.ANULL);
            unionList.add(BuiltinType.ABOOLEAN);
            return new AUnionType(unionList, "OptionalBoolean");
        }
        return AsterixBuiltinFunctions.getResultTypeComputer(fi).computeType(expr, env, metadataProvider);
    }
}
