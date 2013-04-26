package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.base.TypeComputerUtilities;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class FlowRecordResultTypeComputer implements IResultTypeComputer {

    public static final FlowRecordResultTypeComputer INSTANCE = new FlowRecordResultTypeComputer();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        ScalarFunctionCallExpression funcExpr = (ScalarFunctionCallExpression) expression;
        IAType type = TypeComputerUtilities.getRequiredType(funcExpr);
        if (type == null) {
            type = (IAType) env.getType(funcExpr.getArguments().get(0).getValue());
        }
        return type;
    }
}
