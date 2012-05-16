package edu.uci.ics.hyracks.algebricks.core.algebra.typing;

import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NonPropagatingTypeEnvironment extends AbstractTypeEnvironment {

    public NonPropagatingTypeEnvironment(IExpressionTypeComputer expressionTypeComputer,
            IMetadataProvider<?, ?> metadataProvider) {
        super(expressionTypeComputer, metadataProvider);
    }

    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        return varTypeMap.get(var);
    }

    @Override
    public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariables) throws AlgebricksException {
        return getVarType(var);
    }

}
