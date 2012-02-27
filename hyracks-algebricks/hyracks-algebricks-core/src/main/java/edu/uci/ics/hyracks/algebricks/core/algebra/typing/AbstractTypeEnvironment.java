package edu.uci.ics.hyracks.algebricks.core.algebra.typing;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public abstract class AbstractTypeEnvironment implements IVariableTypeEnvironment {

    protected final Map<LogicalVariable, Object> varTypeMap = new HashMap<LogicalVariable, Object>();
    protected final IExpressionTypeComputer expressionTypeComputer;
    protected final IMetadataProvider<?, ?> metadataProvider;

    public AbstractTypeEnvironment(IExpressionTypeComputer expressionTypeComputer,
            IMetadataProvider<?, ?> metadataProvider) {
        this.expressionTypeComputer = expressionTypeComputer;
        this.metadataProvider = metadataProvider;
    }

    @Override
    public Object getType(ILogicalExpression expr) throws AlgebricksException {
        return expressionTypeComputer.getType(expr, metadataProvider, this);
    }

    @Override
    public void setVarType(LogicalVariable var, Object type) {
        varTypeMap.put(var, type);
    }

    @Override
    public boolean substituteProducedVariable(LogicalVariable v1, LogicalVariable v2) throws AlgebricksException {
        Object t = varTypeMap.get(v1);
        if (t == null) {
            return false;
        }
        varTypeMap.put(v1, null);
        varTypeMap.put(v2, t);
        return true;
    }
}
