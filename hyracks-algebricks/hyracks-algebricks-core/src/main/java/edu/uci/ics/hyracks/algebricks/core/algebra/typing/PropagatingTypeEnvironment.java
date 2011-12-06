package edu.uci.ics.hyracks.algebricks.core.algebra.typing;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class PropagatingTypeEnvironment extends AbstractTypeEnvironment {

    private final TypePropagationPolicy policy;

    private final INullableTypeComputer nullableTypeComputer;

    private final ITypeEnvPointer[] envPointers;

    private final List<LogicalVariable> nonNullVariables = new ArrayList<LogicalVariable>();

    public PropagatingTypeEnvironment(IExpressionTypeComputer expressionTypeComputer,
            INullableTypeComputer nullableTypeComputer, IMetadataProvider<?, ?> metadataProvider,
            TypePropagationPolicy policy, ITypeEnvPointer[] envPointers) {
        super(expressionTypeComputer, metadataProvider);
        this.nullableTypeComputer = nullableTypeComputer;
        this.policy = policy;
        this.envPointers = envPointers;
    }

    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        return getVarTypeFullList(var, nonNullVariables);
    }

    public List<LogicalVariable> getNonNullVariables() {
        return nonNullVariables;
    }

    @Override
    public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariableList) throws AlgebricksException {
        nonNullVariableList.addAll(nonNullVariables);
        return getVarTypeFullList(var, nonNullVariableList);
    }

    private Object getVarTypeFullList(LogicalVariable var, List<LogicalVariable> nonNullVariableList)
            throws AlgebricksException {
        Object t = varTypeMap.get(var);
        if (t != null) {
            return t;
        }
        return policy.getVarType(var, nullableTypeComputer, nonNullVariableList, envPointers);
    }
}
