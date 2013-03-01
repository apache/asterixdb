package edu.uci.ics.hyracks.algebricks.core.algebra.typing;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class PropagateOperatorInputsTypeEnvironment extends AbstractTypeEnvironment {

    private final List<LogicalVariable> nonNullVariables = new ArrayList<LogicalVariable>();
    private final ILogicalOperator op;
    private final ITypingContext ctx;

    public PropagateOperatorInputsTypeEnvironment(ILogicalOperator op, ITypingContext ctx,
            IExpressionTypeComputer expressionTypeComputer, IMetadataProvider<?, ?> metadataProvider) {
        super(expressionTypeComputer, metadataProvider);
        this.op = op;
        this.ctx = ctx;
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
        for (Mutable<ILogicalOperator> r : op.getInputs()) {
            ILogicalOperator c = r.getValue();
            IVariableTypeEnvironment env = ctx.getOutputTypeEnvironment(c);
            Object t2 = env.getVarType(var, nonNullVariableList);
            if (t2 != null) {
                return t2;
            }
        }
        return null;
    }

    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        return getVarTypeFullList(var, nonNullVariables);
    }

}
