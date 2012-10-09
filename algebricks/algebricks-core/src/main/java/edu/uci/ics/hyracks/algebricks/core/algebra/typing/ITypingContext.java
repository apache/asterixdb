package edu.uci.ics.hyracks.algebricks.core.algebra.typing;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public interface ITypingContext {
    public abstract IVariableTypeEnvironment getOutputTypeEnvironment(ILogicalOperator op);

    public abstract void setOutputTypeEnvironment(ILogicalOperator op, IVariableTypeEnvironment env);

    public abstract IExpressionTypeComputer getExpressionTypeComputer();

    public abstract INullableTypeComputer getNullableTypeComputer();

    public abstract IMetadataProvider<?, ?> getMetadataProvider();
}
