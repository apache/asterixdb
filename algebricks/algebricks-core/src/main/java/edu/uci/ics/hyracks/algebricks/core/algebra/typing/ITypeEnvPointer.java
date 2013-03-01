package edu.uci.ics.hyracks.algebricks.core.algebra.typing;

import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;

public interface ITypeEnvPointer {
    public IVariableTypeEnvironment getTypeEnv();
}
