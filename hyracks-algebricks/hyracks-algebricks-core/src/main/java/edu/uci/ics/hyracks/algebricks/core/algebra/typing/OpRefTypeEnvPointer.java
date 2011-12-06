package edu.uci.ics.hyracks.algebricks.core.algebra.typing;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;

public class OpRefTypeEnvPointer implements ITypeEnvPointer {

    private final LogicalOperatorReference op;
    private final ITypingContext ctx;

    public OpRefTypeEnvPointer(LogicalOperatorReference op, ITypingContext ctx) {
        this.op = op;
        this.ctx = ctx;
    }

    @Override
    public IVariableTypeEnvironment getTypeEnv() {
        return ctx.getOutputTypeEnvironment(op.getOperator());
    }

    @Override
    public String toString() {
        return this.getClass().getName() + ":" + op;
    }

}
