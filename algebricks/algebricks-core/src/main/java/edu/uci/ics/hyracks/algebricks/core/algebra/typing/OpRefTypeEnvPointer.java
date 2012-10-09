package edu.uci.ics.hyracks.algebricks.core.algebra.typing;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;

public class OpRefTypeEnvPointer implements ITypeEnvPointer {

    private final Mutable<ILogicalOperator> op;
    private final ITypingContext ctx;

    public OpRefTypeEnvPointer(Mutable<ILogicalOperator> op, ITypingContext ctx) {
        this.op = op;
        this.ctx = ctx;
    }

    @Override
    public IVariableTypeEnvironment getTypeEnv() {
        return ctx.getOutputTypeEnvironment(op.getValue());
    }

    @Override
    public String toString() {
        return this.getClass().getName() + ":" + op;
    }

}
