package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class FunctionDropStatement implements Statement {

    private Identifier functionName;
    private int arity;
    private boolean ifExists;

    public FunctionDropStatement(Identifier functionName, int arity, boolean ifExists) {
        this.functionName = functionName;
        this.arity = arity;
        this.ifExists = ifExists;
    }

    @Override
    public Kind getKind() {
        return Kind.FUNCTION_DROP;
    }

    public Identifier getFunctionName() {
        return functionName;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    public int getArity() {
        return arity;
    }

    public void setArity(int arity) {
        this.arity = arity;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitFunctionDropStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
