package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class VariableExpr implements Expression {
    private VarIdentifier var;
    private boolean isNewVar;

    public VariableExpr() {
        super();
        isNewVar = true;
    }

    public VariableExpr(VarIdentifier var) {
        super();
        this.var = var;
        isNewVar = true;
    }

    public boolean getIsNewVar() {
        return isNewVar;
    }

    public void setIsNewVar(boolean isNewVar) {
        this.isNewVar = isNewVar;
    }

    public VarIdentifier getVar() {
        return var;
    }

    public void setVar(VarIdentifier var) {
        this.var = var;
    }

    @Override
    public Kind getKind() {
        return Kind.VARIABLE_EXPRESSION;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitVariableExpr(this, arg);
    }
}
