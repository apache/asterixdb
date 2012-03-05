package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class ForClause implements Clause {
    private VariableExpr varExpr;
    private VariableExpr posExpr;
    private Expression inExpr;

    public ForClause() {
        super();
    }

    public ForClause(VariableExpr varExpr, Expression inExpr) {
        super();
        this.varExpr = varExpr;
        this.inExpr = inExpr;
    }

    public VariableExpr getVarExpr() {
        return varExpr;
    }

    public void setVarExpr(VariableExpr varExpr) {
        this.varExpr = varExpr;
    }

    public Expression getInExpr() {
        return inExpr;
    }

    public void setInExpr(Expression inExpr) {
        this.inExpr = inExpr;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.FOR_CLAUSE;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitForClause(this, arg);
    }

    public void setPosExpr(VariableExpr posExpr) {
        this.posExpr = posExpr;
    }

    public VariableExpr getPosVarExpr() {
        return posExpr;
    }
}
