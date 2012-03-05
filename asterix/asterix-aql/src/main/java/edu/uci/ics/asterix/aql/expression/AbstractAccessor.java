package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Expression;

public abstract class AbstractAccessor implements Expression {
    protected Expression expr;

    public AbstractAccessor(Expression expr) {
        super();
        this.expr = expr;
    }

    public Expression getExpr() {
        return expr;
    }

    public void setExpr(Expression expr) {
        this.expr = expr;
    }

}
