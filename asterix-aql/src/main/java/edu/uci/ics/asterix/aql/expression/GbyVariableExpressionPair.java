package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Expression;

public class GbyVariableExpressionPair {
    private VariableExpr var; // can be null
    private Expression expr;

    public GbyVariableExpressionPair() {
        super();
    }

    public GbyVariableExpressionPair(VariableExpr var, Expression expr) {
        super();
        this.var = var;
        this.expr = expr;
    }

    public VariableExpr getVar() {
        return var;
    }

    public void setVar(VariableExpr var) {
        this.var = var;
    }

    public Expression getExpr() {
        return expr;
    }

    public void setExpr(Expression expr) {
        this.expr = expr;
    }

}
