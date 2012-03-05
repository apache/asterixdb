package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Expression;

public class QuantifiedPair {
    private VariableExpr varExpr;
    private Expression expr;

    public QuantifiedPair() {
    }

    public QuantifiedPair(VariableExpr varExpr, Expression expr) {
        this.varExpr = varExpr;
        this.expr = expr;
    }

    public VariableExpr getVarExpr() {
        return varExpr;
    }

    public void setVarExpr(VariableExpr varExpr) {
        this.varExpr = varExpr;
    }

    public Expression getExpr() {
        return expr;
    }

    public void setExpr(Expression expr) {
        this.expr = expr;
    }

}
