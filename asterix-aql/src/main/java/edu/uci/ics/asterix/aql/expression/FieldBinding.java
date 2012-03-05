package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Expression;

public class FieldBinding {
    private Expression leftExpr;
    private Expression rightExpr;

    public FieldBinding() {
    }

    public FieldBinding(Expression leftExpr, Expression rightExpr) {
        super();
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
    }

    public Expression getLeftExpr() {
        return leftExpr;
    }

    public void setLeftExpr(Expression leftExpr) {
        this.leftExpr = leftExpr;
    }

    public Expression getRightExpr() {
        return rightExpr;
    }

    public void setRightExpr(Expression rightExpr) {
        this.rightExpr = rightExpr;
    }

}
