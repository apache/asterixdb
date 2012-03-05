package edu.uci.ics.asterix.aql.expression;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class UnionExpr implements Expression {

    private List<Expression> exprs;

    public UnionExpr() {
        exprs = new ArrayList<Expression>();
    }

    public UnionExpr(List<Expression> exprs) {
        this.exprs = exprs;
    }

    @Override
    public Kind getKind() {
        return Kind.UNION_EXPRESSION;
    }

    public List<Expression> getExprs() {
        return exprs;
    }

    public void setExprs(List<Expression> exprs) {
        this.exprs = exprs;
    }

    public void addExpr(Expression exp) {
        exprs.add(exp);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitUnionExpr(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
