package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class WhereClause implements Clause {
    private Expression whereExpr;

    public Expression getWhereExpr() {
        return whereExpr;
    }

    public void setWhereExpr(Expression whereExpr) {
        this.whereExpr = whereExpr;
    }

    public WhereClause(Expression whereExpr) {
        super();
        this.whereExpr = whereExpr;
    }

    public WhereClause() {
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.WHERE_CLAUSE;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitWhereClause(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
