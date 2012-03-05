package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class LimitClause implements Clause {
    private Expression limitexpr;
    private Expression offset;

    public LimitClause() {
    }

    public LimitClause(Expression limitexpr, Expression offset) {
        this.limitexpr = limitexpr;
        this.offset = offset;
    }

    public Expression getLimitExpr() {
        return limitexpr;
    }

    public void setLimitExpr(Expression limitexpr) {
        this.limitexpr = limitexpr;
    }

    public Expression getOffset() {
        return offset;
    }

    public void setOffset(Expression offset) {
        this.offset = offset;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.LIMIT_CLAUSE;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitLimitClause(this, arg);
    }
}
