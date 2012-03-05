package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class DieClause implements Clause {
    private Expression expr;

    public DieClause() {
    }

    public DieClause(Expression dieexpr) {
        this.expr = dieexpr;
    }

    public Expression getDieExpr() {
        return expr;
    }

    public void setDieExpr(Expression dieexpr) {
        this.expr = dieexpr;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.DIE_CLAUSE;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitDieClause(this, arg);
    }
}
