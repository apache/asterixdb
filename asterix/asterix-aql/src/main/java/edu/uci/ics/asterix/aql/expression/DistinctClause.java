package edu.uci.ics.asterix.aql.expression;

import java.util.List;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class DistinctClause implements Clause {

    private List<Expression> distinctByExprs;

    public DistinctClause(List<Expression> distinctByExpr) {
        this.distinctByExprs = distinctByExpr;
    }

    public List<Expression> getDistinctByExpr() {
        return distinctByExprs;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.DISTINCT_BY_CLAUSE;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitDistinctClause(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
