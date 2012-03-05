package edu.uci.ics.asterix.aql.expression;

import java.util.List;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Clause.ClauseType;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class FLWOGRExpression implements Expression {
    private List<Clause> clauseList;
    private Expression returnExpr;

    public FLWOGRExpression() {
        super();
    }

    public FLWOGRExpression(List<Clause> clauseList, Expression returnExpr) {
        super();
        this.clauseList = clauseList;
        this.returnExpr = returnExpr;
    }

    public List<Clause> getClauseList() {
        return clauseList;
    }

    public void setClauseList(List<Clause> clauseList) {
        this.clauseList = clauseList;
    }

    public Expression getReturnExpr() {
        return returnExpr;
    }

    public void setReturnExpr(Expression returnExpr) {
        this.returnExpr = returnExpr;
    }

    @Override
    public Kind getKind() {
        return Kind.FLWOGR_EXPRESSION;
    }

    public boolean noForClause() {
        for (Clause c : clauseList) {
            if (c.getClauseType() == ClauseType.FOR_CLAUSE) {
                return false;
            }
        }
        return true;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);

    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitFlworExpression(this, arg);
    }
}
