package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class LetClause implements Clause {
    private VariableExpr varExpr;
    private Expression bindExpr;

    public LetClause() {
        super();
    }

    public LetClause(VariableExpr varExpr, Expression bindExpr) {
        super();
        this.varExpr = varExpr;
        this.bindExpr = bindExpr;
    }

    public VariableExpr getVarExpr() {
        return varExpr;
    }

    public void setVarExpr(VariableExpr varExpr) {
        this.varExpr = varExpr;
    }

    public Expression getBindingExpr() {
        return bindExpr;
    }

    public void setBindingExpr(Expression bindExpr) {
        this.bindExpr = bindExpr;
    }

    public void setBeExpr(Expression beExpr) {
        this.bindExpr = beExpr;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.LET_CLAUSE;
    }

    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitLetClause(this, arg);
    }

}
