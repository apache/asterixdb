package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class UpdateClause implements Clause {

    private Expression target;
    private Expression value;
    private InsertStatement is;
    private DeleteStatement ds;
    private UpdateStatement us;
    private Expression condition;
    private UpdateClause ifbranch;
    private UpdateClause elsebranch;

    public UpdateClause(Expression target, Expression value, InsertStatement is, DeleteStatement ds,
            UpdateStatement us, Expression condition, UpdateClause ifbranch, UpdateClause elsebranch) {
        this.target = target;
        this.value = value;
        this.is = is;
        this.ds = ds;
        this.us = us;
        this.condition = condition;
        this.ifbranch = ifbranch;
        this.elsebranch = elsebranch;
    }

    public Expression getTarget() {
        return target;
    }

    public Expression getValue() {
        return value;
    }

    public InsertStatement getInsertStatement() {
        return is;
    }

    public DeleteStatement getDeleteStatement() {
        return ds;
    }

    public UpdateStatement getUpdateStatement() {
        return us;
    }

    public Expression getCondition() {
        return condition;
    }

    public UpdateClause getIfBranch() {
        return ifbranch;
    }

    public UpdateClause getElseBranch() {
        return elsebranch;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.UPDATE_CLAUSE;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitUpdateClause(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
