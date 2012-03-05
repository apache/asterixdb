package edu.uci.ics.asterix.aql.expression;

import java.util.List;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class UpdateStatement implements Statement {

    private VariableExpr vars;
    private Expression target;
    private Expression condition;
    private List<UpdateClause> ucs;

    public UpdateStatement(VariableExpr vars, Expression target, Expression condition, List<UpdateClause> ucs) {
        this.vars = vars;
        this.target = target;
        this.condition = condition;
        this.ucs = ucs;
    }

    @Override
    public Kind getKind() {
        return Kind.UPDATE;
    }

    public VariableExpr getVariableExpr() {
        return vars;
    }

    public Expression getTarget() {
        return target;
    }

    public Expression getCondition() {
        return condition;
    }

    public List<UpdateClause> getUpdateClauses() {
        return ucs;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitUpdateStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
