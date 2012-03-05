package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class DeleteStatement implements Statement {

    private VariableExpr vars;
    private Identifier datasetName;
    private Expression condition;
    private Clause dieClause;
    private int varCounter;

    public DeleteStatement(VariableExpr vars, Identifier datasetName, Expression condition, Clause dieClause,
            int varCounter) {
        this.vars = vars;
        this.datasetName = datasetName;
        this.condition = condition;
        this.dieClause = dieClause;
        this.varCounter = varCounter;
    }

    @Override
    public Kind getKind() {
        return Kind.DELETE;
    }

    public VariableExpr getVariableExpr() {
        return vars;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public Expression getCondition() {
        return condition;
    }

    public Clause getDieClause() {
        return dieClause;
    }

    public int getVarCounter() {
        return varCounter;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitDeleteStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
