package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class InsertStatement implements Statement {

    private Identifier datasetName;
    private Query query;
    private int varCounter;

    public InsertStatement(Identifier datasetName, Query query, int varCounter) {
        this.datasetName = datasetName;
        this.query = query;
        this.varCounter = varCounter;
    }

    @Override
    public Kind getKind() {
        return Kind.INSERT;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public Query getQuery() {
        return query;
    }

    public int getVarCounter() {
        return varCounter;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitInsertStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
