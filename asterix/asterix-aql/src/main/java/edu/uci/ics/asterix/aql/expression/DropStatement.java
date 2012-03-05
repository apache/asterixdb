package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class DropStatement implements Statement {

    private Identifier datasetName;
    private boolean ifExists;

    public DropStatement(Identifier datasetName, boolean ifExists) {
        this.datasetName = datasetName;
        this.ifExists = ifExists;
    }

    @Override
    public Kind getKind() {
        return Kind.DATASET_DROP;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitDropStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
