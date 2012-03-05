package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class IndexDropStatement implements Statement {

    private Identifier datasetName;
    private Identifier indexName;
    private boolean ifExists;

    public IndexDropStatement(Identifier dataverseName, Identifier indexName, boolean ifExists) {
        this.datasetName = dataverseName;
        this.indexName = indexName;
        this.ifExists = ifExists;
    }

    @Override
    public Kind getKind() {
        return Kind.INDEX_DROP;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public Identifier getIndexName() {
        return indexName;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitIndexDropStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }
}
