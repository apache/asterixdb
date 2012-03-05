package edu.uci.ics.asterix.aql.expression;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class CreateIndexStatement implements Statement {

    private Identifier indexName;
    private boolean needToCreate = true;
    private Identifier datasetName;
    private List<String> fieldExprs = new ArrayList<String>();
    private IndexType indexType = IndexType.BTREE;
    private boolean ifNotExists;

    public CreateIndexStatement() {
    }

    public void setNeedToCreate(boolean needToCreate) {
        this.needToCreate = needToCreate;
    }

    public boolean getNeedToCreate() {
        return needToCreate;
    }

    public Identifier getIndexName() {
        return indexName;
    }

    public void setIndexName(Identifier indexName) {
        this.indexName = indexName;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(Identifier datasetName) {
        this.datasetName = datasetName;
    }

    public List<String> getFieldExprs() {
        return fieldExprs;
    }

    public void addFieldExpr(String fe) {
        this.fieldExprs.add(fe);
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_INDEX;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitCreateIndexStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
