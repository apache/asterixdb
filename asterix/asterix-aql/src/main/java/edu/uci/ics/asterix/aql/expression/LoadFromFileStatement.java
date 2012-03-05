package edu.uci.ics.asterix.aql.expression;

import java.util.Map;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class LoadFromFileStatement implements Statement {

    private Identifier datasetName;
    private String adapter;
    private Map<String, String> properties;
    private boolean dataIsLocallySorted;

    public LoadFromFileStatement(Identifier datasetName, String adapter, Map<String, String> propertiees,
            boolean dataIsLocallySorted) {
        this.datasetName = datasetName;
        this.adapter = adapter;
        this.properties = propertiees;
        this.dataIsLocallySorted = dataIsLocallySorted;
    }

    public String getAdapter() {
        return adapter;
    }

    public void setAdapter(String adapter) {
        this.adapter = adapter;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Kind getKind() {
        return Kind.LOAD_FROM_FILE;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public boolean dataIsAlreadySorted() {
        return dataIsLocallySorted;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitLoadFromFileStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
