package edu.uci.ics.asterix.aql.expression;

import java.util.Map;

public class ExternalDetailsDecl implements IDatasetDetailsDecl {
    private Map<String, String> properties;
    private String adapter;

    public void setAdapter(String adapter) {
        this.adapter = adapter;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getAdapter() {
        return adapter;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
