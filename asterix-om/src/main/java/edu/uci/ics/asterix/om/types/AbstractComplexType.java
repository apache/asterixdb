package edu.uci.ics.asterix.om.types;

public abstract class AbstractComplexType implements IAType {

    private static final long serialVersionUID = 1L;
    protected String typeName;

    public AbstractComplexType(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

}
