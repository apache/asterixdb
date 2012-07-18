package edu.uci.ics.asterix.aql.literal;

import edu.uci.ics.asterix.aql.base.Literal;

public class StringLiteral extends Literal {

    private static final long serialVersionUID = -6342491706277606168L;
    private String value;

    public StringLiteral(String value) {
        super();
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public Type getLiteralType() {
        return Type.STRING;
    }

    @Override
    public String getStringValue() {
        return value;
    }
}
