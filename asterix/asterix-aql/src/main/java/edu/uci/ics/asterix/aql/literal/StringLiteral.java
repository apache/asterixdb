package edu.uci.ics.asterix.aql.literal;

import edu.uci.ics.asterix.aql.base.ILiteral;

public class StringLiteral implements ILiteral {

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

    @Override
    public String toString() {
        return getStringValue();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StringLiteral)) {
            return false;
        }
        StringLiteral s = (StringLiteral) obj;
        return value.equals(s.getValue());
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

}
