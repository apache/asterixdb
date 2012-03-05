package edu.uci.ics.asterix.aql.expression;

public class Identifier {
    protected String value;

    public Identifier() {
    }

    public Identifier(String value) {
        this.value = value;
    }

    public final String getValue() {
        return value;
    }

    public final void setValue(String value) {
        this.value = value;
    }

    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Identifier)) {
            return false;
        } else {
            Identifier i = (Identifier) o;
            return this.value.equals(i.value);
        }
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
