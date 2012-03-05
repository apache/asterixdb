package edu.uci.ics.asterix.aql.expression;

public class FunIdentifier extends Identifier {
    private int arity;

    public FunIdentifier() {
    }

    public FunIdentifier(String name, int arity) {
        this.value = name;
        this.arity = arity;
    }

    public int getArity() {
        return arity;
    }

    public void setArity(int arity) {
        this.arity = arity;
    }

    public boolean equals(Object o) {
        if (FunIdentifier.class.isInstance(o)) {
            FunIdentifier obj = (FunIdentifier) o;
            if (obj.getArity() == arity && obj.getValue().equals(value)) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return value + "@" + arity;
    }

}
