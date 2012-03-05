package edu.uci.ics.asterix.aql.context;

import java.util.HashMap;

import edu.uci.ics.asterix.aql.expression.FunIdentifier;

public class FunctionExpressionMap extends HashMap<Integer, FunIdentifier> {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private boolean varargs;

    public boolean isVarargs() {
        return varargs;
    }

    public void setVarargs(boolean varargs) {
        this.varargs = varargs;
    }

    public FunctionExpressionMap(boolean varargs) {
        super();
        this.varargs = varargs;
    }

    public FunIdentifier get(int arity) {
        if (varargs) {
            return super.get(-1);
        } else {
            return super.get(arity);
        }
    }

    public FunIdentifier put(int arity, FunIdentifier fd) {
        if (varargs) {
            return super.put(-1, fd);
        } else {
            return super.put(arity, fd);
        }
    }
}
