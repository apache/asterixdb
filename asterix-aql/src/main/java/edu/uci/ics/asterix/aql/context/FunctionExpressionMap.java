package edu.uci.ics.asterix.aql.context;

import java.util.HashMap;

import edu.uci.ics.asterix.om.functions.AsterixFunction;

public class FunctionExpressionMap extends HashMap<Integer, AsterixFunction> {
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

    public AsterixFunction get(int arity) {
        if (varargs) {
            return super.get(-1);
        } else {
            return super.get(arity);
        }
    }

    public AsterixFunction put(int arity, AsterixFunction fd) {
        if (varargs) {
            return super.put(-1, fd);
        } else {
            return super.put(arity, fd);
        }
    }
}
