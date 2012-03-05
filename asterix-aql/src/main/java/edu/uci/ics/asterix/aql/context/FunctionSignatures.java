package edu.uci.ics.asterix.aql.context;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.aql.expression.FunIdentifier;

public class FunctionSignatures {
    private final Map<String, FunctionExpressionMap> functionMap;

    public FunctionSignatures() {
        functionMap = new HashMap<String, FunctionExpressionMap>();
    }

    public FunIdentifier get(String name, int arity) {
        FunctionExpressionMap possibleFD = functionMap.get(name);
        if (possibleFD == null) {
            return null;
        } else {
            return possibleFD.get(arity);
        }
    }

    public void put(FunIdentifier fd, boolean varargs) {
        String name = fd.getValue();
        FunctionExpressionMap func = functionMap.get(name);
        if (func == null) {
            func = new FunctionExpressionMap(varargs);
            functionMap.put(name, func);
        }
        func.put(fd.getArity(), fd);
    }
}
