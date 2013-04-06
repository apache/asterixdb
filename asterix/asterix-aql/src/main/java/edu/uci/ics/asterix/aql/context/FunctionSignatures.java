package edu.uci.ics.asterix.aql.context;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.functions.FunctionSignature;

public class FunctionSignatures {
    private final Map<FunctionSignature, FunctionExpressionMap> functionMap;

    public FunctionSignatures() {
        functionMap = new HashMap<FunctionSignature, FunctionExpressionMap>();
    }

    public FunctionSignature get(String dataverse, String name, int arity) {
        FunctionSignature fid = new FunctionSignature(dataverse, name, arity);
        FunctionExpressionMap possibleFD = functionMap.get(fid);
        if (possibleFD == null) {
            return null;
        } else {
            return possibleFD.get(arity);
        }
    }

    public void put(FunctionSignature fd, boolean varargs) {
        FunctionExpressionMap func = functionMap.get(fd);
        if (func == null) {
            func = new FunctionExpressionMap(varargs);
            functionMap.put(fd, func);
        }
        func.put(fd.getArity(), fd);
    }
}
