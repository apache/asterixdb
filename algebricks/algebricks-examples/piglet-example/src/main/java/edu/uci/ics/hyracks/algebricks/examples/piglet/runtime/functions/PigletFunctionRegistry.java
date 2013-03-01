package edu.uci.ics.hyracks.algebricks.examples.piglet.runtime.functions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.examples.piglet.exceptions.PigletException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;

public class PigletFunctionRegistry {
    private static final Map<FunctionIdentifier, IPigletFunctionEvaluatorFactoryBuilder> builderMap;

    static {
        Map<FunctionIdentifier, IPigletFunctionEvaluatorFactoryBuilder> temp = new HashMap<FunctionIdentifier, IPigletFunctionEvaluatorFactoryBuilder>();

        temp.put(AlgebricksBuiltinFunctions.EQ, new IPigletFunctionEvaluatorFactoryBuilder() {
            @Override
            public ICopyEvaluatorFactory buildEvaluatorFactory(FunctionIdentifier fid, ICopyEvaluatorFactory[] arguments) {
                return new IntegerEqFunctionEvaluatorFactory(arguments[0], arguments[1]);
            }
        });

        builderMap = Collections.unmodifiableMap(temp);
    }

    public static ICopyEvaluatorFactory createFunctionEvaluatorFactory(FunctionIdentifier fid, ICopyEvaluatorFactory[] args)
            throws PigletException {
        IPigletFunctionEvaluatorFactoryBuilder builder = builderMap.get(fid);
        if (builder == null) {
            throw new PigletException("Unknown function: " + fid);
        }
        return builder.buildEvaluatorFactory(fid, args);
    }
}