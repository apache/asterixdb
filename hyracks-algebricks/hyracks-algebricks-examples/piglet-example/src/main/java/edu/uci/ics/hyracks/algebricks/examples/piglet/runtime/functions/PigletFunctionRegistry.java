package edu.uci.ics.hyracks.algebricks.examples.piglet.runtime.functions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.examples.piglet.exceptions.PigletException;

public class PigletFunctionRegistry {
    private static final Map<FunctionIdentifier, IPigletFunctionEvaluatorFactoryBuilder> builderMap;

    static {
        Map<FunctionIdentifier, IPigletFunctionEvaluatorFactoryBuilder> temp = new HashMap<FunctionIdentifier, IPigletFunctionEvaluatorFactoryBuilder>();

        temp.put(AlgebricksBuiltinFunctions.EQ, new IPigletFunctionEvaluatorFactoryBuilder() {
            @Override
            public IEvaluatorFactory buildEvaluatorFactory(FunctionIdentifier fid, IEvaluatorFactory[] arguments) {
                return new IntegerEqFunctionEvaluatorFactory(arguments[0], arguments[1]);
            }
        });

        builderMap = Collections.unmodifiableMap(temp);
    }

    public static IEvaluatorFactory createFunctionEvaluatorFactory(FunctionIdentifier fid, IEvaluatorFactory[] args)
            throws PigletException {
        IPigletFunctionEvaluatorFactoryBuilder builder = builderMap.get(fid);
        if (builder == null) {
            throw new PigletException("Unknown function: " + fid);
        }
        return builder.buildEvaluatorFactory(fid, args);
    }
}