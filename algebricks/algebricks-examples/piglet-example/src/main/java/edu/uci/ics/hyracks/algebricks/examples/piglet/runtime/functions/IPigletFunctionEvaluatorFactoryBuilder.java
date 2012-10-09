package edu.uci.ics.hyracks.algebricks.examples.piglet.runtime.functions;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;

public interface IPigletFunctionEvaluatorFactoryBuilder {
    public ICopyEvaluatorFactory buildEvaluatorFactory(FunctionIdentifier fid, ICopyEvaluatorFactory[] arguments);
}