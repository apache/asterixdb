package edu.uci.ics.hyracks.algebricks.examples.piglet.runtime.functions;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;

public interface IPigletFunctionEvaluatorFactoryBuilder {
    public IEvaluatorFactory buildEvaluatorFactory(FunctionIdentifier fid, IEvaluatorFactory[] arguments);
}