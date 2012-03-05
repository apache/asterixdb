package edu.uci.ics.asterix.runtime.base;


import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IRunningAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public interface IRunningAggregateFunctionDynamicDescriptor extends IFunctionDescriptor {
    public IRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(final IEvaluatorFactory[] args)
            throws AlgebricksException;
}
