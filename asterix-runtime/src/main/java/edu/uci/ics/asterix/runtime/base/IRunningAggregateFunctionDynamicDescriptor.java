package edu.uci.ics.asterix.runtime.base;


import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IRunningAggregateFunctionFactory;

public interface IRunningAggregateFunctionDynamicDescriptor extends IFunctionDescriptor {
    public IRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(final IEvaluatorFactory[] args)
            throws AlgebricksException;
}
