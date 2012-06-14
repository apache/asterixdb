package edu.uci.ics.asterix.runtime.base;


import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunctionFactory;

public interface IRunningAggregateFunctionDynamicDescriptor extends IFunctionDescriptor {
    public ICopyRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException;
}
