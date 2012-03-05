package edu.uci.ics.asterix.runtime.base;

import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public interface IAggregateFunctionDynamicDescriptor extends IFunctionDescriptor {
    public IAggregateFunctionFactory createAggregateFunctionFactory(final IEvaluatorFactory[] args)
            throws AlgebricksException;
}
