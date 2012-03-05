package edu.uci.ics.asterix.runtime.base;

import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.ISerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public interface ISerializableAggregateFunctionDynamicDescriptor extends IFunctionDescriptor {
    public ISerializableAggregateFunctionFactory createAggregateFunctionFactory(final IEvaluatorFactory[] args)
            throws AlgebricksException;
}
