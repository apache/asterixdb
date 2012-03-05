package edu.uci.ics.asterix.runtime.base;


import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public interface IScalarFunctionDynamicDescriptor extends IFunctionDescriptor {
    public IEvaluatorFactory createEvaluatorFactory(IEvaluatorFactory[] args) throws AlgebricksException;
}
