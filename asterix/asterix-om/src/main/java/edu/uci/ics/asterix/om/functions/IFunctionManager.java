package edu.uci.ics.asterix.om.functions;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public interface IFunctionManager extends Iterable<IFunctionDescriptorFactory> {

    public void registerFunction(IFunctionDescriptorFactory descriptorFactory) throws AlgebricksException;

    public void unregisterFunction(IFunctionDescriptorFactory descriptorFactory) throws AlgebricksException;

    public IFunctionDescriptor lookupFunction(FunctionIdentifier fid) throws AlgebricksException;
}