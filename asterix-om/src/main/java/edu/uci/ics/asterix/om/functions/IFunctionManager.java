package edu.uci.ics.asterix.om.functions;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public interface IFunctionManager extends Iterable<IFunctionDescriptor> {
    public void registerFunction(IFunctionDescriptor descriptor) throws AlgebricksException;

    public void unregisterFunction(IFunctionDescriptor descriptor) throws AlgebricksException;

    public IFunctionDescriptor lookupFunction(FunctionIdentifier fid) throws AlgebricksException;
}