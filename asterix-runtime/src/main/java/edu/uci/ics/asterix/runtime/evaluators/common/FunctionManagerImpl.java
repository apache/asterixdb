package edu.uci.ics.asterix.runtime.evaluators.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionManager;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FunctionManagerImpl implements IFunctionManager {
    private final Map<FunctionIdentifier, IFunctionDescriptor> functions;

    public FunctionManagerImpl() {
        functions = new HashMap<FunctionIdentifier, IFunctionDescriptor>();
    }

    @Override
    public synchronized IFunctionDescriptor lookupFunction(FunctionIdentifier fid) throws AlgebricksException {
        return functions.get(fid);
    }

    @Override
    public synchronized void registerFunction(IFunctionDescriptor descriptor) throws AlgebricksException {
        functions.put(descriptor.getIdentifier(), descriptor);
    }

    @Override
    public synchronized void unregisterFunction(IFunctionDescriptor descriptor) throws AlgebricksException {
        FunctionIdentifier fid = descriptor.getIdentifier();
        IFunctionDescriptor desc = functions.get(fid);
        if (descriptor == desc) {
            functions.remove(fid);
        }
    }

    @Override
    public synchronized Iterator<IFunctionDescriptor> iterator() {
        return new ArrayList<IFunctionDescriptor>(functions.values()).iterator();
    }
}