package edu.uci.ics.asterix.runtime.evaluators.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.functions.IFunctionManager;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FunctionManagerImpl implements IFunctionManager {
    private final Map<FunctionIdentifier, IFunctionDescriptorFactory> functions;

    public FunctionManagerImpl() {
        functions = new HashMap<FunctionIdentifier, IFunctionDescriptorFactory>();
    }

    @Override
    public synchronized IFunctionDescriptor lookupFunction(FunctionIdentifier fid) throws AlgebricksException {
        return functions.get(fid).createFunctionDescriptor();
    }

    @Override
    public synchronized void registerFunction(IFunctionDescriptorFactory descriptorFactory) throws AlgebricksException {
        FunctionIdentifier fid = descriptorFactory.createFunctionDescriptor().getIdentifier();
        functions.put(fid, descriptorFactory);
    }

    @Override
    public synchronized void unregisterFunction(IFunctionDescriptorFactory descriptorFactory)
            throws AlgebricksException {
        FunctionIdentifier fid = descriptorFactory.createFunctionDescriptor().getIdentifier();
        functions.remove(fid);
    }

    @Override
    public synchronized Iterator<IFunctionDescriptorFactory> iterator() {
        return new ArrayList<IFunctionDescriptorFactory>(functions.values()).iterator();
    }
}