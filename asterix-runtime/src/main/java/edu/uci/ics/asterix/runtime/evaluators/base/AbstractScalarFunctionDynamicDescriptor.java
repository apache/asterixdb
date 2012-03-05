package edu.uci.ics.asterix.runtime.evaluators.base;

import edu.uci.ics.asterix.common.functions.FunctionDescriptorTag;
import edu.uci.ics.asterix.runtime.base.IScalarFunctionDynamicDescriptor;

public abstract class AbstractScalarFunctionDynamicDescriptor implements IScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    @Override
    public FunctionDescriptorTag getFunctionDescriptorTag() {
        return FunctionDescriptorTag.SCALAR;
    }

}
