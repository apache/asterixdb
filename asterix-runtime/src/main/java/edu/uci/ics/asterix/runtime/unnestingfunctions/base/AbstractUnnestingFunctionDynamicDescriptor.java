package edu.uci.ics.asterix.runtime.unnestingfunctions.base;

import edu.uci.ics.asterix.common.functions.FunctionDescriptorTag;
import edu.uci.ics.asterix.runtime.base.IUnnestingFunctionDynamicDescriptor;

public abstract class AbstractUnnestingFunctionDynamicDescriptor implements IUnnestingFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public FunctionDescriptorTag getFunctionDescriptorTag() {
        return FunctionDescriptorTag.UNNEST;
    }
}
