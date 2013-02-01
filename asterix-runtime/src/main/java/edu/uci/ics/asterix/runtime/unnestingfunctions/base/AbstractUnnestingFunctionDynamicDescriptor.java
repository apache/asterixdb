package edu.uci.ics.asterix.runtime.unnestingfunctions.base;

import edu.uci.ics.asterix.common.functions.FunctionDescriptorTag;
import edu.uci.ics.asterix.om.functions.AbstractFunctionDescriptor;

public abstract class AbstractUnnestingFunctionDynamicDescriptor extends AbstractFunctionDescriptor {
    private static final long serialVersionUID = 1L;

    public FunctionDescriptorTag getFunctionDescriptorTag() {
        return FunctionDescriptorTag.UNNEST;
    }
}
