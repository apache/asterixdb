package edu.uci.ics.asterix.runtime.aggregates.base;

import edu.uci.ics.asterix.common.functions.FunctionDescriptorTag;
import edu.uci.ics.asterix.om.functions.AbstractFunctionDescriptor;

public abstract class AbstractAggregateFunctionDynamicDescriptor extends AbstractFunctionDescriptor  {
    private static final long serialVersionUID = 1L;

    public FunctionDescriptorTag getFunctionDescriptorTag() {
        return FunctionDescriptorTag.AGGREGATE;
    }

   
}
