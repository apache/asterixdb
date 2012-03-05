package edu.uci.ics.asterix.runtime.aggregates.base;

import edu.uci.ics.asterix.common.functions.FunctionDescriptorTag;
import edu.uci.ics.asterix.runtime.base.IAggregateFunctionDynamicDescriptor;

public abstract class AbstractAggregateFunctionDynamicDescriptor implements IAggregateFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public FunctionDescriptorTag getFunctionDescriptorTag() {
        return FunctionDescriptorTag.AGGREGATE;
    }
}
