package edu.uci.ics.asterix.runtime.runningaggregates.base;

import edu.uci.ics.asterix.common.functions.FunctionDescriptorTag;
import edu.uci.ics.asterix.runtime.base.IRunningAggregateFunctionDynamicDescriptor;

public abstract class AbstractRunningAggregateFunctionDynamicDescriptor implements
        IRunningAggregateFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public FunctionDescriptorTag getFunctionDescriptorTag() {
        return FunctionDescriptorTag.RUNNINGAGGREGATE;
    }
}
