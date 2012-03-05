package edu.uci.ics.asterix.runtime.aggregates.base;

import edu.uci.ics.asterix.common.functions.FunctionDescriptorTag;
import edu.uci.ics.asterix.runtime.base.ISerializableAggregateFunctionDynamicDescriptor;

public abstract class AbstractSerializableAggregateFunctionDynamicDescriptor implements
        ISerializableAggregateFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    @Override
    public FunctionDescriptorTag getFunctionDescriptorTag() {
        return FunctionDescriptorTag.SERIALAGGREGATE;
    }

}
