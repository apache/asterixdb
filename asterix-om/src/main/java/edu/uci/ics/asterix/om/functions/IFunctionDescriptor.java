package edu.uci.ics.asterix.om.functions;

import java.io.Serializable;

import edu.uci.ics.asterix.common.functions.FunctionDescriptorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public interface IFunctionDescriptor extends Serializable {
    public FunctionIdentifier getIdentifier();

    public FunctionDescriptorTag getFunctionDescriptorTag();
}