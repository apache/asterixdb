package edu.uci.ics.asterix.om.functions;

public interface IFunctionDescriptorFactory {

    /**
     * the artifact registered in function manager
     * @return a new IFunctionDescriptor instance
     */
    public IFunctionDescriptor createFunctionDescriptor();
}
