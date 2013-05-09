package edu.uci.ics.asterix.common.api;

public interface IAsterixRuntimeComponentsProvider {

    public IAsterixRuntimeComponentsProvider getLSMBTreeProvider();

    public IAsterixRuntimeComponentsProvider getLSMRTreeProvider();

    public IAsterixRuntimeComponentsProvider getLSMInvertedIndexProvider();

    public IAsterixRuntimeComponentsProvider getLSMNoIndexProvider();

}
