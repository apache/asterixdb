package edu.uci.ics.asterix.dataflow.base;

import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public interface IAsterixApplicationContextInfo {
    public IIndexRegistryProvider<IIndex> getTreeRegisterProvider();

    public IStorageManagerInterface getStorageManagerInterface();
}
