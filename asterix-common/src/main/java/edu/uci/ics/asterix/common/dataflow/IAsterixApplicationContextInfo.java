package edu.uci.ics.asterix.common.dataflow;

import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public interface IAsterixApplicationContextInfo {
    public IIndexRegistryProvider<IIndex> getIndexRegistryProvider();

    public IStorageManagerInterface getStorageManagerInterface();
}
