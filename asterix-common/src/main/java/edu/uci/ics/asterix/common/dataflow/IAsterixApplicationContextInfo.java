package edu.uci.ics.asterix.common.dataflow;

import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public interface IAsterixApplicationContextInfo {
    public IIndexLifecycleManagerProvider getIndexLifecycleManagerProvider();

    public IStorageManagerInterface getStorageManagerInterface();
}
