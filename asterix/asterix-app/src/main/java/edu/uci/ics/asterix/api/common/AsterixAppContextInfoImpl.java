package edu.uci.ics.asterix.api.common;

import edu.uci.ics.asterix.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.context.AsterixTreeRegistryProvider;
import edu.uci.ics.asterix.dataflow.base.IAsterixApplicationContextInfo;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class AsterixAppContextInfoImpl implements IAsterixApplicationContextInfo {

    public static final AsterixAppContextInfoImpl INSTANCE = new AsterixAppContextInfoImpl();

    private AsterixAppContextInfoImpl() {
    }

    @Override
    public IIndexRegistryProvider<IIndex> getTreeRegisterProvider() {
        return AsterixTreeRegistryProvider.INSTANCE;
    }

    @Override
    public IStorageManagerInterface getStorageManagerInterface() {
        return AsterixStorageManagerInterface.INSTANCE;
    }

}
