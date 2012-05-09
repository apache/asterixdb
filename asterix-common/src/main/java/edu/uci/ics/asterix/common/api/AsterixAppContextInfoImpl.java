package edu.uci.ics.asterix.common.api;

import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.common.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.common.context.AsterixIndexRegistryProvider;
import edu.uci.ics.asterix.common.dataflow.IAsterixApplicationContextInfo;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class AsterixAppContextInfoImpl implements IAsterixApplicationContextInfo {

    public static final AsterixAppContextInfoImpl INSTANCE = new AsterixAppContextInfoImpl();

    private static Map<String, Set<String>> nodeControllerMap;

    private AsterixAppContextInfoImpl() {
    }

    @Override
    public IIndexRegistryProvider<IIndex> getIndexRegistryProvider() {
        return AsterixIndexRegistryProvider.INSTANCE;
    }

    @Override
    public IStorageManagerInterface getStorageManagerInterface() {
        return AsterixStorageManagerInterface.INSTANCE;
    }

    public static void setNodeControllerInfo(Map<String, Set<String>> nodeControllerInfo) {
        nodeControllerMap = nodeControllerInfo;
    }

    public static Map<String, Set<String>> getNodeControllerMap() {
        return nodeControllerMap;
    }

}
