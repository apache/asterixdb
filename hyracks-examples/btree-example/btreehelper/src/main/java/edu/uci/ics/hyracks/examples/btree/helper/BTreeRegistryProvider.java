package edu.uci.ics.hyracks.examples.btree.helper;

import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeRegistry;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBTreeRegistryProvider;

public class BTreeRegistryProvider implements IBTreeRegistryProvider {

    private static final long serialVersionUID = 1L;

    public static final BTreeRegistryProvider INSTANCE = new BTreeRegistryProvider();

    private BTreeRegistryProvider() {
    }

    @Override
    public BTreeRegistry getBTreeRegistry() {
        return RuntimeContext.getInstance().getBTreeRegistry();
    }
}
