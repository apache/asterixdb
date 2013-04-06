package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.ITupleAcceptor;

public enum UnconditionalTupleAcceptor implements ITupleAcceptor {
    INSTANCE;

    @Override
    public boolean accept(ITupleReference tuple) {
        return true;
    }

}
