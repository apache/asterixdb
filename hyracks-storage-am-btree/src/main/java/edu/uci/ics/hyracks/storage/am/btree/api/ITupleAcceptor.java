package edu.uci.ics.hyracks.storage.am.btree.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface ITupleAcceptor {
    public boolean accept(ITupleReference tuple);
}
