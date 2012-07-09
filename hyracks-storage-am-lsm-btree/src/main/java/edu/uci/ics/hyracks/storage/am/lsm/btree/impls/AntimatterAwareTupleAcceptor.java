package edu.uci.ics.hyracks.storage.am.lsm.btree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.ITupleAcceptor;
import edu.uci.ics.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;

public class AntimatterAwareTupleAcceptor implements ITupleAcceptor {

    @Override
    public boolean accept(ITupleReference tuple) {
        if (tuple == null) {
            return true;
        }
        return ((LSMBTreeTupleReference) tuple).isAntimatter();
    }

}
