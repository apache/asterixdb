package edu.uci.ics.hyracks.storage.am.rtree.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public interface IRTreeLeafFrame extends IRTreeFrame {

    public int findTupleIndex(ITupleReference tuple, MultiComparator cmp);

    public boolean intersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp);
}
