package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class SearchPredicate implements ISearchPredicate {

    private static final long serialVersionUID = 1L;

    protected ITupleReference searchKey;
    protected MultiComparator interiorCmp;
    protected MultiComparator leafCmp;

    public SearchPredicate(ITupleReference searchKey, MultiComparator interiorCmp, MultiComparator leafCmp) {
        this.searchKey = searchKey;
        this.interiorCmp = interiorCmp;
        this.leafCmp = leafCmp;
    }

    public ITupleReference getSearchKey() {
        return searchKey;
    }

    public void setSearchKey(ITupleReference searchKey) {
        this.searchKey = searchKey;
    }

    public MultiComparator getInteriorCmp() {
        return interiorCmp;
    }

    public MultiComparator getLeafCmp() {
        return leafCmp;
    }
}
