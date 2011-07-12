package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class SearchPredicate implements ISearchPredicate {

    private static final long serialVersionUID = 1L;

    protected ITupleReference searchKey;
    protected MultiComparator cmp;

    public SearchPredicate(ITupleReference searchKey, MultiComparator cmp) {
        this.searchKey = searchKey;
        this.cmp = cmp;
    }

    public ITupleReference getSearchKey() {
        return searchKey;
    }

    public void setSearchKey(ITupleReference searchKey) {
        this.searchKey = searchKey;
    }

    public MultiComparator getCmp() {
        return cmp;
    }
}
