package edu.uci.ics.hyracks.storage.am.rtree.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.impls.PathList;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public interface IRTreeCursor {
    public void reset();

    public boolean hasNext() throws Exception;

    public void next() throws Exception;

    public void open(PathList pathList, ITupleReference searchKey, int rootPage, MultiComparator interiorCmp,
            MultiComparator leafCmp) throws Exception;

    public ICachedPage getPage();

    public void close() throws Exception;

    public void setBufferCache(IBufferCache bufferCache);

    public void setFileId(int fileId);

    public ITupleReference getTuple();
}
