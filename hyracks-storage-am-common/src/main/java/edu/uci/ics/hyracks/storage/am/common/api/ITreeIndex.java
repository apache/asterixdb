package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOpContext;

public interface ITreeIndex {
    // init:

    public void create(int indexFileId, ITreeIndexFrame leafFrame, ITreeIndexMetaDataFrame metaFrame) throws Exception;

    public void open(int indexFileId);

    // operations:

    public void insert(ITupleReference tuple, IndexOpContext ictx) throws Exception;

    public void update(ITupleReference tuple, IndexOpContext ictx) throws Exception;

    public void delete(ITupleReference tuple, IndexOpContext ictx) throws Exception;

    public IndexOpContext createOpContext(IndexOp op, ITreeIndexFrame leafFrame, ITreeIndexFrame interiorFrame,
            ITreeIndexMetaDataFrame metaFrame);

    // bulk loading:

    public IIndexBulkLoadContext beginBulkLoad(float fillFactor, ITreeIndexFrame leafFrame,
            ITreeIndexFrame interiorFrame, ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException;

    public void bulkLoadAddTuple(IIndexBulkLoadContext ictx, ITupleReference tuple) throws HyracksDataException;

    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException;

    // search:
    public void diskOrderScan(ITreeIndexCursor icursor, ITreeIndexFrame leafFrame, ITreeIndexMetaDataFrame metaFrame,
            IndexOpContext ictx) throws HyracksDataException;

    // utility:

    public IFreePageManager getFreePageManager();

    public int getRootPageId();

    public ITreeIndexFrameFactory getLeafFrameFactory();

    public ITreeIndexFrameFactory getInteriorFrameFactory();

    public int getFieldCount();

    public IndexType getIndexType();
}
