package edu.uci.ics.hyracks.storage.am.rtree.frames;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrameFactory;

public class NSMRTreeFrameFactory implements IRTreeFrameFactory {

    private static final long serialVersionUID = 1L;
    private ITreeIndexTupleWriterFactory tupleWriterFactory;
    private int keyFieldCount;

    public NSMRTreeFrameFactory(ITreeIndexTupleWriterFactory tupleWriterFactory, int keyFieldCount) {
        this.tupleWriterFactory = tupleWriterFactory;
        this.keyFieldCount = keyFieldCount;
    }

    @Override
    public IRTreeFrame getFrame() {
        return new NSMRTreeFrame(tupleWriterFactory.createTupleWriter(), keyFieldCount);
    }
}
