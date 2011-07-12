package edu.uci.ics.hyracks.storage.am.rtree.frames;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;

public class NSMInteriorFrameFactory implements ITreeIndexFrameFactory {

    private static final long serialVersionUID = 1L;
    private ITreeIndexTupleWriterFactory tupleWriterFactory;
    private int keyFieldCount;

    public NSMInteriorFrameFactory(ITreeIndexTupleWriterFactory tupleWriterFactory, int keyFieldCount) {
        this.tupleWriterFactory = tupleWriterFactory;
        this.keyFieldCount = keyFieldCount;
    }

    @Override
    public IRTreeInteriorFrame createFrame() {
        return new NSMInteriorFrame(tupleWriterFactory.createTupleWriter(), keyFieldCount);
    }
}
