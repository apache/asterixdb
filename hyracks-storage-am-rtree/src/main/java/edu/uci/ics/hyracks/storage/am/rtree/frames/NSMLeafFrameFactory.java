package edu.uci.ics.hyracks.storage.am.rtree.frames;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;

public class NSMLeafFrameFactory implements ITreeIndexFrameFactory {

    private static final long serialVersionUID = 1L;
    private ITreeIndexTupleWriterFactory tupleWriterFactory;
    private int keyFieldCount;

    public NSMLeafFrameFactory(ITreeIndexTupleWriterFactory tupleWriterFactory, int keyFieldCount) {
        this.tupleWriterFactory = tupleWriterFactory;
        this.keyFieldCount = keyFieldCount;
    }

    @Override
    public IRTreeLeafFrame createFrame() {
        return new NSMLeafFrame(tupleWriterFactory.createTupleWriter(), keyFieldCount);
    }
}
