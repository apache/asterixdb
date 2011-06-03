package edu.uci.ics.hyracks.storage.am.rtree.frames;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;

public class NSMFrameFactory implements ITreeIndexFrameFactory {

    private static final long serialVersionUID = 1L;
    private ITreeIndexTupleWriterFactory tupleWriterFactory;
    private int keyFieldCount;

    public NSMFrameFactory(ITreeIndexTupleWriterFactory tupleWriterFactory, int keyFieldCount) {
        this.tupleWriterFactory = tupleWriterFactory;
        this.keyFieldCount = keyFieldCount;
    }

    @Override
    public IRTreeFrame createFrame() {
        return new NSMFrame(tupleWriterFactory.createTupleWriter(), keyFieldCount);
    }
}
