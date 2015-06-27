package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.feeds.FeedConstants.StatisticsConstants;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class FeedFrameTupleAccessor implements IFrameTupleAccessor {

    private final FrameTupleAccessor frameAccessor;
    private final int numOpenFields;

    public FeedFrameTupleAccessor(FrameTupleAccessor frameAccessor) {
        this.frameAccessor = frameAccessor;
        int firstRecordStart = frameAccessor.getTupleStartOffset(0) + frameAccessor.getFieldSlotsLength();
        int openPartOffsetOrig = frameAccessor.getBuffer().getInt(firstRecordStart + 6);
        numOpenFields = frameAccessor.getBuffer().getInt(firstRecordStart + openPartOffsetOrig);
    }

    public int getFeedIntakePartition(int tupleIndex) {
        ByteBuffer buffer = frameAccessor.getBuffer();
        int recordStart = frameAccessor.getTupleStartOffset(tupleIndex) + frameAccessor.getFieldSlotsLength();
        int openPartOffsetOrig = buffer.getInt(recordStart + 6);
        int partitionOffset = openPartOffsetOrig + 4 + 8 * numOpenFields
                + StatisticsConstants.INTAKE_PARTITION.length() + 2 + 1;
        return buffer.getInt(recordStart + partitionOffset);
    }
    
    

    @Override
    public int getFieldCount() {
        return frameAccessor.getFieldCount();
    }

    @Override
    public int getFieldSlotsLength() {
        return frameAccessor.getFieldSlotsLength();
    }

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return frameAccessor.getFieldEndOffset(tupleIndex, fIdx);
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return frameAccessor.getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return frameAccessor.getFieldLength(tupleIndex, fIdx);
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return frameAccessor.getTupleEndOffset(tupleIndex);
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        return frameAccessor.getTupleStartOffset(tupleIndex);
    }

    @Override
    public int getTupleCount() {
        return frameAccessor.getTupleCount();
    }

    @Override
    public ByteBuffer getBuffer() {
        return frameAccessor.getBuffer();
    }

    @Override
    public void reset(ByteBuffer buffer) {
        frameAccessor.reset(buffer);
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return frameAccessor.getAbsoluteFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        return frameAccessor.getTupleLength(tupleIndex);
    }

}
