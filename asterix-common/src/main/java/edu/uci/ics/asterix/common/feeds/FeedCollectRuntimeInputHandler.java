package edu.uci.ics.asterix.common.feeds;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class FeedCollectRuntimeInputHandler extends FeedRuntimeInputHandler {

    private final FeedFrameCache feedFrameCache;

    public FeedCollectRuntimeInputHandler(IHyracksTaskContext ctx, FeedConnectionId connectionId, FeedRuntimeId runtimeId,
            IFrameWriter coreOperator, FeedPolicyAccessor fpa, boolean bufferingEnabled,
            FrameTupleAccessor fta, RecordDescriptor recordDesc, IFeedManager feedManager, int nPartitions)
            throws IOException {
        super(ctx, connectionId, runtimeId, coreOperator, fpa, bufferingEnabled, fta, recordDesc, feedManager,
                nPartitions);
        this.feedFrameCache = new FeedFrameCache(ctx, fta, coreOperator);
    }

    public void process(ByteBuffer frame) throws HyracksDataException {
        feedFrameCache.sendMessage(frame);
        super.process(frame);
    }

    public void replayFrom(int recordId) throws HyracksDataException {
        feedFrameCache.replayRecords(recordId);
    }

    public void dropTill(int recordId) {
        feedFrameCache.dropTillRecordId(recordId);
    }
    
    public void replayCached() throws HyracksDataException{
        feedFrameCache.replayAll();
    }

}
