package edu.uci.ics.asterix.metadata.feeds;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.api.IFeedOperatorOutputSideHandler;
import edu.uci.ics.asterix.common.feeds.api.ISubscribableRuntime;
import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class CollectTransformFeedFrameWriter implements IFeedOperatorOutputSideHandler {

    private final FeedConnectionId connectionId;
    private IFrameWriter downstreamWriter;
    private final FrameTupleAccessor inputFrameTupleAccessor;
    private final FrameTupleAppender tupleAppender;
    private final IFrame frame;

    private ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);

    public CollectTransformFeedFrameWriter(IHyracksTaskContext ctx, IFrameWriter downstreamWriter,
            ISubscribableRuntime sourceRuntime, RecordDescriptor outputRecordDescriptor, FeedConnectionId connectionId)
            throws HyracksDataException {
        this.downstreamWriter = downstreamWriter;
        RecordDescriptor inputRecordDescriptor = sourceRuntime.getRecordDescriptor();
        inputFrameTupleAccessor = new FrameTupleAccessor(inputRecordDescriptor);
        tupleAppender = new FrameTupleAppender();
        frame = new VSizeFrame(ctx);
        tupleAppender.reset(frame, true);
        this.connectionId = connectionId;
    }

    @Override
    public void open() throws HyracksDataException {
        downstreamWriter.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inputFrameTupleAccessor.reset(buffer);
        int nTuple = inputFrameTupleAccessor.getTupleCount();
        for (int t = 0; t < nTuple; t++) {
            tupleBuilder.addField(inputFrameTupleAccessor, t, 0);
            appendTupleToFrame();
            tupleBuilder.reset();
        }
    }

    private void appendTupleToFrame() throws HyracksDataException {
        if (!tupleAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                tupleBuilder.getSize())) {
            FrameUtils.flushFrame(frame.getBuffer(), downstreamWriter);
            tupleAppender.reset(frame, true);
            if (!tupleAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new IllegalStateException();
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        downstreamWriter.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        downstreamWriter.close();
    }

    @Override
    public FeedId getFeedId() {
        return connectionId.getFeedId();
    }

    @Override
    public Type getType() {
        return Type.COLLECT_TRANSFORM_FEED_OUTPUT_HANDLER;
    }

    public IFrameWriter getDownstreamWriter() {
        return downstreamWriter;
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public void reset(IFrameWriter writer) {
        this.downstreamWriter = writer;
    }

}