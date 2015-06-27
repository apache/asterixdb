package edu.uci.ics.asterix.common.feeds;

import edu.uci.ics.asterix.common.feeds.api.IExceptionHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class IntakeSideMonitoredBuffer extends MonitoredBuffer {

    public IntakeSideMonitoredBuffer(IHyracksTaskContext ctx, FeedRuntimeInputHandler inputHandler, IFrameWriter frameWriter,
            FrameTupleAccessor fta, RecordDescriptor recordDesc, IFeedMetricCollector metricCollector,
            FeedConnectionId connectionId, FeedRuntimeId runtimeId, IExceptionHandler exceptionHandler,
            IFrameEventCallback callback, int nPartitions, FeedPolicyAccessor policyAccessor) {
        super(ctx, inputHandler, frameWriter, fta,  recordDesc, metricCollector, connectionId, runtimeId,
                exceptionHandler, callback, nPartitions, policyAccessor);
    }

    @Override
    protected boolean monitorProcessingRate() {
        return false;
    }

    @Override
    protected boolean logInflowOutflowRate() {
        return false;
    }

    @Override
    protected IFramePreprocessor getFramePreProcessor() {
        return null;
    }

    @Override
    protected IFramePostProcessor getFramePostProcessor() {
        return null;
    }

    @Override
    protected boolean monitorInputQueueLength() {
        return false;
    }

    @Override
    protected boolean reportOutflowRate() {
        return false;
    }

    @Override
    protected boolean reportInflowRate() {
        return true;
    }

}
