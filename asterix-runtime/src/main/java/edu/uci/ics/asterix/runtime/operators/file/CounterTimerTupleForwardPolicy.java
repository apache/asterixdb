package edu.uci.ics.asterix.runtime.operators.file;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.parse.ITupleForwardPolicy;
import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class CounterTimerTupleForwardPolicy implements ITupleForwardPolicy {

    public static final String BATCH_SIZE = "batch-size";
    public static final String BATCH_INTERVAL = "batch-interval";

    private static final Logger LOGGER = Logger.getLogger(CounterTimerTupleForwardPolicy.class.getName());
   
    private FrameTupleAppender appender;
    private IFrame frame;
    private IFrameWriter writer;
    private int batchSize;
    private long batchInterval;
    private int tuplesInFrame = 0;
    private TimeBasedFlushTask flushTask;
    private Timer timer;
    private Object lock = new Object();
    private boolean activeTimer = false;

    public void configure(Map<String, String> configuration) {
        String propValue = (String) configuration.get(BATCH_SIZE);
        if (propValue != null) {
            batchSize = Integer.parseInt(propValue);
        } else {
            batchSize = -1;
        }

        propValue = (String) configuration.get(BATCH_INTERVAL);
        if (propValue != null) {
            batchInterval = Long.parseLong(propValue);
            activeTimer = true;
        }
    }

    public void initialize(IHyracksTaskContext ctx, IFrameWriter writer) throws HyracksDataException {
        this.appender = new FrameTupleAppender();
        this.frame = new VSizeFrame(ctx);
        appender.reset(frame, true);
        this.writer = writer;
        if (activeTimer) {
            this.timer = new Timer();
            this.flushTask = new TimeBasedFlushTask(writer, lock);
            timer.scheduleAtFixedRate(flushTask, 0, batchInterval);
        }
    }

    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException {
        if (activeTimer) {
            synchronized (lock) {
                addTupleToFrame(tb);
            }
        } else {
            addTupleToFrame(tb);
        }
        tuplesInFrame++;
    }

    private void addTupleToFrame(ArrayTupleBuilder tb) throws HyracksDataException {
        if (tuplesInFrame == batchSize || !appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("flushing frame containg (" + tuplesInFrame + ") tuples");
            }
            FrameUtils.flushFrame(frame.getBuffer(), writer);
            tuplesInFrame = 0;
            appender.reset(frame, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new IllegalStateException();
            }
        }
    }

    public void close() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            if (activeTimer) {
                synchronized (lock) {
                    FrameUtils.flushFrame(frame.getBuffer(), writer);
                }
            } else {
                FrameUtils.flushFrame(frame.getBuffer(), writer);
            }
        }

        if (timer != null) {
            timer.cancel();
        }
    }

    private class TimeBasedFlushTask extends TimerTask {

        private IFrameWriter writer;
        private final Object lock;

        public TimeBasedFlushTask(IFrameWriter writer, Object lock) {
            this.writer = writer;
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                if (tuplesInFrame > 0) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("TTL expired flushing frame (" + tuplesInFrame + ")");
                    }
                    synchronized (lock) {
                        FrameUtils.flushFrame(frame.getBuffer(), writer);
                        appender.reset(frame, true);
                        tuplesInFrame = 0;
                    }
                }
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public TupleForwardPolicyType getType() {
        return TupleForwardPolicyType.COUNTER_TIMER_EXPIRED;
    }

}