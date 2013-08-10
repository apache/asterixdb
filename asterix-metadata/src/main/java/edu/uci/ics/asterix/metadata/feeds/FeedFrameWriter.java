package edu.uci.ics.asterix.metadata.feeds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.metadata.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.metadata.feeds.SuperFeedManager.FeedReportMessageType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class FeedFrameWriter implements IFrameWriter {

    private static final Logger LOGGER = Logger.getLogger(FeedFrameWriter.class.getName());

    private IFrameWriter writer;

    private IOperatorNodePushable nodePushable;

    private FeedPolicyEnforcer policyEnforcer;

    private FeedConnectionId feedId;

    private LinkedBlockingQueue<Long> statsOutbox;

    private final boolean collectStatistics;

    private List<ByteBuffer> frames = new ArrayList<ByteBuffer>();

    private Mode mode;

    private String nodeId;

    public static final long FLUSH_THRESHOLD_TIME = 5000;

    private FramePushWait framePushWait;

    private FeedRuntimeType feedRuntimeType;

    private int partition;

    private Timer timer;

    private ExecutorService executorService;

    private FrameTupleAccessor fta;

    public enum Mode {
        FORWARD,
        STORE
    }

    public FeedFrameWriter(IFrameWriter writer, IOperatorNodePushable nodePushable, FeedConnectionId feedId,
            FeedPolicyEnforcer policyEnforcer, String nodeId, FeedRuntimeType feedRuntimeType, int partition,
            FrameTupleAccessor fta) {
        this.writer = writer;
        this.mode = Mode.FORWARD;
        this.nodePushable = nodePushable;
        this.feedId = feedId;
        this.policyEnforcer = policyEnforcer;
        this.feedRuntimeType = feedRuntimeType;
        this.partition = partition;
        this.executorService = FeedManager.INSTANCE.getFeedExecutorService(feedId);
        this.collectStatistics = policyEnforcer.getFeedPolicyAccessor().collectStatistics();
        if (collectStatistics) {
            this.statsOutbox = new LinkedBlockingQueue<Long>();
            timer = new Timer();
            framePushWait = new FramePushWait(nodePushable, FLUSH_THRESHOLD_TIME, feedId, nodeId, feedRuntimeType,
                    partition, FLUSH_THRESHOLD_TIME, timer);

            timer.scheduleAtFixedRate(framePushWait, 0, FLUSH_THRESHOLD_TIME);
        }
        this.fta = fta;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode newMode) throws HyracksDataException {
        if (this.mode.equals(newMode)) {
            return;
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Switching to :" + newMode + " from " + this.mode);
        }
        switch (newMode) {
            case FORWARD:
                this.mode = newMode;
                break;
            case STORE:
                this.mode = newMode;
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Beginning to store frames :");
                    LOGGER.info("Frames accumulated till now:" + frames.size());
                }
                break;
        }

    }

    public List<ByteBuffer> getStoredFrames() {
        return frames;
    }

    public void clear() {
        frames.clear();
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    public void reset() {
        framePushWait.reset();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        switch (mode) {
            case FORWARD:
                try {
                    if (collectStatistics) {
                        fta.reset(buffer);
                        framePushWait.notifyStart();
                        writer.nextFrame(buffer);
                        framePushWait.notifyFinish(fta.getTupleCount());
                    } else {
                        writer.nextFrame(buffer);
                    }
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Unable to write frame " + " on behalf of " + nodePushable.getDisplayName());
                    }
                }
                if (frames.size() > 0) {
                    for (ByteBuffer buf : frames) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Flusing OLD frame: " + buf + " on behalf of "
                                    + nodePushable.getDisplayName());
                        }
                        writer.nextFrame(buf);
                    }
                }
                frames.clear();
                break;
            case STORE:
                ByteBuffer storageBuffer = ByteBuffer.allocate(buffer.capacity());
                storageBuffer.put(buffer);
                frames.add(storageBuffer);
                storageBuffer.flip();
                break;
        }
    }

    private static class FramePushWait extends TimerTask {

        private long startTime = -1;
        private IOperatorNodePushable nodePushable;
        private State state;
        private long flushThresholdTime;
        private static final String EOL = "\n";
        private FeedConnectionId feedId;
        private String nodeId;
        private FeedRuntimeType feedRuntimeType;
        private int partition;
        private AtomicLong numTuplesInInterval = new AtomicLong(0);
        private long period;
        private boolean collectThroughput;
        private FeedMessageService mesgService;

        public FramePushWait(IOperatorNodePushable nodePushable, long flushThresholdTime, FeedConnectionId feedId,
                String nodeId, FeedRuntimeType feedRuntimeType, int partition, long period, Timer timer) {
            this.nodePushable = nodePushable;
            this.flushThresholdTime = flushThresholdTime;
            this.state = State.INTIALIZED;
            this.feedId = feedId;
            this.nodeId = nodeId;
            this.feedRuntimeType = feedRuntimeType;
            this.partition = partition;
            this.period = period;
            this.collectThroughput = feedRuntimeType.equals(FeedRuntimeType.INGESTION);
        }

        public void notifyStart() {
            startTime = System.currentTimeMillis();
            state = State.WAITING_FOR_FLUSH_COMPLETION;

        }

        public void reset() {
            mesgService = null;
        }

        public void notifyFinish(int numTuples) {
            state = State.WAITNG_FOR_NEXT_FRAME;
            numTuplesInInterval.set(numTuplesInInterval.get() + numTuples);
        }

        @Override
        public void run() {
            if (state.equals(State.WAITING_FOR_FLUSH_COMPLETION)) {
                long currentTime = System.currentTimeMillis();
                if (currentTime - startTime > flushThresholdTime) {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Congestion reported by " + feedRuntimeType + " [" + partition + "]");
                    }
                    sendReportToSFM(currentTime - startTime, FeedReportMessageType.CONGESTION);
                }
            }
            if (collectThroughput) {
                System.out.println(" NUMBER of TUPLES " + numTuplesInInterval.get() + " in  " + period);
                int instantTput = (int) Math.ceil((((double) numTuplesInInterval.get() * 1000) / period));
                sendReportToSFM(instantTput, FeedReportMessageType.THROUGHPUT);
            }
            numTuplesInInterval.set(0);
        }

        private void sendReportToSFM(long value, SuperFeedManager.FeedReportMessageType mesgType) {
            String feedRep = feedId.getDataverse() + ":" + feedId.getFeedName() + ":" + feedId.getDatasetName();
            String operator = "" + feedRuntimeType;
            String message = mesgType.name().toLowerCase() + "|" + feedRep + "|" + operator + "|" + partition + "|"
                    + value + "|" + nodeId + "|" + EOL;
            if (mesgService == null) {
                while (mesgService == null) {
                    mesgService = FeedManager.INSTANCE.getFeedMessageService(feedId);
                    if (mesgService == null) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            try {
                mesgService.sendMessage(message);
            } catch (IOException ioe) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Unable to send feed report to SFM for feed " + feedId + " " + feedRuntimeType + "["
                            + partition + "]");
                }
            }
        }

        private enum State {
            INTIALIZED,
            WAITING_FOR_FLUSH_COMPLETION,
            WAITNG_FOR_NEXT_FRAME
        }

    }

    private static class FeedOperatorStatisticsCollector implements Runnable {

        private final FeedConnectionId feedId;
        private final LinkedBlockingQueue<Long> inbox;
        private final long[] readings;
        private int readingIndex = 0;
        private int historySize = 10;
        private double runningAvg = -1;
        private double deviationPercentageThreshold = 50;
        private int successiveThresholds = 0;
        private IOperatorNodePushable coreOperatorNodePushable;
        private int count;

        public FeedOperatorStatisticsCollector(FeedConnectionId feedId, LinkedBlockingQueue<Long> inbox,
                IOperatorNodePushable coreOperatorNodePushable) {
            this.feedId = feedId;
            this.inbox = inbox;
            this.readings = new long[historySize];
            this.coreOperatorNodePushable = coreOperatorNodePushable;
        }

        @Override
        public void run() {
            SuperFeedManager sfm = null;
            try {
                while (sfm == null) {
                    sfm = FeedManager.INSTANCE.getSuperFeedManager(feedId);
                    if (sfm == null) {
                        Thread.sleep(2000);
                    }
                }

                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Obtained SFM " + sfm + " " + coreOperatorNodePushable.getDisplayName());
                }
                while (true) {
                    Long reading = inbox.take();
                    if (count != historySize) {
                        count++;
                    }
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Obtained Reading " + reading + " " + coreOperatorNodePushable.getDisplayName());
                    }
                    double newRunningAvg;
                    double deviation = 0;
                    if (runningAvg >= 0) {
                        int prevIndex = readingIndex == 0 ? historySize - 1 : readingIndex - 1;
                        newRunningAvg = (runningAvg * count - readings[prevIndex] + reading) / (count);
                        deviation = reading - runningAvg;
                    } else {
                        newRunningAvg = reading;
                    }

                    double devPercentage = (deviation * 100 / runningAvg);

                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Current reading :" + reading + " Previous avg:" + runningAvg + " New Average: "
                                + newRunningAvg + " deviation % " + devPercentage + " Op "
                                + coreOperatorNodePushable.getDisplayName());
                    }

                    if (devPercentage > deviationPercentageThreshold) {
                        successiveThresholds++;
                        if (successiveThresholds > 1) {
                            if (LOGGER.isLoggable(Level.SEVERE)) {
                                LOGGER.severe("CONGESTION in sending frames by "
                                        + coreOperatorNodePushable.getDisplayName());
                            }
                            successiveThresholds = 0;
                        }
                    } else {
                        runningAvg = newRunningAvg;
                        readings[readingIndex] = reading;
                        readingIndex = (readingIndex + 1) % historySize;
                    }
                }
            } catch (InterruptedException ie) {
                // do nothing
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
        if (framePushWait != null) {
            framePushWait.cancel();
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (framePushWait != null) {
            framePushWait.cancel();
        }
        writer.close();
    }

    public IFrameWriter getWriter() {
        return writer;
    }

    public void setWriter(IFrameWriter writer) {
        this.writer = writer;
    }

    @Override
    public String toString() {
        return "MaterializingFrameWriter using " + writer;
    }

}
