package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class MemoryContentionResponsiveHHJ extends OptimizedHybridHashJoin {
    private int processedFrames = 0;
    private int frameInterval = 0;
    private boolean eventBased = false;
    private int timeInterval =0;
    private Timer timer;

    /**
     * Timer Task to update Memory Budget when running on Time Strategy.
     */
    class UpdateMemoryTask extends TimerTask {
        public void run() {
            LOGGER.info("Update Memory Budget Based on TIME");
            updateMemoryBudgetBuildPhase();
        }
    }

    public MemoryContentionResponsiveHHJ(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions, String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd, ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval, IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1,int timeInterval,int frameInterval,boolean eventBased) {
        super(jobletCtx, memSizeInFrames, numOfPartitions, probeRelName, buildRelName, probeRd, buildRd, probeHpc, buildHpc, probePredEval, buildPredEval, isLeftOuter, nullWriterFactories1);
        SetTimeInterval(timeInterval);
        this.frameInterval = frameInterval;
        this.eventBased = eventBased;
    }

    private void SetTimeInterval(int interval){
        timeInterval = interval;
        if(interval > 0) {
            if (timer == null) {
                timer = new Timer("Update Budget");
            } else {
                timer.cancel();
            }
            timer.schedule(new UpdateMemoryTask(), 0, timeInterval);
        }
        else if(timer != null){
            timer.cancel();
        }
    }

    public void build(ByteBuffer buffer)  throws HyracksDataException {
        if(frameInterval > 0 && processedFrames % frameInterval == 0){
            LOGGER.info("Update Memory Budget Based on FRAME");
            updateMemoryBudgetBuildPhase();
        }
        super.build(buffer);
        processedFrames++;
    }

    /**
     * Updates Memory Budget During Build Phase
     */
    private void updateMemoryBudgetBuildPhase(){
        int newBudget = new Random().nextInt(2*memSizeInFrames)+ this.memSizeInFrames;
    }

    @Override
    public void spillPartition(int pid) throws HyracksDataException {
        if(eventBased){
            LOGGER.info("Update Memory Budget Based on EVENT");
            updateMemoryBudgetBuildPhase();
        }
        super.spillPartition(pid);
    }
    @Override
    public void completeProbe(IFrameWriter writer) throws HyracksDataException {
        super.completeProbe(writer);
        LOGGER.info("Complete HHJ Round");
        if(timer != null) {
            timer.cancel();
        }
    }
}
