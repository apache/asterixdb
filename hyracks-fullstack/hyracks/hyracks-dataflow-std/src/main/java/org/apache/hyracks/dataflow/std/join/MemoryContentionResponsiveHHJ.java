package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.*;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class MemoryContentionResponsiveHHJ extends OptimizedHybridHashJoin {
    private int processedFrames = 0;
    private int frameInterval = 0;
    private boolean eventBased = false;
    private int originalBudget;

    /**
     * Timer Task to update Memory Budget when running on Time Strategy.
     */
    class UpdateMemoryTask extends TimerTask {
        public void run() {
            LOGGER.info("Update Memory Budget Based on TIME");
            try {
                updateMemoryBudgetBuildPhase();
            } catch (HyracksDataException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public MemoryContentionResponsiveHHJ(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions, String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd, ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval, IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1,int timeInterval,int frameInterval,boolean eventBased) {
        super(jobletCtx, memSizeInFrames, numOfPartitions, probeRelName, buildRelName, probeRd, buildRd, probeHpc, buildHpc, probePredEval, buildPredEval, isLeftOuter, nullWriterFactories1);
        this.frameInterval = frameInterval;
        this.eventBased = eventBased;
        originalBudget = memSizeInFrames;
    }


    public void initBuild() throws HyracksDataException {
        IDeallocatableFramePool framePool =
                new DeallocatableFramePoolDynamicBudget(jobletCtx, memSizeInFrames * jobletCtx.getInitialFrameSize());
        initBuildInternal(framePool);
    }

    public void build(ByteBuffer buffer)  throws HyracksDataException {
        if(frameInterval > 0 && processedFrames % frameInterval == 0){
            LOGGER.info("Update Memory Budget Based on FRAME");
            updateMemoryBudgetBuildPhase();
        }
        super.build(buffer);
        processedFrames++;
    }

    public void closeBuild() throws HyracksDataException {
        try{
            super.closeBuild();
        }catch (Exception ex){
            this.fail();
        }
    }

    /**
     * Updates Memory Budget During Build Phase
     */
    private void updateMemoryBudgetBuildPhase() throws HyracksDataException {
        int newBudgetInFrames = new Random().nextInt(originalBudget) + this.originalBudget;
        if(newBudgetInFrames >= memSizeInFrames){
            bufferManager.updateMemoryBudget(newBudgetInFrames);
            memSizeInFrames = newBudgetInFrames;
        }
        else {
            LOGGER.info("Memory Contention Build");
            while (!bufferManager.updateMemoryBudget(newBudgetInFrames)) {
                int victimPartition = spillPolicy.findSpilledPartitionWithMaxMemoryUsage();
                if (victimPartition < 0) {
                    victimPartition = spillPolicy.findInMemPartitionWithMaxMemoryUsage();
                }
                if (victimPartition < 0) {
                    break;
                }
                int framesToRelease = bufferManager.getPhysicalSize(victimPartition) / jobletCtx.getInitialFrameSize();
                spillPartition(victimPartition);
                memSizeInFrames -= framesToRelease;
                memSizeInFrames = memSizeInFrames <= newBudgetInFrames ? newBudgetInFrames : memSizeInFrames;
            }
        }
    }

    protected void processTupleBuildPhase(int tid, int pid) throws HyracksDataException {
        // insertTuple prevents the tuple to acquire a number of frames that is > the frame limit
        if (eventBased) {
            if(!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
                updateMemoryBudgetBuildPhase();
                super.processTupleBuildPhaseInternal(tid,pid);
            }
        }
        else {
            super.processTupleBuildPhaseInternal(tid,pid);
        }
    }
    @Override
    public void completeProbe(IFrameWriter writer) throws HyracksDataException {
        super.completeProbe(writer);
    }
}
