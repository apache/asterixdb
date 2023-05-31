package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MemoryContentionResponsiveHHJ extends HybridHashJoin implements IHybridHashJoin{
    private static final Logger LOGGER = LogManager.getLogger();

    public MemoryContentionResponsiveHHJ(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions,
                                         String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd,
                                         ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval,
                                         IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) {
        super(jobletCtx, memSizeInFrames, numOfPartitions, probeRelName, buildRelName, probeRd, buildRd, probeHpc, buildHpc, probePredEval, buildPredEval, isLeftOuter, nullWriterFactories1);
        dynamicMemory = true;
        this.deleteAfterReload = false;
    }


    /**
     * Update Memory Budget, spilling partitions that can released memory.
     * <p>Partitions already spilled are prefered to be spilled again.</p>
     * @param newMemory
     * @return Number of Frames Released (-) or aquired (+)
     * @throws HyracksDataException Exception
     */
    @Override
    public int updateMemoryBudget(int newMemory) throws HyracksDataException {
        int originalMemory = this.memSizeInFrames;
        if(!bufferManager.updateMemoryBudget(newMemory)) { //Try to update bufferManager's Budget, this will not spill any partition.
            //If it Fails, spill candidate Partitions until the desired budget is achieved
            LOGGER.info("Spilling due to Memory Contention on Build Phase");
            this.memSizeInFrames -=  buildPartitionManager.spillToReleaseFrames(this.memSizeInFrames - newMemory);
        }
        else {
            //If was possible to update bufferManager's budget update the memSizeInFrames value.
            this.memSizeInFrames = newMemory;
        }
        return -(originalMemory - this.memSizeInFrames);
    }

    @Override
    public int updateMemoryBudgetProbe(int newMemory) throws HyracksDataException {
        int originalMemory = this.memSizeInFrames;
        if(newMemory > this.memSizeInFrames){
            updateMemoryBudget(newMemory);
            this.memSizeInFrames = newMemory;
            selectAPartitionToReload();
        }
        else if(newMemory < this.memSizeInFrames){
            if(!bufferManager.updateMemoryBudget(newMemory)) {
                this.memSizeInFrames -=  probePartitionManager.spillToReleaseFrames(this.memSizeInFrames - newMemory);
                LOGGER.info("Spilling PROBE Partition due to memory contention in Probe");
            }
            if(!bufferManager.updateMemoryBudget(newMemory)) {
                this.memSizeInFrames -= buildPartitionManager.spillToReleaseFrames(this.memSizeInFrames - newMemory);
                LOGGER.info("Spilling BUILD Partition due to memory contention in Probe");
            }
            else{
                this.memSizeInFrames = newMemory;
            }
        }
        return -(originalMemory - this.memSizeInFrames);
    }
}
