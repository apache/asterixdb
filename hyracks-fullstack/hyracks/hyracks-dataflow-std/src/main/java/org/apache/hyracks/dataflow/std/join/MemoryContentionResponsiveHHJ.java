package org.apache.hyracks.dataflow.std.join;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;

public class MemoryContentionResponsiveHHJ extends HybridHashJoin implements IHybridHashJoin{
    private static final Logger LOGGER = LogManager.getLogger();
    public MemoryContentionResponsiveHHJ(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions,
                                         String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd,
                                         ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval,
                                         IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) {
        super(jobletCtx, memSizeInFrames, numOfPartitions, probeRelName, buildRelName, probeRd, buildRd, probeHpc, buildHpc, probePredEval, buildPredEval, isLeftOuter, nullWriterFactories1);
        dynamicMemory = true;
    }


    /**
     * Update Memory Budget, spilling partitions that can released memory.
     * <p>Partitions already spilled are prefered to be spilled again.</p>
     * @param newMemory
     * @return
     * @throws HyracksDataException
     */
    @Override
    public int updateMemoryBudget(int newMemory) throws HyracksDataException {
        int originalMemory = this.memSizeInFrames;
        if(!bufferManager.updateMemoryBudget(newMemory)) {
            this.memSizeInFrames -=  buildPartitionManager.spillToReleaseFrames(this.memSizeInFrames - newMemory);
        }
        else {
            this.memSizeInFrames = newMemory;
        }
        return -(originalMemory - this.memSizeInFrames);
    }

    @Override
    public int updateMemoryBudgetProbe(int newMemory) throws HyracksDataException {
        int originalMemory = this.memSizeInFrames;
        if(newMemory > this.memSizeInFrames){
            updateMemoryBudget(newMemory);
            bringPartitionsBack();
        }
        else if(newMemory < this.memSizeInFrames){
            if(!bufferManager.updateMemoryBudget(newMemory)) {
                this.memSizeInFrames -= buildPartitionManager.spillToReleaseFrames(this.memSizeInFrames - newMemory);
            }
            if(!bufferManager.updateMemoryBudget(newMemory)) {
                this.memSizeInFrames -=  probePartitionManager.spillToReleaseFrames(this.memSizeInFrames - newMemory);
            }
        }
        buildHashTable();
        return -(originalMemory - this.memSizeInFrames);
    }
}
