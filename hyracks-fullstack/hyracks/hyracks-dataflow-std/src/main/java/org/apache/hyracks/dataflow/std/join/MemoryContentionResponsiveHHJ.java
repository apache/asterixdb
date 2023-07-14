package org.apache.hyracks.dataflow.std.join;

import org.apache.http.util.Asserts;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.*;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Random;

public class MemoryContentionResponsiveHHJ extends OptimizedHybridHashJoin {
    private int processedFrames = 0;
    private int frameInterval = 10;
    private boolean eventBased = false;
    private int originalBudget;
    private Random random = new Random();
    ITuplePairComparator comparator;
    BitSet inconsistentStatus;

    //Knobs
    private boolean memoryExpansionBuild = true;
    private boolean memoryContentionBuild = true;
    private boolean memoryExpansionProbe = false;
    private boolean memoryContentionProbe = true;
    private boolean probeInconsistentThisRound = true;

    public MemoryContentionResponsiveHHJ(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions, String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd, ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval, IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) {
        super(jobletCtx, memSizeInFrames, numOfPartitions, probeRelName, buildRelName, probeRd, buildRd, probeHpc, buildHpc, probePredEval, buildPredEval, isLeftOuter, nullWriterFactories1);
        originalBudget = memSizeInFrames;
        inconsistentStatus = new BitSet(numOfPartitions);
    }

    public void initBuild() throws HyracksDataException {
        IDeallocatableFramePool framePool =
                new DeallocatableFramePoolDynamicBudget(jobletCtx, memSizeInFrames * jobletCtx.getInitialFrameSize());
        initBuildInternal(framePool);
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        if (frameInterval > 0 && processedFrames % frameInterval == 0) {
            updateMemoryBudgetBuildPhase();
        }
        super.build(buffer);
        processedFrames++;
    }

    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        if (frameInterval > 0 &&processedFrames % frameInterval == 0) {
            updateMemoryBudgetProbePhase();
        }
        super.probe(buffer, writer);
        processedFrames++;
    }

    public void closeBuild() throws HyracksDataException {
        try {
            super.closeBuild();
        } catch (Exception ex) {
            this.fail();
        }
    }

    /**
     * Updates Memory Budget During Build Phase
     */
    private void updateMemoryBudgetBuildPhase() throws HyracksDataException {
        int newBudgetInFrames = random.nextInt(originalBudget) + this.originalBudget;
        if (newBudgetInFrames >= memSizeInFrames && memoryExpansionBuild) {
            bufferManager.updateMemoryBudget(newBudgetInFrames);
            memSizeInFrames = newBudgetInFrames;
        } else if(memoryContentionBuild){
            while (!bufferManager.updateMemoryBudget(newBudgetInFrames)) {
                int victimPartition = spillPolicy.findSpilledPartitionWithMaxMemoryUsage();
                if (victimPartition < 0) {
                    victimPartition = spillPolicy.findInMemPartitionWithMaxMemoryUsage();
                }
                if (victimPartition < 0) {
                    break;
                }
                int framesToRelease = bufferManager.getPhysicalSize(victimPartition) / jobletCtx.getInitialFrameSize();
                LOGGER.info(String.format("Spill Due to Memory Contention Partition:%d", victimPartition));
                spillPartition(victimPartition);
                memSizeInFrames -= framesToRelease;
                memSizeInFrames = memSizeInFrames <= newBudgetInFrames ? newBudgetInFrames : memSizeInFrames;
            }
        }
    }

    private void updateMemoryBudgetProbePhase() throws HyracksDataException {
        int newBudgetInFrames = random.nextInt(originalBudget) + this.originalBudget;
        if (newBudgetInFrames > memSizeInFrames && memoryExpansionProbe) { //Memory Expansion Scenario
            if (bufferManager.updateMemoryBudget(newBudgetInFrames)) {
                memSizeInFrames = newBudgetInFrames;
            }
            memoryExpansionProbe();
        }
        else if (memoryContentionProbe){
            memoryContentionPorbe(newBudgetInFrames);
        }
    }

    private void memoryContentionPorbe(int newBudgetInFrames) throws HyracksDataException {
        while (!bufferManager.updateMemoryBudget(newBudgetInFrames)) {
            int victimPartition = spillPolicy.findInMemPartitionWithMaxMemoryUsage();
            if (victimPartition < 0) {
                break;
            }
            int framesToRelease = bufferManager.getPhysicalSize(victimPartition) / jobletCtx.getInitialFrameSize();
            LOGGER.info(String.format("Spill Due to Memory Contention PROBE Partition:%d", victimPartition));
            spillPartition(victimPartition);
            memSizeInFrames -= framesToRelease;
            memSizeInFrames = memSizeInFrames <= newBudgetInFrames ? newBudgetInFrames : memSizeInFrames;
        }
    }

    private void memoryExpansionProbe() throws HyracksDataException {
        freeSpace fs = calculateFreeSpace();
        int partitionToReload = selectAPartitionToReload(fs.freeSpace, 0, fs.tuplesInMemory);
        int inconsistentPartitions = inconsistentStatus.cardinality();
        while (partitionToReload >= 0) {
            if (partitionToReload >= 0) {
                //Flush Tuples that are in the probe output buffer of the partition that will be reloaded to disk.
                //Before reloading Build Tuples into memory.
                //Clear the outputbuffer later
                if (bufferManager.getNumTuples(partitionToReload) > 0) {
                    LOGGER.info(String.format("Probe later: %d",bufferManager.getNumTuples(partitionToReload) ));
                    RunFileWriter probeRFWriter =
                            getSpillWriterOrCreateNewOneIfNotExist(probeRFWriters, probeRelName, partitionToReload);
                    bufferManager.flushPartition(partitionToReload, probeRFWriter);
                    LOGGER.info(String.format("Probe Partition %d Size:%d",partitionToReload,probeRFWriter.getFileSize()));
                    bufferManager.clearPartition(partitionToReload);
                }
                //Reload Build Tuples from disk into memory.
                RunFileWriter buildRFWriter =
                        getSpillWriterOrCreateNewOneIfNotExist(buildRFWriters, buildRelName, partitionToReload);
                if(loadSpilledPartitionToMem(partitionToReload, buildRFWriter)){
                    inconsistentStatus.set(partitionToReload, true);
                }
            }
            fs = calculateFreeSpace();
            partitionToReload = selectAPartitionToReload(fs.freeSpace, 0, fs.tuplesInMemory);
        }
        rebuildHashTable();
    }

    @Override
    public RunFileReader getProbeRFReader(int pid) throws HyracksDataException {
        return probeRFWriters[pid] == null ? null : probeRFWriters[pid].createReader();
    }
    protected boolean loadSpilledPartitionToMem(int pid, RunFileWriter wr) throws HyracksDataException {
        RunFileReader r = wr.createReader();
            r.open();
            if (reloadBuffer == null) {
                reloadBuffer = new VSizeFrame(jobletCtx);
            }
            while (r.nextFrame(reloadBuffer)) {
                if (stats != null) {
                    //TODO: be certain it is the case this is actually eagerly read
                    stats.getBytesRead().update(reloadBuffer.getBuffer().limit());
                }
                accessorBuild.reset(reloadBuffer.getBuffer());
                for (int tid = 0; tid < accessorBuild.getTupleCount(); tid++) {
                    if (!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
                        // for some reason (e.g. fragmentation) if inserting fails, we need to clear the occupied frames
                        bufferManager.clearPartition(pid);
                        return false;
                    }
                }
            }
            // Closes and deletes the run file if it is already loaded into memory.
        spilledStatus.set(pid, false);
        buildRFWriters[pid] = null;
        return true;
    }

    private void rebuildHashTable() throws HyracksDataException {
        freeSpace fs = calculateFreeSpace();
        ISerializableTable table2 = new SerializableHashTable(fs.tuplesInMemory, jobletCtx, bufferManagerForHashTable);
        this.inMemJoiner = new InMemoryHashJoin(jobletCtx, new FrameTupleAccessor(probeRd), probeHpc,
                new FrameTupleAccessor(buildRd), buildRd, buildHpc, isLeftOuter, nonMatchWriters, table2, isReversed,
                bufferManagerForHashTable);
        inMemJoiner.setComparator(this.comparator);
        bufferManager.setConstrain(VPartitionTupleBufferManager.NO_CONSTRAIN);
        buildHashTable();
    }

    @Override
    public void initProbe(ITuplePairComparator comparator) {
        this.comparator = comparator;
        probePSizeInTups = new int[numOfPartitions];
        inMemJoiner.setComparator(comparator);
        bufferManager.setConstrain(VPartitionTupleBufferManager.NO_CONSTRAIN);
    }

    @Override
    public BitSet getPartitionStatus() {
        inconsistentStatus.or(spilledStatus);
        return inconsistentStatus;
    }

    protected void processTupleBuildPhase(int tid, int pid) throws HyracksDataException {
        //If event Based Memory adaption is on and the first insert try fails updates the memory budget.
        if (eventBased) {
            if (!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
                updateMemoryBudgetBuildPhase();
                super.processTupleBuildPhaseInternal(tid, pid);
            }
        } else {
            super.processTupleBuildPhaseInternal(tid, pid);
        }
    }

    @Override
    public void completeProbe(IFrameWriter writer) throws HyracksDataException {
        if(probeInconsistentThisRound){
            probeInconsistent(writer);
        }
        super.completeProbe(writer);
    }


    private void probeInconsistent(IFrameWriter writer) throws HyracksDataException {
        for(int i = inconsistentStatus.nextSetBit(0);i<numOfPartitions && i >= 0;i = inconsistentStatus.nextSetBit(i+1)){
            if(spilledStatus.get(i)){
                return;
            }
            RunFileReader reader = getProbeRFReader(i);
            if(reader != null) {
                LOGGER.info(String.format("Partition %d is Inconsistent file has %d bytes", i, reader.getFileSize()));
                reader.open();
                while(reader.nextFrame(reloadBuffer)) {
                    accessorProbe.reset(reloadBuffer.getBuffer());
                    LOGGER.info(String.format("Number of Tuples: %d", accessorProbe.getTupleCount()));
                    probe(reloadBuffer.getBuffer(),writer);
                }
            }

        }

    }
    protected void closeAllSpilledPartitions(RunFileWriter[] runFileWriters, String refName) throws HyracksDataException {
        spillRemaining(runFileWriters, refName);
    }
}
