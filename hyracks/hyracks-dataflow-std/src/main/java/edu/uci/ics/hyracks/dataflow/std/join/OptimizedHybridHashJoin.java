/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.join;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable;
import edu.uci.ics.hyracks.dataflow.std.structures.SerializableHashTable;

/**
 * @author pouria
 *         This class mainly applies one level of HHJ on a pair of
 *         relations. It is always called by the descriptor.
 */
public class OptimizedHybridHashJoin {

    private final int NO_MORE_FREE_BUFFER = -1;
    private final int END_OF_PARTITION = -1;
    private final int INVALID_BUFFER = -2;
    private final int UNALLOCATED_FRAME = -3;
    private final int BUFFER_FOR_RESIDENT_PARTS = -1;

    private IHyracksTaskContext ctx;

    private final String rel0Name;
    private final String rel1Name;

    private final int[] buildKeys;
    private final int[] probeKeys;

    private final IBinaryComparator[] comparators;

    private ITuplePartitionComputer buildHpc;
    private ITuplePartitionComputer probeHpc;

    private final RecordDescriptor buildRd;
    private final RecordDescriptor probeRd;

    private RunFileWriter[] buildRFWriters; //writing spilled build partitions
    private RunFileWriter[] probeRFWriters; //writing spilled probe partitions
    
    private final IPredicateEvaluator predEvaluator;
    private final boolean isLeftOuter;
    private final INullWriter[] nullWriters1;

    private ByteBuffer[] memBuffs; //Memory buffers for build
    private int[] curPBuff; //Current (last) Buffer for each partition
    private int[] nextBuff; //Next buffer in the partition's buffer chain
    private int[] buildPSizeInTups; //Size of build partitions (in tuples)
    private int[] probePSizeInTups; //Size of probe partitions (in tuples)
    private int nextFreeBuffIx; //Index of next available free buffer to allocate/use
    private BitSet pStatus; //0=resident, 1=spilled
    private int numOfPartitions;
    private int memForJoin;
    private InMemoryHashJoin inMemJoiner; //Used for joining resident partitions

    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;
    private FrameTupleAppender buildTupAppender;
    private FrameTupleAppender probeTupAppenderToResident;
    private FrameTupleAppender probeTupAppenderToSpilled;

    private int numOfSpilledParts;
    private ByteBuffer[] sPartBuffs;    //Buffers for probe spilled partitions (one buffer per spilled partition)
    private ByteBuffer probeResBuff;    //Buffer for probe resident partition tuples
    private ByteBuffer reloadBuffer;    //Buffer for reloading spilled partitions during partition tuning 
    
    private int[] buildPSizeInFrames; //Used for partition tuning
    private int freeFramesCounter; //Used for partition tuning
    
    private boolean isTableEmpty;	//Added for handling the case, where build side is empty (tableSize is 0)
    private boolean isReversed;		//Added for handling correct calling for predicate-evaluator upon recursive calls that cause role-reversal
    
    public OptimizedHybridHashJoin(IHyracksTaskContext ctx, int memForJoin, int numOfPartitions, String rel0Name,
            String rel1Name, int[] keys0, int[] keys1, IBinaryComparator[] comparators, RecordDescriptor buildRd,
            RecordDescriptor probeRd, ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator predEval) {
        this.ctx = ctx;
        this.memForJoin = memForJoin;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;
        this.buildKeys = keys1;
        this.probeKeys = keys0;
        this.comparators = comparators;
        this.rel0Name = rel0Name;
        this.rel1Name = rel1Name;

        this.numOfPartitions = numOfPartitions;
        this.buildRFWriters = new RunFileWriter[numOfPartitions];
        this.probeRFWriters = new RunFileWriter[numOfPartitions];

        this.accessorBuild = new FrameTupleAccessor(ctx.getFrameSize(), buildRd);
        this.accessorProbe = new FrameTupleAccessor(ctx.getFrameSize(), probeRd);

        this.predEvaluator = predEval;
        this.isLeftOuter = false;
        this.nullWriters1 = null;
        this.isReversed = false;

    }

    public OptimizedHybridHashJoin(IHyracksTaskContext ctx, int memForJoin, int numOfPartitions, String rel0Name,
            String rel1Name, int[] keys0, int[] keys1, IBinaryComparator[] comparators, RecordDescriptor buildRd,
            RecordDescriptor probeRd, ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc,
            IPredicateEvaluator predEval, boolean isLeftOuter, INullWriterFactory[] nullWriterFactories1) {
        this.ctx = ctx;
        this.memForJoin = memForJoin;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;
        this.buildKeys = keys1;
        this.probeKeys = keys0;
        this.comparators = comparators;
        this.rel0Name = rel0Name;
        this.rel1Name = rel1Name;

        this.numOfPartitions = numOfPartitions;
        this.buildRFWriters = new RunFileWriter[numOfPartitions];
        this.probeRFWriters = new RunFileWriter[numOfPartitions];

        this.accessorBuild = new FrameTupleAccessor(ctx.getFrameSize(), buildRd);
        this.accessorProbe = new FrameTupleAccessor(ctx.getFrameSize(), probeRd);
        
        this.predEvaluator = predEval;
        this.isLeftOuter = isLeftOuter;
        this.isReversed = false;
        
        this.nullWriters1 = isLeftOuter ? new INullWriter[nullWriterFactories1.length] : null;
        if (isLeftOuter) {
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
            }
        }
    }

    public void initBuild() throws HyracksDataException {
        memBuffs = new ByteBuffer[memForJoin];
        curPBuff = new int[numOfPartitions];
        nextBuff = new int[memForJoin];
        pStatus = new BitSet(numOfPartitions);
        buildPSizeInTups = new int[numOfPartitions];

        buildPSizeInFrames = new int[numOfPartitions];
        freeFramesCounter = memForJoin - numOfPartitions;

        for (int i = 0; i < numOfPartitions; i++) { //Allocating one buffer per partition and setting as the head of the chain of buffers for that partition
            memBuffs[i] = ctx.allocateFrame();
            curPBuff[i] = i;
            nextBuff[i] = -1;
            buildPSizeInFrames[i] = 1; //The dedicated initial buffer
        }

        nextFreeBuffIx = ((numOfPartitions < memForJoin) ? numOfPartitions : NO_MORE_FREE_BUFFER); //Setting the chain of unallocated frames
        for (int i = numOfPartitions; i < memBuffs.length; i++) {
            nextBuff[i] = UNALLOCATED_FRAME;
        }

        buildTupAppender = new FrameTupleAppender(ctx.getFrameSize());

    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        accessorBuild.reset(buffer);
        int tupleCount = accessorBuild.getTupleCount();

        boolean print = false;
        if (print) {
            accessorBuild.prettyPrint();
        }

        for (int i = 0; i < tupleCount; ++i) {
            int pid = buildHpc.partition(accessorBuild, i, numOfPartitions);
            processTuple(i, pid);
            buildPSizeInTups[pid]++;
        }

    }

    private void processTuple(int tid, int pid) throws HyracksDataException {
        ByteBuffer partition = memBuffs[curPBuff[pid]]; //Getting current buffer for the target partition

        if (!pStatus.get(pid)) { //resident partition
            buildTupAppender.reset(partition, false);
            while (true) {
                if (buildTupAppender.append(accessorBuild, tid)) { //Tuple added to resident partition successfully
                    break;
                }
                //partition does not have enough room
                int newBuffIx = allocateFreeBuffer(pid);
                if (newBuffIx == NO_MORE_FREE_BUFFER) { //Spill one partition
                    int pidToSpill = selectPartitionToSpill();
                    if (pidToSpill == -1) { //No more partition to spill
                        throw new HyracksDataException("not enough memory for Hash Join (Allocation exceeds the limit)");
                    }
                    spillPartition(pidToSpill);
                    buildTupAppender.reset(memBuffs[pidToSpill], true);
                    processTuple(tid, pid);
                    break;
                } //New Buffer allocated successfully
                partition = memBuffs[curPBuff[pid]]; //Current Buffer for the partition is now updated by allocateFreeBuffer() call above
                buildTupAppender.reset(partition, true);
                if (!buildTupAppender.append(accessorBuild, tid)) {
                    throw new HyracksDataException("Invalid State (Can not append to newly allocated buffer)");
                }
                buildPSizeInFrames[pid]++;
                break;
            }
        } else { //spilled partition
            boolean needClear = false;
            while (true) {
                buildTupAppender.reset(partition, needClear);
                if (buildTupAppender.append(accessorBuild, tid)) {
                    break;
                }
                //Dedicated in-memory buffer for the partition is full, needed to be flushed first 
                buildWrite(pid, partition);
                partition.clear();
                needClear = true;
                buildPSizeInFrames[pid]++;
            }
        }
    }

    private int allocateFreeBuffer(int pid) throws HyracksDataException {
        if (nextFreeBuffIx != NO_MORE_FREE_BUFFER) {
            if (memBuffs[nextFreeBuffIx] == null) {
                memBuffs[nextFreeBuffIx] = ctx.allocateFrame();
            }
            int curPartBuffIx = curPBuff[pid];
            curPBuff[pid] = nextFreeBuffIx;
            int oldNext = nextBuff[nextFreeBuffIx];
            nextBuff[nextFreeBuffIx] = curPartBuffIx;
            if (oldNext == UNALLOCATED_FRAME) {
                nextFreeBuffIx++;
                if (nextFreeBuffIx == memForJoin) { //No more free buffer
                    nextFreeBuffIx = NO_MORE_FREE_BUFFER;
                }
            } else {
                nextFreeBuffIx = oldNext;
            }
            (memBuffs[curPBuff[pid]]).clear();

            freeFramesCounter--;
            return (curPBuff[pid]);
        } else {
            return NO_MORE_FREE_BUFFER; //A partitions needs to be spilled (if feasible)
        }
    }

    private int selectPartitionToSpill() {
        int maxSize = -1;
        int partitionToSpill = -1;
        for (int i = 0; i < buildPSizeInTups.length; i++) { //Find the largest partition, to spill
            if (!pStatus.get(i) && (buildPSizeInTups[i] > maxSize)) {
                maxSize = buildPSizeInTups[i];
                partitionToSpill = i;
            }
        }
        return partitionToSpill;
    }

    private void spillPartition(int pid) throws HyracksDataException {
        int curBuffIx = curPBuff[pid];
        ByteBuffer buff = null;
        while (curBuffIx != END_OF_PARTITION) {
            buff = memBuffs[curBuffIx];
            buildWrite(pid, buff);
            buff.clear();

            int freedBuffIx = curBuffIx;
            curBuffIx = nextBuff[curBuffIx];

            if (freedBuffIx != pid) {
                nextBuff[freedBuffIx] = nextFreeBuffIx;
                nextFreeBuffIx = freedBuffIx;
                freeFramesCounter++;
            }
        }
        curPBuff[pid] = pid;
        pStatus.set(pid);
    }

    private void buildWrite(int pid, ByteBuffer buff) throws HyracksDataException {
        RunFileWriter writer = buildRFWriters[pid];
        if (writer == null) {
            FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(rel0Name);
            writer = new RunFileWriter(file, ctx.getIOManager());
            writer.open();
            buildRFWriters[pid] = writer;
        }
        writer.nextFrame(buff);
    }

    public void closeBuild() throws HyracksDataException {
        for (int i = 0; i < numOfPartitions; i++) { //Remove Empty Partitions' allocated frame
            if (buildPSizeInTups[i] == 0) {
                buildPSizeInFrames[i]--;
                nextBuff[curPBuff[i]] = nextFreeBuffIx;
                nextFreeBuffIx = curPBuff[i];
                curPBuff[i] = INVALID_BUFFER;
                freeFramesCounter++;
            }
        }

        ByteBuffer buff = null;
        for (int i = pStatus.nextSetBit(0); i >= 0; i = pStatus.nextSetBit(i + 1)) { //flushing and DeAllocating the dedicated buffers for the spilled partitions
            buff = memBuffs[i];
            accessorBuild.reset(buff);
            if (accessorBuild.getTupleCount() > 0) {
                buildWrite(i, buff);
                buildPSizeInFrames[i]++;
            }
            nextBuff[i] = nextFreeBuffIx;
            nextFreeBuffIx = i;
            freeFramesCounter++;
            curPBuff[i] = INVALID_BUFFER;

            if (buildRFWriters[i] != null) {
                buildRFWriters[i].close();
            }
        }

        partitionTune(); //Trying to bring back as many spilled partitions as possible, making them resident

        int inMemTupCount = 0;
        numOfSpilledParts = 0;

        for (int i = 0; i < numOfPartitions; i++) {
            if (!pStatus.get(i)) {
                inMemTupCount += buildPSizeInTups[i];
            } else {
                numOfSpilledParts++;
            }
        }

        createInMemoryJoiner(inMemTupCount);
        cacheInMemJoin();
        this.isTableEmpty = (inMemTupCount == 0);
    }

    private void partitionTune() throws HyracksDataException {
        reloadBuffer = ctx.allocateFrame();
        ArrayList<Integer> reloadSet = selectPartitionsToReload();
        for (int i = 0; i < reloadSet.size(); i++) {
            int pid = reloadSet.get(i);
            int[] buffsToLoad = new int[buildPSizeInFrames[pid]];
            for (int j = 0; j < buffsToLoad.length; j++) {
                buffsToLoad[j] = nextFreeBuffIx;
                int oldNext = nextBuff[nextFreeBuffIx];
                if (oldNext == UNALLOCATED_FRAME) {
                    nextFreeBuffIx++;
                    if (nextFreeBuffIx == memForJoin) { //No more free buffer
                        nextFreeBuffIx = NO_MORE_FREE_BUFFER;
                    }
                } else {
                    nextFreeBuffIx = oldNext;
                }

            }
            curPBuff[pid] = buffsToLoad[0];
            for (int k = 1; k < buffsToLoad.length; k++) {
                nextBuff[buffsToLoad[k - 1]] = buffsToLoad[k];
            }
            loadPartitionInMem(pid, buildRFWriters[pid], buffsToLoad);
        }
        reloadSet.clear();
        reloadSet = null;
    }

    private void loadPartitionInMem(int pid, RunFileWriter wr, int[] buffs) throws HyracksDataException {
        RunFileReader r = wr.createReader();
        r.open();
        int counter = 0;
        ByteBuffer mBuff = null;
        reloadBuffer.clear();
        while (r.nextFrame(reloadBuffer)) {
            mBuff = memBuffs[buffs[counter]];
            if (mBuff == null) {
                mBuff = ctx.allocateFrame();
                memBuffs[buffs[counter]] = mBuff;
            }
            FrameUtils.copy(reloadBuffer, mBuff);
            counter++;
            reloadBuffer.clear();
        }

        int curNext = nextBuff[buffs[buffs.length - 1]];
        nextBuff[buffs[buffs.length - 1]] = END_OF_PARTITION;
        nextFreeBuffIx = curNext;

        r.close();
        pStatus.set(pid, false);
        buildRFWriters[pid] = null;
    }

    private ArrayList<Integer> selectPartitionsToReload() {
        ArrayList<Integer> p = new ArrayList<Integer>();
        for (int i = pStatus.nextSetBit(0); i >= 0; i = pStatus.nextSetBit(i + 1)) {
            if (buildPSizeInFrames[i] > 0 && (freeFramesCounter - buildPSizeInFrames[i] >= 0)) {
                p.add(i);
                freeFramesCounter -= buildPSizeInFrames[i];
            }
            if (freeFramesCounter < 1) { //No more free buffer available
                return p;
            }
        }
        return p;
    }

    private void createInMemoryJoiner(int inMemTupCount) throws HyracksDataException {
        ISerializableTable table = new SerializableHashTable(inMemTupCount, ctx);
        this.inMemJoiner = new InMemoryHashJoin(ctx, inMemTupCount,
                new FrameTupleAccessor(ctx.getFrameSize(), probeRd), probeHpc, new FrameTupleAccessor(
                        ctx.getFrameSize(), buildRd), buildHpc, new FrameTuplePairComparator(probeKeys, buildKeys,
                        comparators), isLeftOuter, nullWriters1, table, predEvaluator, isReversed);
    }

    private void cacheInMemJoin() throws HyracksDataException {

        for (int pid = 0; pid < numOfPartitions; pid++) {
            if (!pStatus.get(pid)) {
                int nextBuffIx = curPBuff[pid];
                while (nextBuffIx > -1) { //It is not Invalid or End_Of_Partition
                    inMemJoiner.build(memBuffs[nextBuffIx]);
                    nextBuffIx = nextBuff[nextBuffIx];
                }
            }
        }
    }

    public void initProbe() throws HyracksDataException {

        sPartBuffs = new ByteBuffer[numOfSpilledParts];
        for (int i = 0; i < numOfSpilledParts; i++) {
            sPartBuffs[i] = ctx.allocateFrame();
        }
        curPBuff = new int[numOfPartitions];
        int nextBuffIxToAlloc = 0;
        /* We only need to allocate one frame per spilled partition. 
         * Resident partitions do not need frames in probe, as their tuples join 
         * immediately with the resident build tuples using the inMemoryHashJoin */
        for (int i = 0; i < numOfPartitions; i++) {
            curPBuff[i] = (pStatus.get(i)) ? nextBuffIxToAlloc++ : BUFFER_FOR_RESIDENT_PARTS;
        }
        probePSizeInTups = new int[numOfPartitions];
        probeRFWriters = new RunFileWriter[numOfPartitions];

        probeResBuff = ctx.allocateFrame();

        probeTupAppenderToResident = new FrameTupleAppender(ctx.getFrameSize());
        probeTupAppenderToResident.reset(probeResBuff, true);

        probeTupAppenderToSpilled = new FrameTupleAppender(ctx.getFrameSize());

    }

    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();

        boolean print = false;
        if (print) {
            accessorProbe.prettyPrint();
        }

        if (numOfSpilledParts == 0) {
            inMemJoiner.join(buffer, writer);
            return;
        }

        for (int i = 0; i < tupleCount; ++i) {
            int pid = probeHpc.partition(accessorProbe, i, numOfPartitions);

            if (buildPSizeInTups[pid] > 0) { //Tuple has potential match from previous phase
                if (pStatus.get(pid)) { //pid is Spilled
                    boolean needToClear = false;
                    ByteBuffer buff = sPartBuffs[curPBuff[pid]];
                    while (true) {
                        probeTupAppenderToSpilled.reset(buff, needToClear);
                        if (probeTupAppenderToSpilled.append(accessorProbe, i)) {
                            break;
                        }
                        probeWrite(pid, buff);
                        buff.clear();
                        needToClear = true;
                    }
                } else { //pid is Resident
                    while (true) {
                        if (probeTupAppenderToResident.append(accessorProbe, i)) {
                            break;
                        }
                        inMemJoiner.join(probeResBuff, writer);
                        probeTupAppenderToResident.reset(probeResBuff, true);
                    }

                }
                probePSizeInTups[pid]++;
            }

        }

    }

    public void closeProbe(IFrameWriter writer) throws HyracksDataException { //We do NOT join the spilled partitions here, that decision is made at the descriptor level (which join technique to use)
        inMemJoiner.join(probeResBuff, writer);
        inMemJoiner.closeJoin(writer);

        for (int pid = pStatus.nextSetBit(0); pid >= 0; pid = pStatus.nextSetBit(pid + 1)) {
            ByteBuffer buff = sPartBuffs[curPBuff[pid]];
            accessorProbe.reset(buff);
            if (accessorProbe.getTupleCount() > 0) {
                probeWrite(pid, buff);
            }
            closeProbeWriter(pid);
        }
    }

    private void probeWrite(int pid, ByteBuffer buff) throws HyracksDataException {
        RunFileWriter pWriter = probeRFWriters[pid];
        if (pWriter == null) {
            FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(rel1Name);
            pWriter = new RunFileWriter(file, ctx.getIOManager());
            pWriter.open();
            probeRFWriters[pid] = pWriter;
        }
        pWriter.nextFrame(buff);
    }

    private void closeProbeWriter(int pid) throws HyracksDataException {
        RunFileWriter writer = probeRFWriters[pid];
        if (writer != null) {
            writer.close();
        }
    }

    public RunFileReader getBuildRFReader(int pid) throws HyracksDataException {
        return ((buildRFWriters[pid] == null) ? null : (buildRFWriters[pid]).createReader());
    }

    public long getBuildPartitionSize(int pid) {
        return ((buildRFWriters[pid] == null) ? 0 : buildRFWriters[pid].getFileSize());
    }

    public int getBuildPartitionSizeInTup(int pid) {
        return (buildPSizeInTups[pid]);
    }

    public RunFileReader getProbeRFReader(int pid) throws HyracksDataException {
        return ((probeRFWriters[pid] == null) ? null : (probeRFWriters[pid]).createReader());
    }

    public long getProbePartitionSize(int pid) {
        return ((probeRFWriters[pid] == null) ? 0 : probeRFWriters[pid].getFileSize());
    }

    public int getProbePartitionSizeInTup(int pid) {
        return (probePSizeInTups[pid]);
    }

    public int getMaxBuildPartitionSize() {
        int max = buildPSizeInTups[0];
        for (int i = 1; i < buildPSizeInTups.length; i++) {
            if (buildPSizeInTups[i] > max) {
                max = buildPSizeInTups[i];
            }
        }
        return max;
    }

    public int getMaxProbePartitionSize() {
        int max = probePSizeInTups[0];
        for (int i = 1; i < probePSizeInTups.length; i++) {
            if (probePSizeInTups[i] > max) {
                max = probePSizeInTups[i];
            }
        }
        return max;
    }

    public BitSet getPartitinStatus() {
        return pStatus;
    }

    public String debugGetStats() {
        int numOfResidentPartitions = 0;
        int numOfSpilledPartitions = 0;
        double sumOfBuildSpilledSizes = 0;
        double sumOfProbeSpilledSizes = 0;
        int numOfInMemTups = 0;
        for (int i = 0; i < numOfPartitions; i++) {
            if (pStatus.get(i)) { //Spilled
                numOfSpilledPartitions++;
                sumOfBuildSpilledSizes += buildPSizeInTups[i];
                sumOfProbeSpilledSizes += probePSizeInTups[i];
            } else { //Resident
                numOfResidentPartitions++;
                numOfInMemTups += buildPSizeInTups[i];
            }
        }

        double avgBuildSpSz = sumOfBuildSpilledSizes / numOfSpilledPartitions;
        double avgProbeSpSz = sumOfProbeSpilledSizes / numOfSpilledPartitions;
        String s = "Resident Partitions:\t" + numOfResidentPartitions + "\nSpilled Partitions:\t"
                + numOfSpilledPartitions + "\nAvg Build Spilled Size:\t" + avgBuildSpSz + "\nAvg Probe Spilled Size:\t"
                + avgProbeSpSz + "\nIn-Memory Tups:\t" + numOfInMemTups + "\nNum of Free Buffers:\t"
                + freeFramesCounter;
        return s;
    }

    public boolean isTableEmpty() {
        return this.isTableEmpty;
    }
    
    public void setIsReversed(boolean b){
    	this.isReversed = b;
    }
}
