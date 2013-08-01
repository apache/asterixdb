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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluator;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import edu.uci.ics.hyracks.dataflow.common.data.partition.RepartitionComputerGeneratorFactory;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable;
import edu.uci.ics.hyracks.dataflow.std.structures.SerializableHashTable;

/**
 * @author pouria
 *         This class guides the joining process, and switches between different
 *         joining techniques, w.r.t the implemented optimizations and skew in size of the
 *         partitions.
 *         - Operator overview:
 *         Assume we are trying to do (R Join S), with M buffers available, while we have an estimate on the size
 *         of R (in terms of buffers). HHJ (Hybrid Hash Join) has two main phases: Build and Probe, where in our implementation Probe phase
 *         can apply HHJ recursively, based on the value of M and size of R and S. HHJ phases proceed as follow:
 *         BUILD:
 *         Calculate number of partitions (Based on the size of R, fudge factor and M) [See Shapiro's paper for the detailed discussion].
 *         Initialize the build phase (one frame per partition, all partitions considered resident at first)
 *         Read tuples of R, frame by frame, and hash each tuple (based on a given hash function) to find
 *         its target partition and try to append it to that partition:
 *         If target partition's buffer is full, try to allocate a new buffer for it.
 *         if no free buffer is available, find the largest resident partition and spill it. Using its freed
 *         buffers after spilling, allocate a new buffer for the target partition.
 *         Being done with R, close the build phase. (During closing we write the very last buffer of each
 *         spilled partition to the disk, and we do partition tuning, where we try to bring back as many buffers, belonging to
 *         spilled partitions as possible into memory, based on the free buffers - We will stop at the point where remaining free buffers is not enough
 *         for reloading an entire partition back into memory)
 *         Create the hash table for the resident partitions (basically we create an in-memory hash join here)
 *         PROBE:
 *         Initialize the probe phase on S (mainly allocate one buffer per spilled partition, and one buffer
 *         for the whole resident partitions)
 *         Read tuples of S, frame by frame and hash each tuple T to its target partition P
 *         if P is a resident partition, pass T to the in-memory hash join and generate the output record,
 *         if any matching(s) record found
 *         if P is spilled, write T to the dedicated buffer for P (on the probe side)
 *         Once scanning of S is done, we try to join partition pairs (Ri, Si) of the spilled partitions:
 *         if any of Ri or Si is smaller than M, then we simply use an in-memory hash join to join them
 *         otherwise we apply HHJ recursively:
 *         if after applying HHJ recursively, we do not gain enough size reduction (max size of the
 *         resulting partitions were more than 80% of the initial Ri,Si size) then we switch to
 *         nested loop join for joining.
 *         (At each step of partition-pair joining, we consider role reversal, which means if size of Si were
 *         greater than Ri, then we make sure that we switch the roles of build/probe between them)
 */

public class OptimizedHybridHashJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int BUILD_AND_PARTITION_ACTIVITY_ID = 0;
    private static final int PARTITION_AND_JOIN_ACTIVITY_ID = 1;

    private static final long serialVersionUID = 1L;
    private static final double NLJ_SWITCH_THRESHOLD = 0.8;

    private static final String PROBE_REL = "RelR";
    private static final String BUILD_REL = "RelS";

    private final int memsize;
    private final int inputsize0;
    private final double fudgeFactor;
    private final int[] probeKeys;
    private final int[] buildKeys;
    private final IBinaryHashFunctionFamily[] hashFunctionGeneratorFactories;
    private final IBinaryComparatorFactory[] comparatorFactories; //For in-mem HJ
    private final ITuplePairComparatorFactory tuplePairComparatorFactory0; //For NLJ in probe
    private final ITuplePairComparatorFactory tuplePairComparatorFactory1; //For NLJ in probe
    private final IPredicateEvaluatorFactory predEvaluatorFactory;
    
    private final boolean isLeftOuter;
    private final INullWriterFactory[] nullWriterFactories1;
    
    private static final Logger LOGGER = Logger.getLogger(OptimizedHybridHashJoinOperatorDescriptor.class.getName());

    public OptimizedHybridHashJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memsize, int inputsize0,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFamily[] hashFunctionGeneratorFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor,
            ITuplePairComparatorFactory tupPaircomparatorFactory0,
            ITuplePairComparatorFactory tupPaircomparatorFactory1, IPredicateEvaluatorFactory predEvaluatorFactory, boolean isLeftOuter,
            INullWriterFactory[] nullWriterFactories1) throws HyracksDataException {

        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.fudgeFactor = factor;
        this.probeKeys = keys0;
        this.buildKeys = keys1;
        this.hashFunctionGeneratorFactories = hashFunctionGeneratorFactories;
        this.comparatorFactories = comparatorFactories;
        this.tuplePairComparatorFactory0 = tupPaircomparatorFactory0;
        this.tuplePairComparatorFactory1 = tupPaircomparatorFactory1;
        recordDescriptors[0] = recordDescriptor;
        this.predEvaluatorFactory = predEvaluatorFactory;
        this.isLeftOuter = isLeftOuter;
        this.nullWriterFactories1 = nullWriterFactories1;
    }

    public OptimizedHybridHashJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memsize, int inputsize0,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFamily[] hashFunctionGeneratorFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor,
            ITuplePairComparatorFactory tupPaircomparatorFactory0, ITuplePairComparatorFactory tupPaircomparatorFactory1, IPredicateEvaluatorFactory predEvaluatorFactory)
            throws HyracksDataException {

        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.fudgeFactor = factor;
        this.probeKeys = keys0;
        this.buildKeys = keys1;
        this.hashFunctionGeneratorFactories = hashFunctionGeneratorFactories;
        this.comparatorFactories = comparatorFactories;
        this.tuplePairComparatorFactory0 = tupPaircomparatorFactory0;
        this.tuplePairComparatorFactory1 = tupPaircomparatorFactory1;
        this.predEvaluatorFactory = predEvaluatorFactory;
        recordDescriptors[0] = recordDescriptor;
        this.isLeftOuter = false;
        this.nullWriterFactories1 = null;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId buildAid = new ActivityId(odId, BUILD_AND_PARTITION_ACTIVITY_ID);
        ActivityId probeAid = new ActivityId(odId, PARTITION_AND_JOIN_ACTIVITY_ID);
        PartitionAndBuildActivityNode phase1 = new PartitionAndBuildActivityNode(buildAid, probeAid);
        ProbeAndJoinActivityNode phase2 = new ProbeAndJoinActivityNode(probeAid, buildAid);

        builder.addActivity(this, phase1);
        builder.addSourceEdge(1, phase1, 0);

        builder.addActivity(this, phase2);
        builder.addSourceEdge(0, phase2, 0);

        builder.addBlockingEdge(phase1, phase2);

        builder.addTargetEdge(0, phase2, 0);

    }

    //memorySize is the memory for join (we have already excluded the 2 buffers for in/out)
    private int getNumberOfPartitions(int memorySize, int buildSize, double factor, int nPartitions)
            throws HyracksDataException {
        int numberOfPartitions = 0;
        if (memorySize <= 1) {
            throw new HyracksDataException("not enough memory is available for Hybrid Hash Join");
        }
        if (memorySize > buildSize) {
            return 1; //We will switch to in-Mem HJ eventually
        }
        numberOfPartitions = (int) (Math.ceil((double) (buildSize * factor / nPartitions - memorySize)
                / (double) (memorySize - 1)));
        if (numberOfPartitions <= 0) {
            numberOfPartitions = 1; //becomes in-memory hash join
        }
        if (numberOfPartitions > memorySize) {
            numberOfPartitions = (int) Math.ceil(Math.sqrt(buildSize * factor / nPartitions));
            return (numberOfPartitions < memorySize ? numberOfPartitions : memorySize);
        }
        return numberOfPartitions;
    }

    public static class BuildAndPartitionTaskState extends AbstractStateObject {
    	
        private int memForJoin;
        private int numOfPartitions;
        private OptimizedHybridHashJoin hybridHJ;

        public BuildAndPartitionTaskState() {
        }

        private BuildAndPartitionTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }

    }

    /*
     * Build phase of Hybrid Hash Join:
     * Creating an instance of Hybrid Hash Join, using Shapiro's formula
     * to get the optimal number of partitions, build relation is read and
     * partitioned, and hybrid hash join instance gets ready for the probing.
     * (See OptimizedHybridHashJoin for the details on different steps)
     */
    private class PartitionAndBuildActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId probeAid;

        public PartitionAndBuildActivityNode(ActivityId id, ActivityId probeAid) {
            super(id);
            this.probeAid = probeAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {

            final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(probeAid, 0);

            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; i++) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }
            
            final IPredicateEvaluator predEvaluator = ( predEvaluatorFactory == null ? null : predEvaluatorFactory.createPredicateEvaluator());
            
            
            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private BuildAndPartitionTaskState state = new BuildAndPartitionTaskState(ctx.getJobletContext()
                        .getJobId(), new TaskId(getActivityId(), partition));

                ITuplePartitionComputer probeHpc = new FieldHashPartitionComputerFamily(probeKeys,
                        hashFunctionGeneratorFactories).createPartitioner(0);
                ITuplePartitionComputer buildHpc = new FieldHashPartitionComputerFamily(buildKeys,
                        hashFunctionGeneratorFactories).createPartitioner(0);

                @Override
                public void open() throws HyracksDataException {
                    if (memsize <= 2) { //Dedicated buffers: One buffer to read and one buffer for output
                        throw new HyracksDataException("not enough memory for Hybrid Hash Join");
                    }
                    state.memForJoin = memsize - 2;
                    state.numOfPartitions = getNumberOfPartitions(state.memForJoin, inputsize0, fudgeFactor,
                            nPartitions);
                    if(!isLeftOuter){
                    	state.hybridHJ = new OptimizedHybridHashJoin(ctx, state.memForJoin, state.numOfPartitions,
                                PROBE_REL, BUILD_REL, probeKeys, buildKeys, comparators, probeRd, buildRd, probeHpc,
                                buildHpc, predEvaluator);
                    }
                    else{
                    	state.hybridHJ = new OptimizedHybridHashJoin(ctx, state.memForJoin, state.numOfPartitions,
                                PROBE_REL, BUILD_REL, probeKeys, buildKeys, comparators, probeRd, buildRd, probeHpc,
                                buildHpc, predEvaluator, isLeftOuter, nullWriterFactories1);
                    }
                    
                    state.hybridHJ.initBuild();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.hybridHJ.build(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.hybridHJ.closeBuild();
                    ctx.setStateObject(state);
                    LOGGER.log(Level.FINE, "OptimizedHybridHashJoin closed its build phase");
                }

                @Override
                public void fail() throws HyracksDataException {
                }

            };
            return op;
        }
    }

    /*
     * Probe phase of Hybrid Hash Join:
     * Reading the probe side and partitioning it, resident tuples get
     * joined with the build side residents (through formerly created HybridHashJoin in the build phase)
     * and spilled partitions get written to run files. During the close() call, pairs of spilled partition
     * (build side spilled partition and its corresponding probe side spilled partition) join, by applying
     * Hybrid Hash Join recursively on them.
     */
    private class ProbeAndJoinActivityNode extends AbstractActivityNode {
    	
        private static final long serialVersionUID = 1L;

        private final ActivityId buildAid;

        public ProbeAndJoinActivityNode(ActivityId id, ActivityId buildAid) {
            super(id);
            this.buildAid = buildAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
                throws HyracksDataException {

            final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(buildAid, 0);
            final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            final ITuplePairComparator nljComparator0 = tuplePairComparatorFactory0.createTuplePairComparator(ctx);
            final ITuplePairComparator nljComparator1 = tuplePairComparatorFactory1.createTuplePairComparator(ctx);
            final IPredicateEvaluator predEvaluator = ( predEvaluatorFactory == null ? null : predEvaluatorFactory.createPredicateEvaluator());
            
            for (int i = 0; i < comparatorFactories.length; i++) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }

            final INullWriter[] nullWriters1 = isLeftOuter ? new INullWriter[nullWriterFactories1.length] : null;
            if (isLeftOuter) {
                for (int i = 0; i < nullWriterFactories1.length; i++) {
                    nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
                }
            }

            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private BuildAndPartitionTaskState state;
                private ByteBuffer rPartbuff = ctx.allocateFrame();

                private ITuplePartitionComputerFamily hpcf0 = new FieldHashPartitionComputerFamily(probeKeys,
                        hashFunctionGeneratorFactories);
                private ITuplePartitionComputerFamily hpcf1 = new FieldHashPartitionComputerFamily(buildKeys,
                        hashFunctionGeneratorFactories);

                private ITuplePartitionComputer hpcRep0;
                private ITuplePartitionComputer hpcRep1;

                @Override
                public void open() throws HyracksDataException {
                    state = (BuildAndPartitionTaskState) ctx.getStateObject(new TaskId(new ActivityId(getOperatorId(),
                            BUILD_AND_PARTITION_ACTIVITY_ID), partition));

                    writer.open();
                    state.hybridHJ.initProbe();

                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                	state.hybridHJ.probe(buffer, writer);
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }

                @Override
                public void close() throws HyracksDataException {

                    state.hybridHJ.closeProbe(writer);

                    BitSet partitionStatus = state.hybridHJ.getPartitinStatus();
                    hpcRep0 = new RepartitionComputerGeneratorFactory(state.numOfPartitions, hpcf0)
                            .createPartitioner(0);
                    hpcRep1 = new RepartitionComputerGeneratorFactory(state.numOfPartitions, hpcf1)
                            .createPartitioner(0);

                    rPartbuff.clear();
                    for (int pid = partitionStatus.nextSetBit(0); pid >= 0; pid = partitionStatus.nextSetBit(pid + 1)) {

                        RunFileReader bReader = state.hybridHJ.getBuildRFReader(pid);
                        RunFileReader pReader = state.hybridHJ.getProbeRFReader(pid);

                        if (bReader == null || pReader == null) { //either of sides (or both) does not have any tuple, thus no need for joining (no potential match)
                            continue;
                        }
                        int bSize = state.hybridHJ.getBuildPartitionSizeInTup(pid);
                        int pSize = state.hybridHJ.getProbePartitionSizeInTup(pid);
                        int beforeMax = (bSize > pSize) ? bSize : pSize;
                        joinPartitionPair(state.hybridHJ, bReader, pReader, pid, beforeMax, 1);

                    }
                    writer.close();
                }

                private void joinPartitionPair(OptimizedHybridHashJoin ohhj, RunFileReader buildSideReader,
                        RunFileReader probeSideReader, int pid, int beforeMax, int level) throws HyracksDataException {
                    ITuplePartitionComputer probeHpc = new FieldHashPartitionComputerFamily(probeKeys,
                            hashFunctionGeneratorFactories).createPartitioner(level);
                    ITuplePartitionComputer buildHpc = new FieldHashPartitionComputerFamily(buildKeys,
                            hashFunctionGeneratorFactories).createPartitioner(level);
                    
                    long buildPartSize = ohhj.getBuildPartitionSize(pid) / ctx.getFrameSize();
                    long probePartSize = ohhj.getProbePartitionSize(pid) / ctx.getFrameSize();
                    
                    LOGGER.log(Level.FINE,"Joining Partition Pairs (pid "+pid+") - (level "+level+") - BuildSize:\t"+buildPartSize+"\tProbeSize:\t"+probePartSize+" - MemForJoin "+(state.memForJoin));

                    //Apply in-Mem HJ if possible
                    if ((buildPartSize < state.memForJoin) || (probePartSize < state.memForJoin)) {
                        int tabSize = -1;
                        
                        if (isLeftOuter || buildPartSize < probePartSize) {
                            tabSize = ohhj.getBuildPartitionSizeInTup(pid);
                           
                            if (tabSize == 0) {
                                throw new HyracksDataException(
                                        "Trying to join an empty partition. Invalid table size for inMemoryHashJoin.");
                            }
                          //Build Side is smaller
                            applyInMemHashJoin(buildKeys, probeKeys, tabSize, probeRd, buildRd, hpcRep0, hpcRep1,
                                    buildSideReader, probeSideReader, false, pid);

                        } 
                        
                        else { //Role Reversal
                            tabSize = ohhj.getProbePartitionSizeInTup(pid);
                            if (tabSize == 0) {
                                throw new HyracksDataException(
                                        "Trying to join an empty partition. Invalid table size for inMemoryHashJoin.");
                            }
                            //Probe Side is smaller
                            
                            applyInMemHashJoin(probeKeys, buildKeys, tabSize, buildRd, probeRd, hpcRep1, hpcRep0,
                                    probeSideReader, buildSideReader, true, pid);
                        }
                    }
                    //Apply (Recursive) HHJ
                    else {
                        OptimizedHybridHashJoin rHHj;
                        if (isLeftOuter || buildPartSize < probePartSize) { //Build Side is smaller
                        	LOGGER.log(Level.FINE,"\tApply RecursiveHHJ for (pid "+pid+") - (level "+level+") [buildSize is smaller]");
                            int n = getNumberOfPartitions(state.memForJoin, (int) buildPartSize, fudgeFactor,
                                    nPartitions);
                           
                            rHHj = new OptimizedHybridHashJoin(ctx, state.memForJoin, n, PROBE_REL, BUILD_REL,
                                    probeKeys, buildKeys, comparators, probeRd, buildRd, probeHpc, buildHpc, predEvaluator);

                            buildSideReader.open();
                            rHHj.initBuild();
                            rPartbuff.clear();
                            while (buildSideReader.nextFrame(rPartbuff)) {
                                rHHj.build(rPartbuff);
                            }

                            rHHj.closeBuild();

                            probeSideReader.open();
                            rHHj.initProbe();
                            rPartbuff.clear();
                            while (probeSideReader.nextFrame(rPartbuff)) {
                                rHHj.probe(rPartbuff, writer);
                            }
                            rHHj.closeProbe(writer);

                            int maxAfterBuildSize = rHHj.getMaxBuildPartitionSize();
                            int maxAfterProbeSize = rHHj.getMaxProbePartitionSize();
                            int afterMax = (maxAfterBuildSize > maxAfterProbeSize) ? maxAfterBuildSize
                                    : maxAfterProbeSize;

                            BitSet rPStatus = rHHj.getPartitinStatus();
                            if (afterMax < NLJ_SWITCH_THRESHOLD * beforeMax) {
                                for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus.nextSetBit(rPid + 1)) {
                                    RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                                    RunFileReader rprfw = rHHj.getProbeRFReader(rPid);

                                    if (rbrfw == null || rprfw == null) {
                                        continue;
                                    }

                                    joinPartitionPair(rHHj, rbrfw, rprfw, rPid, afterMax, (level + 1));
                                }

                            } else { //Switch to NLJ (Further recursion seems not to be useful)
                            	LOGGER.log(Level.FINE,"\tSwitched to NLJ for (pid "+pid+") - (level "+level+") (reverse false) [coming from buildSize was smaller]");
                                for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus.nextSetBit(rPid + 1)) {
                                    RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                                    RunFileReader rprfw = rHHj.getProbeRFReader(rPid);
                                    
                                    if (rbrfw == null || rprfw == null) {
                                        continue;
                                    }

                                    int buildSideInTups = rHHj.getBuildPartitionSizeInTup(rPid);
                                    int probeSideInTups = rHHj.getProbePartitionSizeInTup(rPid);
                                    if (isLeftOuter || buildSideInTups < probeSideInTups) {
                                        applyNestedLoopJoin(probeRd, buildRd, state.memForJoin, rbrfw, rprfw,
                                                nljComparator0, false);
                                    } else {
                                        applyNestedLoopJoin(buildRd, probeRd, state.memForJoin, rprfw, rbrfw,
                                                nljComparator1, false);
                                    }
                                }
                            }
                        } else { //Role Reversal (Probe Side is smaller)
                        	LOGGER.log(Level.FINE,"\tApply RecursiveHHJ for (pid "+pid+") - (level "+level+") WITH REVERSAL [probeSize is smaller]");
                            int n = getNumberOfPartitions(state.memForJoin, (int) probePartSize, fudgeFactor,
                                    nPartitions);
                            
                            rHHj = new OptimizedHybridHashJoin(ctx, state.memForJoin, n, BUILD_REL, PROBE_REL,
                                    buildKeys, probeKeys, comparators, buildRd, probeRd, buildHpc, probeHpc, predEvaluator);
                            rHHj.setIsReversed(true);	//Added to use predicateEvaluator (for inMemoryHashJoin) correctly

                            probeSideReader.open();
                            rHHj.initBuild();
                            rPartbuff.clear();
                            while (probeSideReader.nextFrame(rPartbuff)) {
                                rHHj.build(rPartbuff);
                            }
                            rHHj.closeBuild();
                            rHHj.initProbe();
                            buildSideReader.open();
                            rPartbuff.clear();
                            while (buildSideReader.nextFrame(rPartbuff)) {
                                rHHj.probe(rPartbuff, writer);
                            }
                            rHHj.closeProbe(writer);
                            int maxAfterBuildSize = rHHj.getMaxBuildPartitionSize();
                            int maxAfterProbeSize = rHHj.getMaxProbePartitionSize();
                            int afterMax = (maxAfterBuildSize > maxAfterProbeSize) ? maxAfterBuildSize
                                    : maxAfterProbeSize;
                            BitSet rPStatus = rHHj.getPartitinStatus();

                            if (afterMax < NLJ_SWITCH_THRESHOLD * beforeMax) {
                                for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus.nextSetBit(rPid + 1)) {
                                    RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                                    RunFileReader rprfw = rHHj.getProbeRFReader(rPid);

                                    if (rbrfw == null || rprfw == null) {
                                        continue;
                                    }

                                    joinPartitionPair(rHHj, rprfw, rbrfw, rPid, afterMax, (level + 1));
                                }
                            } else { //Switch to NLJ (Further recursion seems not to be effective)
                            	LOGGER.log(Level.FINE,"\tSwitched to NLJ for (pid "+pid+") - (level "+level+") (reverse true) [coming from probeSize was smaller]");
                            	for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus.nextSetBit(rPid + 1)) {
                                    RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                                    RunFileReader rprfw = rHHj.getProbeRFReader(rPid);
                                    
                                    if (rbrfw == null || rprfw == null) {
                                        continue;
                                    }

                                    long buildSideSize = rbrfw.getFileSize();
                                    long probeSideSize = rprfw.getFileSize();
                                    if (buildSideSize > probeSideSize) {
                                        applyNestedLoopJoin(buildRd, probeRd, state.memForJoin, rbrfw, rprfw,
                                                nljComparator1, true);
                                    } else {
                                        applyNestedLoopJoin(probeRd, buildRd, state.memForJoin, rprfw, rbrfw,
                                                nljComparator0, true);
                                    }
                                }
                            }
                        }
                        buildSideReader.close();
                        probeSideReader.close();
                    }
                }

                private void applyInMemHashJoin(int[] bKeys, int[] pKeys, int tabSize, RecordDescriptor buildRDesc,
                        RecordDescriptor probeRDesc, ITuplePartitionComputer hpcRepLarger,
                        ITuplePartitionComputer hpcRepSmaller, RunFileReader bReader, RunFileReader pReader, boolean reverse, int pid)
                        throws HyracksDataException {
                	LOGGER.log(Level.FINE,"\t(pid "+pid+") - applyInMemHashJoin (reversal "+reverse+")");
                    ISerializableTable table = new SerializableHashTable(tabSize, ctx);
                    InMemoryHashJoin joiner = new InMemoryHashJoin(ctx, tabSize, new FrameTupleAccessor(
                            ctx.getFrameSize(), probeRDesc), hpcRepLarger, new FrameTupleAccessor(ctx.getFrameSize(),
                            buildRDesc), hpcRepSmaller, new FrameTuplePairComparator(pKeys, bKeys, comparators),
                            isLeftOuter, nullWriters1, table, predEvaluator, reverse);

                    bReader.open();
                    rPartbuff.clear();
                    while (bReader.nextFrame(rPartbuff)) {
                        ByteBuffer copyBuffer = ctx.allocateFrame(); //We need to allocate a copyBuffer, because this buffer gets added to the buffers list in the InMemoryHashJoin
                        FrameUtils.copy(rPartbuff, copyBuffer);
                        FrameUtils.makeReadable(copyBuffer);
                        joiner.build(copyBuffer);
                        rPartbuff.clear();
                    }
                    bReader.close();
                    rPartbuff.clear();
                    // probe
                    pReader.open();
                    while (pReader.nextFrame(rPartbuff)) {
                        joiner.join(rPartbuff, writer);
                        rPartbuff.clear();
                    }
                    pReader.close();
                    joiner.closeJoin(writer);
                }

                private void applyNestedLoopJoin(RecordDescriptor outerRd, RecordDescriptor innerRd, int memorySize,
                        RunFileReader outerReader, RunFileReader innerReader, ITuplePairComparator nljComparator, boolean reverse)
                        throws HyracksDataException {
                	
                    NestedLoopJoin nlj = new NestedLoopJoin(ctx, new FrameTupleAccessor(ctx.getFrameSize(), outerRd),
                            new FrameTupleAccessor(ctx.getFrameSize(), innerRd), nljComparator, memorySize, predEvaluator, false, null);

                    ByteBuffer cacheBuff = ctx.allocateFrame();
                    innerReader.open();
                    while (innerReader.nextFrame(cacheBuff)) {
                        FrameUtils.makeReadable(cacheBuff);
                        nlj.cache(cacheBuff);
                        cacheBuff.clear();
                    }
                    nlj.closeCache();

                    ByteBuffer joinBuff = ctx.allocateFrame();
                    outerReader.open();

                    while (outerReader.nextFrame(joinBuff)) {
                        FrameUtils.makeReadable(joinBuff);
                        nlj.join(joinBuff, writer);
                        joinBuff.clear();
                    }

                    nlj.closeJoin(writer);
                    outerReader.close();
                    innerReader.close();
                }
            };
            return op;
        }
    }
}
