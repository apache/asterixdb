/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.operators.joins.intervalpartition;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedMemoryConstrain;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * This class mainly applies one level of HHJ on a pair of
 * relations. It is always called by the descriptor.
 */
public class IntervalPartitionJoin {

    // Used for special probe BigObject which can not be held into the Join memory
    private FrameTupleAppender bigProbeFrameAppender;

    enum SIDE {
        BUILD,
        PROBE
    }

    private IHyracksTaskContext ctx;

    private final String buildRelName;
    private final String probeRelName;

    private final ITuplePartitionComputer buildHpc;
    private final ITuplePartitionComputer probeHpc;

    private final RecordDescriptor buildRd;
    private final RecordDescriptor probeRd;

    private RunFileWriter[] buildRFWriters; //writing spilled build partitions
    private RunFileWriter[] probeRFWriters; //writing spilled probe partitions

    private final int memForJoin;
    private final int k;
    private final int numOfPartitions;
    private InMemoryIntervalPartitionJoin inMemJoiner[]; //Used for joining resident partitions

    private VPartitionTupleBufferManager buildBufferManager;
    private VPartitionTupleBufferManager probeBufferManager;

    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;

    private static final Logger LOGGER = Logger.getLogger(IntervalPartitionJoin.class.getName());

    // stats information
    private IntervalPartitionJoinData ipjd;

    private IFrame reloadBuffer;
    private TuplePointer tempPtr = new TuplePointer();

    private IIntervalMergeJoinChecker imjc;

    public IntervalPartitionJoin(IHyracksTaskContext ctx, int memForJoin, int k, int numOfPartitions,
            String buildRelName, String probeRelName, IIntervalMergeJoinChecker imjc, RecordDescriptor buildRd,
            RecordDescriptor probeRd, ITuplePartitionComputer buildHpc, ITuplePartitionComputer probeHpc) {
        this.ctx = ctx;
        this.memForJoin = memForJoin;
        this.k = k;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;
        this.imjc = imjc;
        this.buildRelName = buildRelName;
        this.probeRelName = probeRelName;

        this.numOfPartitions = numOfPartitions;
        this.buildRFWriters = new RunFileWriter[numOfPartitions];
        this.probeRFWriters = new RunFileWriter[numOfPartitions];
        this.inMemJoiner = new InMemoryIntervalPartitionJoin[numOfPartitions];

        this.accessorBuild = new FrameTupleAccessor(buildRd);
        this.accessorProbe = new FrameTupleAccessor(probeRd);

        ipjd = new IntervalPartitionJoinData(k, imjc, numOfPartitions);
    }

    public void initBuild() throws HyracksDataException {
        buildBufferManager = new VPartitionTupleBufferManager(ctx, getPartitionMemoryConstrain(), numOfPartitions,
                memForJoin * ctx.getInitialFrameSize());
    }

    private IPartitionedMemoryConstrain getPartitionMemoryConstrain() {
        IPartitionedMemoryConstrain constrain = new IPartitionedMemoryConstrain() {
            @Override
            public int frameLimit(int partitionId) {
                return Integer.MAX_VALUE;
            }
        };
        return constrain;
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        accessorBuild.reset(buffer);
        int tupleCount = accessorBuild.getTupleCount();

        for (int i = 0; i < tupleCount; ++i) {
            int pid = buildHpc.partition(accessorBuild, i, k);
            processTuple(i, pid);
            ipjd.buildIncrementCount(pid);
        }
    }

    public void closeBuild() throws HyracksDataException {
        int inMemoryPartitions = 0, totalBuildPartitions = 0;
        flushAndClearBuildSpilledPartition();

        // Trying to bring back as many spilled partitions as possible, making them resident
        bringBackSpilledPartitionIfHasMoreMemory();

        // Update build partition join map based on partitions with actual data.
        for (int i = ipjd.buildNextInMemory(0); i >= 0; i = ipjd.buildNextInMemory(i + 1)) {
            if (ipjd.buildGetCount(i) == 0) {
                ipjd.buildRemoveFromJoin(i);
            } else if (ipjd.buildGetCount(i) > 0) {
                // Set up build memory for processing joins for partitions in memory.
                createInMemoryJoiner(i);
                inMemoryPartitions++;
                totalBuildPartitions += ipjd.buildGetCount(i);
            }
        }

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("IntervalPartitionJoin has closed the build phase. Total tuples: " + totalBuildPartitions
                    + ", In memory partitions: " + inMemoryPartitions + ", Spilled partitions: "
                    + ipjd.buildGetSpilledCount());
        }
    }

    private void processTuple(int tid, int pid) throws HyracksDataException {
        while (!buildBufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
            int victimPartition = selectPartitionToSpill();
            if (victimPartition < 0) {
                throw new HyracksDataException(
                        "No more space left in the memory buffer, please give join more memory budgets.");
            }
            spillPartition(victimPartition);
        }
    }

    private int selectPartitionToSpill() {
        int partitionToSpill = selectLargestSpilledPartition();
        int maxToSpillPartSize = 0;
        if (partitionToSpill < 0 || (maxToSpillPartSize = buildBufferManager.getPhysicalSize(partitionToSpill)) == ctx
                .getInitialFrameSize()) {
            int partitionInMem = selectNextInMemoryPartitionToSpill();
            if (partitionInMem >= 0 && buildBufferManager.getPhysicalSize(partitionInMem) > maxToSpillPartSize) {
                partitionToSpill = partitionInMem;
            }
        }
        return partitionToSpill;
    }

    /**
     * Select next partition to spill. The partitions have been numbered in the order they should be spilled.
     *
     * @return
     */
    private int selectNextInMemoryPartitionToSpill() {
        for (int i = ipjd.buildNextInMemoryWithResults(0); i >= 0; i = ipjd.buildNextInMemoryWithResults(i + 1)) {
            if (!ipjd.buildIsSpilled(i) && buildBufferManager.getPhysicalSize(i) > 0) {
                return i;
            }
        }
        return -1;
    }

    private int selectLargestSpilledPartition() {
        int pid = -1;
        int max = 0;
        for (int i = ipjd.buildNextSpilled(0); i >= 0; i = ipjd.buildNextSpilled(i + 1)) {
            int partSize = buildBufferManager.getPhysicalSize(i);
            if (partSize > max) {
                max = partSize;
                pid = i;
            }
        }
        return pid;
    }

    private void spillPartition(int pid) throws HyracksDataException {
        RunFileWriter writer = getSpillWriterOrCreateNewOneIfNotExist(pid, SIDE.BUILD);
        buildBufferManager.flushPartition(pid, writer);
        buildBufferManager.clearPartition(pid);
        ipjd.buildSpill(pid);
    }

    private RunFileWriter getSpillWriterOrCreateNewOneIfNotExist(int pid, SIDE whichSide) throws HyracksDataException {
        RunFileWriter[] runFileWriters = null;
        String refName = null;
        switch (whichSide) {
            case BUILD:
                runFileWriters = buildRFWriters;
                refName = buildRelName;
                break;
            case PROBE:
                refName = probeRelName;
                runFileWriters = probeRFWriters;
                break;
        }
        RunFileWriter writer = runFileWriters[pid];
        if (writer == null) {
            FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(refName);
            writer = new RunFileWriter(file, ctx.getIOManager());
            writer.open();
            runFileWriters[pid] = writer;
        }
        return writer;
    }

    public void clearBuildMemory() throws HyracksDataException {
        for (int pid = 0; pid < numOfPartitions; ++pid) {
            if (buildBufferManager.getNumTuples(pid) > 0) {
                buildBufferManager.clearPartition(pid);
                ipjd.buildRemoveFromJoin(pid);
            }
        }
    }

    private void flushAndClearBuildSpilledPartition() throws HyracksDataException {
        for (int pid = ipjd.buildNextSpilled(0); pid >= 0; pid = ipjd.buildNextSpilled(pid + 1)) {
            if (buildBufferManager.getNumTuples(pid) > 0) {
                buildBufferManager.flushPartition(pid, getSpillWriterOrCreateNewOneIfNotExist(pid, SIDE.BUILD));
                buildBufferManager.clearPartition(pid);
                buildRFWriters[pid].close();
            }
        }
    }

    private void flushAndClearProbeSpilledPartition() throws HyracksDataException {
        for (int pid = 0; pid < numOfPartitions; ++pid) {
            if (probeBufferManager.getNumTuples(pid) > 0) {
                probeBufferManager.flushPartition(pid, getSpillWriterOrCreateNewOneIfNotExist(pid, SIDE.PROBE));
                probeBufferManager.clearPartition(pid);
                probeRFWriters[pid].close();
            }
        }
    }

    private void bringBackSpilledPartitionIfHasMoreMemory() throws HyracksDataException {
        // we need number of |spilledPartitions| buffers to store the probe data
        int freeSpace = (memForJoin - ipjd.buildGetSpilledCount()) * ctx.getInitialFrameSize();
        for (int i = ipjd.buildNextInMemoryWithResults(0); i >= 0; i = ipjd.buildNextInMemoryWithResults(i + 1)) {
            freeSpace -= buildBufferManager.getPhysicalSize(i);
        }

        int pid = 0;
        while ((pid = selectPartitionsToReload(freeSpace, pid)) >= 0) {
            if (!loadPartitionInMem(pid, buildRFWriters[pid])) {
                return;
            }
            freeSpace -= buildBufferManager.getPhysicalSize(pid);
        }
    }

    private boolean loadPartitionInMem(int pid, RunFileWriter wr) throws HyracksDataException {
        RunFileReader r = wr.createDeleteOnCloseReader();
        r.open();
        if (reloadBuffer == null) {
            reloadBuffer = new VSizeFrame(ctx);
        }
        while (r.nextFrame(reloadBuffer)) {
            accessorBuild.reset(reloadBuffer.getBuffer());
            for (int tid = 0; tid < accessorBuild.getTupleCount(); tid++) {
                if (!buildBufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
                    // for some reason (e.g. due to fragmentation) if the inserting failed, we need to clear the occupied frames
                    buildBufferManager.clearPartition(pid);
                    r.close();
                    return false;
                }
            }
        }

        r.close();
        ipjd.buildLoad(pid);
        buildRFWriters[pid] = null;
        return true;
    }

    private int selectPartitionsToReload(int freeSpace, int pid) {
        for (int id = ipjd.buildNextSpilled(0); id >= 0; id = ipjd.buildNextSpilled(id + 1)) {
            assert buildRFWriters[id].getFileSize() > 0 : "How comes a spilled partition have size 0?";
            if (freeSpace >= buildRFWriters[id].getFileSize()) {
                return id;
            }
        }
        return -1;
    }

    private void createInMemoryJoiner(int pid) throws HyracksDataException {
        this.inMemJoiner[pid] = new InMemoryIntervalPartitionJoin(ctx,
                buildBufferManager.getPartitionFrameBufferManager(pid), imjc, buildRd, probeRd);
    }

    private void closeInMemoryJoiner(int pid, IFrameWriter writer) throws HyracksDataException {
        this.inMemJoiner[pid].closeJoin(writer);
        this.inMemJoiner[pid] = null;
    }

    public void initProbe() throws HyracksDataException {
        int probeMemory = numOfPartitions > memForJoin ? memForJoin : numOfPartitions;
        probeBufferManager = new VPartitionTupleBufferManager(ctx, getPartitionMemoryConstrain(), numOfPartitions,
                (probeMemory) * ctx.getInitialFrameSize());

        probeRFWriters = new RunFileWriter[numOfPartitions];
    }

    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();

        for (int i = 0; i < tupleCount; ++i) {
            int pid = probeHpc.partition(accessorProbe, i, k);
            if (!ipjd.hasProbeJoinMap(pid)) {
                // Set probe join map
                ipjd.setProbeJoinMap(pid,
                        IntervalPartitionUtil.getProbeJoinPartitions(pid, ipjd.buildPSizeInTups, imjc, k));
            }

            // Tuple has potential match from build phase
            if (!ipjd.isProbeJoinMapEmpty(pid)) {
                if (ipjd.probeHasSpilled(pid)) {
                    // pid is Spilled
                    while (!probeBufferManager.insertTuple(pid, accessorProbe, i, tempPtr)) {
                        int victim = pid;
                        if (probeBufferManager.getNumTuples(pid) == 0) {
                            // current pid is empty, choose the biggest one
                            victim = selectLargestSpilledPartition();
                        }
                        if (victim < 0) {
                            // current tuple is too big for all the free space
                            flushBigProbeObjectToDisk(pid, accessorProbe, i);
                            break;
                        }
                        RunFileWriter runFileWriter = getSpillWriterOrCreateNewOneIfNotExist(victim, SIDE.PROBE);
                        probeBufferManager.flushPartition(victim, runFileWriter);
                        probeBufferManager.clearPartition(victim);
                    }
                }
                for (Iterator<Integer> pidIterator = ipjd.getProbeJoinMap(pid); pidIterator.hasNext();) {
                    // pid has join partitions that are Resident
                    int j = pidIterator.next();
                    if (inMemJoiner[j] != null) {
                        inMemJoiner[j].join(accessorProbe, i, writer);
                    }
                }
            }
            ipjd.probeIncrementCount(pid);
        }
    }

    public void closeProbe(IFrameWriter writer) throws HyracksDataException {
        // We do NOT join the spilled partitions here, that decision is made at the descriptor level (which join technique to use)
        for (int i = 0; i < inMemJoiner.length; ++i) {
            if (inMemJoiner[i] != null) {
                closeInMemoryJoiner(i, writer);
                ipjd.buildLogJoined(i);
            }
        }
        clearBuildMemory();
        flushAndClearProbeSpilledPartition();
        probeBufferManager.close();
        probeBufferManager = null;
    }

    private void flushBigProbeObjectToDisk(int pid, FrameTupleAccessor accessorProbe, int i)
            throws HyracksDataException {
        if (bigProbeFrameAppender == null) {
            bigProbeFrameAppender = new FrameTupleAppender(new VSizeFrame(ctx));
        }
        RunFileWriter runFileWriter = getSpillWriterOrCreateNewOneIfNotExist(pid, SIDE.PROBE);
        if (!bigProbeFrameAppender.append(accessorProbe, i)) {
            throw new HyracksDataException("The given tuple is too big");
        }
        bigProbeFrameAppender.write(runFileWriter, true);
    }

    public RunFileReader getBuildRFReader(int pid) throws HyracksDataException {
        return ((buildRFWriters[pid] == null) ? null : (buildRFWriters[pid]).createReader());
    }

    public RunFileReader getProbeRFReader(int pid) throws HyracksDataException {
        return ((probeRFWriters[pid] == null) ? null : (probeRFWriters[pid]).createReader());
    }

    public void joinSpilledPartitions(IFrameWriter writer) throws HyracksDataException {
        LinkedHashMap<Integer, LinkedHashSet<Integer>> probeInMemoryJoinMap;
        if (reloadBuffer == null) {
            reloadBuffer = new VSizeFrame(ctx);
        }
        HashSet<Integer> inMemory = new HashSet<Integer>();
        while (ipjd.buildGetSpilledCount() > 0) {
            // Load back spilled build partitions.
            // TODO only load partition required for spill join. Consider both sides.
            bringBackSpilledPartitionIfHasMoreMemory();

            probeInMemoryJoinMap = ipjd.probeGetInMemoryJoinMap();

            // Create in memory joiners.
            for (int pid = ipjd.buildNextInMemoryWithResults(0); pid >= 0; pid = ipjd
                    .buildNextInMemoryWithResults(pid + 1)) {
                createInMemoryJoiner(pid);
                inMemory.add(pid);
            }

            // Join all build partitions with disk probe partitions.
            for (Entry<Integer, LinkedHashSet<Integer>> entry : probeInMemoryJoinMap.entrySet()) {
                if (ipjd.probeGetCount(entry.getKey()) > 0 && probeInMemoryJoinMap.get(entry.getKey()).size() > 0) {
                    RunFileReader pReader = getProbeRFReader(entry.getKey());
                    pReader.open();
                    while (pReader.nextFrame(reloadBuffer)) {
                        accessorProbe.reset(reloadBuffer.getBuffer());
                        for (int i = 0; i < accessorProbe.getTupleCount(); ++i) {
                            // Tuple has potential match from build phase
                            for (Integer j : probeInMemoryJoinMap.get(entry.getKey())) {
                                // j has join partitions that are Resident
                                if (inMemJoiner[j] != null) {
                                    inMemJoiner[j].join(accessorProbe, i, writer);
                                }
                            }
                        }
                    }
                    pReader.close();
                }
            }

            // Clean up build memory.
            for (int pid = ipjd.buildNextInMemoryWithResults(0); pid >= 0; pid = ipjd
                    .buildNextInMemoryWithResults(pid + 1)) {
                closeInMemoryJoiner(pid, writer);
                ipjd.buildLogJoined(pid);
            }
            inMemory.clear();
            clearBuildMemory();
        }
    }

    class IntervalPartitionJoinData {
        private LinkedHashMap<Integer, LinkedHashSet<Integer>> probeJoinMap;

        private int[] buildPSizeInTups;
        private int[] probePSizeInTups;

        private BitSet buildJoinedCompleted; //0=waiting, 1=joined
        private BitSet buildSpilledStatus; //0=resident, 1=spilled
        private BitSet buildInMemoryStatus; //0=unknown, 1=resident
        private BitSet probeSpilledStatus; //0=resident, 1=spilled

        public IntervalPartitionJoinData(int k, IIntervalMergeJoinChecker imjc, int numberOfPartitions) {
            probeJoinMap = new LinkedHashMap<Integer, LinkedHashSet<Integer>>();

            buildPSizeInTups = new int[numberOfPartitions];
            probePSizeInTups = new int[numberOfPartitions];

            buildJoinedCompleted = new BitSet(numberOfPartitions);
            buildInMemoryStatus = new BitSet(numberOfPartitions);
            buildSpilledStatus = new BitSet(numberOfPartitions);
            probeSpilledStatus = new BitSet(numberOfPartitions);
        }

        public LinkedHashMap<Integer, LinkedHashSet<Integer>> probeGetInMemoryJoinMap() {
            return IntervalPartitionUtil.getInMemorySpillJoinMap(probeJoinMap, buildInMemoryStatus, probeSpilledStatus);
        }

        public boolean hasProbeJoinMap(int pid) {
            return probeJoinMap.containsKey(pid);
        }

        public boolean isProbeJoinMapEmpty(int pid) {
            return probeJoinMap.get(pid).isEmpty();
        }

        public Iterator<Integer> getProbeJoinMap(int pid) {
            return probeJoinMap.get(pid).iterator();
        }

        public void setProbeJoinMap(int pid, LinkedHashSet<Integer> map) {
            probeJoinMap.put(new Integer(pid), map);
            for (Integer i : map) {
                if (buildIsSpilled(i)) {
                    // Build join partition has spilled. Now spill the probe also.
                    probeSpilledStatus.set(pid);
                }
            }
        }

        public void buildIncrementCount(int pid) {
            buildInMemoryStatus.set(pid);
            buildPSizeInTups[pid]++;
        }

        public int buildGetCount(int pid) {
            return buildPSizeInTups[pid];
        }

        public void buildLogJoined(int pid) {
            buildSpilledStatus.clear(pid);
            buildJoinedCompleted.set(pid);
        }

        public void buildRemoveFromJoin(int pid) {
            buildSpilledStatus.clear(pid);
            buildJoinedCompleted.set(pid);
        }

        public boolean buildHasBeenJoined(int pid) {
            return buildJoinedCompleted.get(pid);
        }

        public int buildGetSpilledCount() {
            return buildSpilledStatus.cardinality();
        }

        public void buildSpill(int pid) {
            buildInMemoryStatus.clear(pid);
            buildSpilledStatus.set(pid);
        }

        public void buildLoad(int pid) {
            buildInMemoryStatus.set(pid);
            buildSpilledStatus.clear(pid);
        }

        public boolean buildIsSpilled(int pid) {
            return buildSpilledStatus.get(pid);
        }

        public int buildNextSpilled(int pid) {
            return buildSpilledStatus.nextSetBit(pid);
        }

        public int buildNextInMemoryWithResults(int pid) {
            pid = buildNextInMemory(pid);
            do {
                if (pid < 0 || buildGetCount(pid) > 0) {
                    return pid;
                }
                pid = buildNextInMemory(pid + 1);
            } while (pid >= 0);
            return -1;
        }

        public int buildNextInMemory(int pid) {
            pid = buildSpilledStatus.nextClearBit(pid);
            if (pid >= numOfPartitions) {
                return -1;
            }
            do {
                if (!buildHasBeenJoined(pid)) {
                    return pid;
                }
                pid = buildSpilledStatus.nextClearBit(pid + 1);
            } while (pid >= 0 && pid < numOfPartitions);
            return -1;
        }

        public void probeIncrementCount(int pid) {
            probePSizeInTups[pid]++;
        }

        public int probeGetCount(int pid) {
            return probePSizeInTups[pid];
        }

        public void probeSpill(int pid) {
            probeSpilledStatus.set(pid);
        }

        public boolean probeHasSpilled(int pid) {
            return probeSpilledStatus.get(pid);
        }

        public int buildGetMaxPartitionSize() {
            int max = buildPSizeInTups[0];
            for (int i = 1; i < buildPSizeInTups.length; i++) {
                if (buildPSizeInTups[i] > max) {
                    max = buildPSizeInTups[i];
                }
            }
            return max;
        }

        public int probeGetMaxPartitionSize() {
            int max = probePSizeInTups[0];
            for (int i = 1; i < probePSizeInTups.length; i++) {
                if (probePSizeInTups[i] > max) {
                    max = probePSizeInTups[i];
                }
            }
            return max;
        }

    }

    public void closeAndDeleteRunFiles() throws HyracksDataException {
        for (RunFileWriter rfw : buildRFWriters) {
            if (rfw != null) {
                FileUtils.deleteQuietly(rfw.getFileReference().getFile());
            }
        }
        for (RunFileWriter rfw : probeRFWriters) {
            if (rfw != null) {
                FileUtils.deleteQuietly(rfw.getFileReference().getFile());
            }
        }
    }

}
