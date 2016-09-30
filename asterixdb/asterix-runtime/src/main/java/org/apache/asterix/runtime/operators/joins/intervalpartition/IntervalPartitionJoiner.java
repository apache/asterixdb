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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.BufferInfo;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.join.AbstractMergeJoiner;
import org.apache.hyracks.dataflow.std.join.MergeJoinLocks;
import org.apache.hyracks.dataflow.std.join.MergeStatus;
import org.apache.hyracks.dataflow.std.structures.RunFilePointer;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class IntervalPartitionJoiner extends AbstractMergeJoiner {

    private static final Logger LOGGER = Logger.getLogger(IntervalPartitionJoiner.class.getName());

    private final RunFileWriter probeRunFileWriter;
    private int probeRunFilePid = -1;

    private final ITuplePartitionComputer buildHpc;
    private final ITuplePartitionComputer probeHpc;

    private final int buildMemory;
    private final int k;
    private final int numOfPartitions;
    private long buildSize = 0;
    private long probeSize = 0;
    private final TreeMap<RunFilePointer, Integer> probeRunFilePointers;

    private final VPartitionTupleBufferManager buildBufferManager;
    private final TuplePointer tempPtr = new TuplePointer();
    private final List<Integer> buildInMemoryPartitions;
    private final FrameTupleAccessor accessorBuild;
    private BufferInfo bufferInfo;

    private long spillWriteCount = 0;
    private long spillReadCount = 0;
    private long joinComparisonCount = 0;
    private long joinResultCount = 0;
    private final IIntervalMergeJoinChecker imjc;
    private final FrameTupleAccessor accessorProbe;
    private final IFrame reloadBuffer;
    private boolean moreBuildProcessing = true;
    private final List<IFrameBufferManager> fbms = new ArrayList<>();

    public IntervalPartitionJoiner(IHyracksTaskContext ctx, int memorySize, int partition, int k, MergeStatus status,
            MergeJoinLocks locks, IIntervalMergeJoinChecker imjc, RecordDescriptor leftRd, RecordDescriptor rightRd,
            ITuplePartitionComputer buildHpc, ITuplePartitionComputer probeHpc) throws HyracksDataException {
        super(ctx, partition, status, locks, leftRd, rightRd);

        bufferInfo = new BufferInfo(null, -1, -1);

        this.accessorProbe = new FrameTupleAccessor(leftRd);
        reloadBuffer = new VSizeFrame(ctx);

        this.numOfPartitions = IntervalPartitionUtil.getMaxPartitions(k);;
        this.imjc = imjc;

        // TODO fix available memory size
        this.buildMemory = memorySize;
        buildBufferManager = new VPartitionTupleBufferManager(ctx, VPartitionTupleBufferManager.NO_CONSTRAIN,
                numOfPartitions, buildMemory * ctx.getInitialFrameSize());

        this.k = k;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;

        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile("IntervalPartitionJoiner");
        probeRunFileWriter = new RunFileWriter(file, ctx.getIOManager());
        probeRunFileWriter.open();

        probeRunFilePointers = new TreeMap<>(RunFilePointer.ASC);
        buildInMemoryPartitions = new LinkedList<>();

        this.accessorBuild = new FrameTupleAccessor(rightRd);

        LOGGER.setLevel(Level.FINE);
        System.out.println("IntervalIndexJoiner: Logging level is: " + LOGGER.getLevel());
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("IntervalIndexJoiner has started partition " + partition + " with " + memorySize
                    + " frames of memory.");
        }
    }

    @Override
    public void processLeftFrame(IFrameWriter writer) throws HyracksDataException {
        while (inputAccessor[LEFT_PARTITION].exists()) {
            int pid = probeHpc.partition(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(), k);

            if (probeRunFilePid != pid) {
                // Log new partition locations.
                RunFilePointer rfp = new RunFilePointer(probeRunFileWriter.getFileSize(),
                        inputAccessor[LEFT_PARTITION].getTupleId());
                probeRunFilePointers.put(rfp, pid);
                probeRunFilePid = pid;
            }
            inputAccessor[LEFT_PARTITION].next();
            probeSize++;
        }
        inputBuffer[LEFT_PARTITION].rewind();
        probeRunFileWriter.nextFrame(inputBuffer[LEFT_PARTITION]);
        spillWriteCount++;
    }

    @Override
    public void processLeftClose(IFrameWriter writer) throws HyracksDataException {
        joinLoopOnMemory(writer);

        // Flush result.
        resultAppender.write(writer, true);
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("IntervalPartitionJoiner statitics: " + k + " k, " + joinComparisonCount + " comparisons, "
                    + joinResultCount + " results, " + spillWriteCount + " written, " + spillReadCount + " read.");
        }
    }

    private void joinLoopOnMemory(IFrameWriter writer) throws HyracksDataException {
        RunFileReader pReader = probeRunFileWriter.createDeleteOnCloseReader();
        pReader.open();
        // Load first frame.
        loadReaderNextFrame(pReader);

        while (moreBuildProcessing) {
            fillMemory();
            joinMemoryBlockWithRunFile(writer, pReader);

            // Clean up
            for (int pid : buildInMemoryPartitions) {
                buildBufferManager.clearPartition(pid);
            }
            buildInMemoryPartitions.clear();
        }
        pReader.close();
    }

    private void joinMemoryBlockWithRunFile(IFrameWriter writer, RunFileReader pReader) throws HyracksDataException {
        // Join Disk partitions with Memory partitions
        for (RunFilePointer probeId : probeRunFilePointers.navigableKeySet()) {
            Pair<Integer, Integer> probe = IntervalPartitionUtil.getIntervalPartition(probeRunFilePointers.get(probeId),
                    k);
            for (int buildId : buildInMemoryPartitions) {
                Pair<Integer, Integer> build = IntervalPartitionUtil.getIntervalPartition(buildId, k);
                if (imjc.compareIntervalPartition(probe.first, probe.second, build.first, build.second)) {
                    fbms.add(buildBufferManager.getPartitionFrameBufferManager(buildId));
                }
            }
            if (!fbms.isEmpty()) {
                join(pReader, probeId, fbms, writer);
            }
            fbms.clear();
        }
    }

    private void join(RunFileReader pReader, RunFilePointer rfpStart, List<IFrameBufferManager> buildFbms,
            IFrameWriter writer) throws HyracksDataException {
        long fileOffsetStart = rfpStart.getFileOffset();
        int tupleStart = rfpStart.getTupleIndex();

        RunFilePointer rfpEnd = probeRunFilePointers.higherKey(rfpStart);
        long fileOffsetEnd = rfpEnd == null ? pReader.getFileSize() : rfpEnd.getFileOffset();
        int tupleEnd = rfpEnd == null ? Integer.MAX_VALUE : rfpEnd.getTupleIndex();

        if (pReader.getReadPointer() != fileOffsetStart) {
            pReader.reset(fileOffsetStart);
            loadReaderNextFrame(pReader);
        }
        do {
            int start = pReader.getReadPointer() == fileOffsetStart ? tupleStart : 0;
            int end = pReader.getReadPointer() == fileOffsetEnd ? tupleEnd : accessorProbe.getTupleCount();

            for (int i = start; i < end; ++i) {
                // Tuple has potential match from build phase
                for (IFrameBufferManager fbm : buildFbms) {
                    joinTupleWithMemoryPartition(accessorProbe, i, fbm, writer);
                }
            }
        } while (pReader.getReadPointer() < fileOffsetEnd && loadReaderNextFrame(pReader));
    }

    private boolean loadReaderNextFrame(RunFileReader pReader) throws HyracksDataException {
        if (pReader.nextFrame(reloadBuffer)) {
            accessorProbe.reset(reloadBuffer.getBuffer());
            spillReadCount++;
            return true;
        }
        return false;
    }

    public void joinTupleWithMemoryPartition(IFrameTupleAccessor accessorProbe, int probeTupleIndex,
            IFrameBufferManager fbm, IFrameWriter writer) throws HyracksDataException {
        if (fbm.getNumFrames() == 0) {
            return;
        }
        fbm.resetIterator();
        int frameIndex = fbm.next();
        while (fbm.exists()) {
            fbm.getFrame(frameIndex, bufferInfo);
            accessorBuild.reset(bufferInfo.getBuffer());
            for (int buildTupleIndex = 0; buildTupleIndex < accessorBuild.getTupleCount(); ++buildTupleIndex) {
                if (imjc.checkToSaveInResult(accessorBuild, buildTupleIndex, accessorProbe, probeTupleIndex, false)) {
                    appendToResult(accessorBuild, buildTupleIndex, accessorProbe, probeTupleIndex, writer);
                }
                joinComparisonCount++;
            }
            frameIndex = fbm.next();
        }
    }

    private void appendToResult(IFrameTupleAccessor accessorBuild, int buildSidetIx, IFrameTupleAccessor accessorProbe,
            int probeSidetIx, IFrameWriter writer) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, resultAppender, accessorBuild, buildSidetIx, accessorProbe,
                probeSidetIx);
        joinResultCount++;
    }

    private void fillMemory() throws HyracksDataException {
        int buildPid = -1;
        TupleStatus ts;
        for (ts = loadRightTuple(); ts.isLoaded(); ts = loadRightTuple()) {
            int pid = buildHpc.partition(inputAccessor[RIGHT_PARTITION], inputAccessor[RIGHT_PARTITION].getTupleId(),
                    k);
            if (!buildBufferManager.insertTuple(pid, inputAccessor[RIGHT_PARTITION],
                    inputAccessor[RIGHT_PARTITION].getTupleId(), tempPtr)) {
                return;
            }

            if (buildPid != pid) {
                // Track new partitions in memory.
                buildInMemoryPartitions.add(pid);
                buildPid = pid;
            }
            inputAccessor[RIGHT_PARTITION].next();
            buildSize++;
        }
        if (ts.isEmpty()) {
            moreBuildProcessing = false;
        }
    }

    private TupleStatus loadRightTuple() throws HyracksDataException {
        TupleStatus loaded = loadMemoryTuple(RIGHT_PARTITION);
        if (loaded == TupleStatus.UNKNOWN) {
            loaded = pauseAndLoadRightTuple();
        }
        return loaded;
    }

}
