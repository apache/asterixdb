package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.dataflow.common.io.RunFileReader;

import java.nio.ByteBuffer;
import java.util.BitSet;

public interface IHybridHashJoin {
    void initBuild() throws HyracksDataException;

    void build(ByteBuffer buffer) throws HyracksDataException;

    void closeBuild() throws HyracksDataException;

    void clearBuildTempFiles() throws HyracksDataException;

    void clearProbeTempFiles() throws HyracksDataException;

    void fail() throws HyracksDataException;

    void initProbe(ITuplePairComparator comparator) throws HyracksDataException;

    void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException;

    void completeProbe(IFrameWriter writer) throws HyracksDataException;

    void releaseResource() throws HyracksDataException;

    RunFileReader getBuildRFReader(int pid) throws HyracksDataException;

    int getBuildPartitionSizeInTup(int pid);

    RunFileReader getProbeRFReader(int pid) throws HyracksDataException;

    int getProbePartitionSizeInTup(int pid);

    int getMaxBuildPartitionSize();

    int getMaxProbePartitionSize();

    BitSet getPartitionStatus();

    int getPartitionSize(int pid);

    void setIsReversed(boolean reversed);

    void setOperatorStats(IOperatorStats stats);
    int updateMemoryBudget(int newBudget) throws HyracksDataException;
    int updateMemoryBudgetProbe(int newBudget) throws HyracksDataException;
}
