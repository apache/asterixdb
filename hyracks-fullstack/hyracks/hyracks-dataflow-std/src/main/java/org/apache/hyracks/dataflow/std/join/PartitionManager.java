package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;

import java.util.*;
import java.util.stream.Collectors;

public class PartitionManager {
    private final List<Partition> partitions = new ArrayList<>();
    IFrame reloadBuffer;

    /**
     * Buffer manager, responsable for allocating frames from a memory pool to partition buffers.<br>
     * It is shared among Build Partitions, Probe Partitions and Hash Table.
     */
    private static IPartitionedTupleBufferManager bufferManager;
    /**
     * Partition Couputer, often called the Split Function.<br>
     * This class hashes a tuple to a partition based on the number of partitions being used.
     */
    private final ITuplePartitionComputer partitionComputer;
    /**
     * Frame accessor, provides access to tuples in a byte buffer. It must be created with a record descriptor.
     */
    private final IFrameTupleAccessor tupleAccessor;

    public PartitionManager(int numberOfPartitions,
                            IHyracksJobletContext context,
                            IPartitionedTupleBufferManager bufferManager,
                            ITuplePartitionComputer partitionComputer,
                            IFrameTupleAccessor frameTupleAccessor,
                            IFrameTupleAppender frameTupleAppender) throws HyracksDataException {
        this.bufferManager = bufferManager;
        this.tupleAccessor = frameTupleAccessor;
        this.partitionComputer = partitionComputer;
        reloadBuffer = new VSizeFrame(context);
        for (int i = 0; i < numberOfPartitions; i++) {
            partitions.add(new Partition(i, bufferManager, context, frameTupleAccessor, frameTupleAppender, reloadBuffer));
        }
        if (getTotalMemory() > bufferManager.getBufferPoolSize()) {
            throw new HyracksDataException("Number of Partitions can't be used. The memory budget in frames is smaller than the number of partitions");
        }
    }


    /**
     * Get the total number of tuples in memory at the time.
     *
     * @return Number of tuples
     */
    public int getTuplesInMemory() {
        return partitions.stream().mapToInt(Partition::getTuplesInMemory).sum();
    }

    public int getTuplesInMemoryFromResidentPartitions() {
        List<Partition> residents = getMemoryResidentPartitions();
        if (residents != null) {
            return residents.stream().mapToInt(Partition::getTuplesInMemory).sum();
        }
        return 0;
    }

    //todo: Remove this method, its unsafe Or create a Interface for Partition to hide methods that should only be accessed by PartitionManager
    public Partition getPartition(int id) {
        return partitions.get(id);
    }

    /**
     * Get number of tuples in memory from a specific partition
     *
     * @param partitionId
     * @return
     */
    public int getTuplesInMemory(int partitionId) {
        return partitions.get(partitionId).getTuplesInMemory();
    }

    /**
     * Get total memory used in frames by all partitions
     *
     * @return
     */
    public double getTotalMemory() {
        return partitions.stream().mapToLong(Partition::getMemoryUsed).sum();
    }

    /**
     * Insert tuple into a Partition hashed using {@code partitionComputer}.
     *
     * @param tupleId Tuple id
     * @return <b>TRUE</b>True if was successfully inserted or <b>FALSE</b> if there is no more frames available in the Memory Pool.
     * @throws HyracksDataException Exception
     */
    public boolean insertTuple(int tupleId) throws HyracksDataException {
        int partitionId = partitionComputer.partition(tupleAccessor, tupleId, getNumberOfPartitions());
        return partitions.get(partitionId).insertTuple(tupleId);
    }

    /**
     * Get number of partitions in the partition manager.
     *
     * @return Number of Partitions
     */
    public int getNumberOfPartitions() {
        return partitions.size();
    }

    /**
     * Internal method that filter the spilled partitions.
     *
     * @return List of Partitions
     */
    private List<Partition> getSpilledPartitions() {
        return partitions.stream().filter(Partition::getSpilledStatus).collect(Collectors.toList());
    }

    /**
     * Internal method that filter the spilled partitions.
     *
     * @return List of Partitions
     */
    private List<Partition> getMemoryResidentPartitions() {
        return partitions.stream().filter(p -> !p.getSpilledStatus()).collect(Collectors.toList());
    }

    /**
     * Spill a partition to Disk
     *
     * @param id id of partition to be spilled
     * @throws HyracksDataException Exception
     */
    public void spillPartition(int id) throws HyracksDataException {
        partitions.get(id).spill();
    }

    //region [FIND PARTITION]

    /**
     * Internal method to filter based on Buffer usage.
     *
     * @param partitions List of Partitions to be compared
     * @param reversed   <b>TRUE</b> to use desc or <b>FALSE</b> to use asc
     * @return
     */
    private int bufferFilter(List<Partition> partitions, boolean reversed) {
        Comparator<Partition> bufferComparator = Comparator.comparing(Partition::getMemoryUsed);
        Comparator<Partition> tuplesInMemoryComparator = Comparator.comparing(Partition::getTuplesInMemory);
        bufferComparator = bufferComparator.thenComparing((tuplesInMemoryComparator));
        if (reversed)
            bufferComparator = bufferComparator.reversed();
        bufferComparator = bufferComparator.thenComparing(Partition::getId);
        partitions.sort(bufferComparator);
        return partitions.size() > 0 ? partitions.get(0).getId() : -1;
    }

    /**
     * Get id of Spilled Partition with Larger Buffer:
     * <ul>
     *     <li>If two or mor partitions have the same buffer size (frames) than return the one with more tuples in memory</li>
     *     <li>If there is a tie return the lowest id</li>
     * </ul>
     *
     * @return Partition id
     */
    public int getSpilledWithLargerBuffer() {
        List<Partition> spilled = getSpilledPartitions();
        return bufferFilter(spilled, true);
    }

    /**
     * Get id of Spilled Partition with Smaller Buffer:
     * <ul>
     *     <li>If two or mor partitions have the same buffer size (frames) than return the one with less tuples in memory</li>
     *     <li>If there is a tie return the lowest id</li>
     * </ul>
     *
     * @return Partition id
     */
    public int getSpilledWithSmallerBuffer() {
        List<Partition> spilled = getSpilledPartitions();
        return bufferFilter(spilled, false);
    }

    /**
     * Get id of Resident Partition with Larger Buffer:
     * <ul>
     *     <li>If two or mor partitions have the same buffer size (frames) than return the one with more tuples in memory</li>
     *     <li>If there is a tie return the lowest id</li>
     * </ul>
     *
     * @return Partition id
     */
    public int getResidentWithLargerBuffer() {
        List<Partition> resident = getMemoryResidentPartitions();
        return bufferFilter(resident, true);
    }

    /**
     * Get id of Spilled Partition with Smaller Buffer:
     * <ul>
     *     <li>If two or mor partitions have the same buffer size (frames) than return the one with less tuples in memory</li>
     *     <li>If there is a tie return the lowest id</li>
     * </ul>
     *
     * @return Partition id
     */
    public int getResidentWithSmallerBuffer() {
        List<Partition> resident = getMemoryResidentPartitions();
        return bufferFilter(resident, false);
    }

    /**
     * Get id of Spilled Partition with Larger Buffer:
     * <ul>
     *     <li>If two or mor partitions have the same buffer size (frames) than return the one with more tuples in memory</li>
     *     <li>If there is a tie return the lowest id</li>
     * </ul>
     *
     * @return Partition id
     */
    public int getNumberOfTuplesOfLargestPartition() {
        Comparator<Partition> comparator = Comparator.comparing(Partition::getTuplesProcessed).reversed();
        return Collections.max(partitions, comparator).getTuplesProcessed();
    }

    /**
     * Get id of the spilled with most bytes spilled that can fit its spilled tuples in {@code numberOfFrames}.
     *
     * @param numberOfFrames Number of Frames
     * @return Partition's id
     */
    public int getBestSpilledPartitionThatFit(int numberOfFrames) {
        // TODO: 5/24/23
        return 0;
    }

    /**
     * Get id of the largest partition that if spilled can free {@code numberOfFrames} .
     *
     * @param numberOfFrames Number of Frames
     * @return Partition's id
     */
    public int getBestPartitionThatUse(int numberOfFrames) {
        // TODO: 5/24/23
        return 0;
    }


    //endregion

    /**
     * Close all spilled partitions, spilling all tuples that are spill in memory.s
     *
     * @throws HyracksDataException Exception
     */
    public void closeSpilledPartitions() throws HyracksDataException {
        List<Partition> spilledPartitions = getSpilledPartitions();
        for (Partition p : spilledPartitions) {
            p.close();
        }
    }

    /**
     * Reload partition from disk into memory
     *
     * @param id id of Partition to be reloaded.
     * @return <b>TRUE</b> if reload was successfull.
     * @throws HyracksDataException Exception
     */
    public boolean reloadPartition(int id) throws HyracksDataException {
        return partitions.get(id).reload();
    }

    /**
     * Check if all partitions are memory resident.
     *
     * @return <b>TRUE</b> if no partitions were spilled and <b>FALSE</b> if one or more are spilled.
     */
    public boolean areAllPartitionsMemoryResident() {
        return getSpilledStatus().stream().sum() == 0;
    }

    /**
     * Return a Bit Set containing the spilled status.<br>
     * This bit set is generated by the Partition's status, only updatable by the Partition itself. <br>
     * This is a single source of truth.
     *
     * @return
     */
    public BitSet getSpilledStatus() {
        BitSet bitSet = new BitSet(getNumberOfPartitions());
        for (Partition p : getSpilledPartitions()) {
            bitSet.set(p.getId());
        }
        return bitSet;
    }

    /**
     * Close all Partitions and their file accessors.
     *
     * @throws HyracksDataException Exception
     */
    public void cleanUp() throws HyracksDataException {
        for (Partition partition : partitions) {
            partition.cleanUp();
        }
    }
}
