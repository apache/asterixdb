package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.Joblet;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;

import java.util.*;
import java.util.stream.Collectors;

public class PartitionManager {
    public final List<Partition> partitions = new ArrayList<>();
    IFrame reloadBuffer;
    public BitSet spilledStatus;
    private IHyracksJobletContext context;

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
                            IFrameTupleAppender frameTupleAppender,
                            BitSet spilledStatus,
                            String relationName) throws HyracksDataException {
        this.context = context;
        this.bufferManager = bufferManager;
        this.tupleAccessor = frameTupleAccessor;
        this.partitionComputer = partitionComputer;
        reloadBuffer = new VSizeFrame(context);
        this.spilledStatus = spilledStatus;
        for (int i = 0; i < numberOfPartitions; i++) {
            partitions.add(new Partition(i, bufferManager, context, frameTupleAccessor, frameTupleAppender, reloadBuffer, relationName));
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

    public int getTuplesProcessed() {
        return partitions.stream().mapToInt(Partition::getTuplesProcessed).sum();
    }

    public int getTuplesSpilled() {
        return partitions.stream().mapToInt(Partition::getTuplesSpilled).sum();
    }

    public int getBytesSpilled() {
        return partitions.stream().mapToInt(Partition::getBytesSpilled).sum();
    }

    public int getBytesReloaded() {
        return partitions.stream().mapToInt(Partition::getBytesReloaded).sum();
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
        for(int i =0;i<partitions.size();i++){
            Partition p = partitions.get(i);
            if(p.getId() == id){
                return p;
            }
        }
        return null;
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
     * Get total memory used in <b>BYTES</b> by all partitions.
     *
     * @return
     */
    public double getTotalMemory() {
        return partitions.stream().mapToLong(Partition::getMemoryUsed).sum();
    }

    /**
     * Insert tuple into a Partition, the partition that will hold this tuple is obtained using {@code partitionComputer}.
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
     * <p>This method insert a tuple into a partition managed by this class, selecting the partition based on the {@code partitionComputer}.</p>
     * <p>If this partition's buffer is full, and there are no buffers available in the {@code bufferManager}'s Pool than a partition will be selected to be spilled to disk.</p>
     * <p>This partition will be seleceted based on the {@code spillPolicy}</p>
     *
     * @param tupleId     tuple id to be inserted.
     * @param spillPolicy spill policy that will determine witch partition will be spilled in the case that are no buffers available.
     * @throws HyracksDataException Exception
     */
    public void insertTupleWithSpillPolicy(int tupleId, PreferToSpillFullyOccupiedFramePolicy spillPolicy) throws HyracksDataException {
        int partitionId = partitionComputer.partition(tupleAccessor, tupleId, getNumberOfPartitions());
        while (!partitions.get(partitionId).insertTuple(tupleId)) {
            spillPartition(spillPolicy.selectVictimPartition(partitionId));
        }
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
    public List<Partition> getSpilledPartitions() {
        return partitions.stream().filter(Partition::getSpilledStatus).collect(Collectors.toList());
    }

    public List<Partition> getSpilledOrInconsistentPartitions() {
        return partitions.stream().filter(p -> p.getReloadedStatus() || p.getSpilledStatus()).collect(Collectors.toList());
    }

    /**
     * Internal method that filter the spilled partitions.
     *
     * @return List of Partitions
     */
    public List<Partition> getMemoryResidentPartitions() {
        return partitions.stream().filter(p -> !p.getSpilledStatus()).collect(Collectors.toList());
    }

    /**
     * Spill a partition to Disk
     *
     * @param id id of partition to be spilled
     * @return Number of Frames Released
     * @throws HyracksDataException Exception
     */
    public int spillPartition(int id) throws HyracksDataException {
        Partition p = getPartition(id);
        int oldMemoryUsage = p.getMemoryUsed();
        partitions.get(id).spill();
        spilledStatus.set(id);
        return (oldMemoryUsage - p.getMemoryUsed()) / context.getInitialFrameSize();
    }

    /**
     * Spill all partition to Disk
     *
     * @return Number of Frames Released
     * @throws HyracksDataException Exception
     */
    public int spillAll() throws HyracksDataException {
        int framesReleased = 0;
        for(Partition p : partitions){
            framesReleased += spillPartition(p.getId());
        }
        return framesReleased;
    }

    //region [FIND PARTITION]

    /**
     * Internal method to filter based on Buffer usage.
     *
     * @param partitions List of Partitions to be compared
     * @param reversed   <b>TRUE</b> to use desc or <b>FALSE</b> to use asc
     * @return
     */
    private Partition bufferFilter(List<Partition> partitions, boolean reversed) {
        Comparator<Partition> bufferComparator = Comparator.comparing(Partition::getMemoryUsed);
        Comparator<Partition> tuplesInMemoryComparator = Comparator.comparing(Partition::getTuplesInMemory);
        bufferComparator = bufferComparator.thenComparing((tuplesInMemoryComparator));
        if (reversed)
            bufferComparator = bufferComparator.reversed();
        bufferComparator = bufferComparator.thenComparing(Partition::getId);
        partitions.sort(bufferComparator);
        return partitions.size() > 0 ? partitions.get(0) : null;
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
        Partition p = bufferFilter(spilled, true);
        if (p == null)
            return -1;
        return bufferFilter(spilled, true).getId();
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
        return bufferFilter(spilled, false).getId();
    }

    public List<Partition> getSpillCandidatePartitions(){
        List<Partition> candidates =  partitions.stream().filter(p -> p.getFramesUsed() > 1).collect(Collectors.toList());
        PartitionComparatorBuilder comparator = new PartitionComparatorBuilder();
        comparator.addStatusComparator(true);
        comparator.addBufferSizeComparator(true);
        comparator.addInMemoryTupleComparator(true);
        candidates.sort(comparator.build());
        return candidates;
    }

    /**
     * Spill Partitions until release {@code numberOfFrames}
     * <p>This method stops spilling partitions if the number of frames is reached. Otherwise keeps spilling candidates.</p>
     * @param numberOfFrames Frames to release
     * @return Number of frames Released
     * @throws HyracksDataException
     */
    public int spillToReleaseFrames(int numberOfFrames) throws HyracksDataException{
        int framesReleased = 0;
        List<Partition> candidates = getSpillCandidatePartitions();
        for(Partition p:candidates){
            framesReleased += spillPartition(p.getId());
            if(framesReleased >= numberOfFrames){
                break;
            }
        }
        return framesReleased;
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

    //endregion

    /**
     * Close all spilled partitions, spilling all tuples that are spill in memory.s
     *
     * @throws HyracksDataException Exception
     */
    public void closeSpilledPartitions() throws HyracksDataException {
        List<Partition> spilledPartitions = getSpilledPartitions();
        for (Partition p : spilledPartitions) {
            if (p.getTuplesProcessed() > 0)
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
    public boolean reloadPartition(int id,boolean deleteAfterReload) throws HyracksDataException {
        if (partitions.get(id).reload(deleteAfterReload)) {
            spilledStatus.clear(id);
            return true;
        }
        return false;
    }

    /**
     * Check if all partitions are memory resident.
     *
     * @return <b>TRUE</b> if no partitions were spilled and <b>FALSE</b> if one or more are spilled.
     */
    public boolean areAllPartitionsMemoryResident() {
        return getSpilledStatus().cardinality() == 0;
    }

    /**
     * Return a Bit Set containing the spilled status.<br>
     * This bit set is generated by the Partition's status, only updatable by the Partition itself. <br>
     * This is a single source of truth.
     *
     * @return
     */
    public BitSet getSpilledStatus() {
        for(Partition p : partitions){
            spilledStatus.set(p.getId(),p.getSpilledStatus());
        }
        return spilledStatus;
    }

    /**
     * Return a Bit Set containing the spilled status.<br>
     * This bit set is generated by the Partition's status, only updatable by the Partition itself. <br>
     * This is a single source of truth.
     *
     * @return
     */
    public BitSet getInconsistentStatus() {
        BitSet inconsistent = new BitSet(getNumberOfPartitions());
        for(Partition p : partitions){
            inconsistent.set(p.getId(),p.getSpilledStatus() || p.getReloadedStatus());
        }
        return inconsistent;
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

