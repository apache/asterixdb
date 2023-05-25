package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.*;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.*;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * This Class describes a Partition of a Hybrid Hash Join Operator.<br>
 * It works as a unique source of truth for its status.
 */
public class Partition {

    //region [PROPERTIES]
    /**
     * Partition id
     */
    private final int id;

    /**
     * Return Partition's ID
     *
     * @return Partition id
     */
    public int getId() {
        return id;
    }

    /**
     * Number of Tuples resident in memory.
     */
    private int tuplesInMemory = 0;

    /**
     * Return number of Tuples resident in memory
     *
     * @return Number of Tuples in Memory
     */
    public int getTuplesInMemory() {
        return tuplesInMemory;
    }

    /**
     * Number of Tuples Spilled to Disk
     */
    private int tuplesSpilled = 0;

    /**
     * Return number of Tuples spilled to disk
     *
     * @return Number of Tuples spilled to disk
     */
    public int getTuplesSpilled() {
        return tuplesSpilled;
    }

    /**
     * Spilled Status<br>
     * <b>TRUE</b> if Partition is spilled.<br>
     * <b>FALSE</b> if Partition is memory resident.
     */
    private boolean spilled = false;

    /**
     * Return the spilled status of Partition.
     *
     * @return <b>TRUE</b> if Partition is spilled.<br>
     * <b>FALSE</b> if Partition is memory resident.
     */
    public boolean getSpilledStatus() {
        return spilled;
    }

    /**
     * Get total number of Tuples processed in this Partition, considering both memory resident and spilled tuples.
     *
     * @return Total number of processed tuples
     */
    public int getTuplesProcessed() {
        return tuplesInMemory + tuplesSpilled;
    }

    /**
     * Get number of Frames used by Partition.
     *
     * @return Number of Frames
     */
    public int getMemoryUsed() {
        return Math.max(bufferManager.getPhysicalSize(id), context.getInitialFrameSize());
    }

    /**
     * Get the File Reader for the temporary file that store spilled tuples.
     *
     * @return File Reader
     */
    public RunFileReader getRfReader() {
        return rfReader;
    }

    /**
     * Frame accessor, provides access to tuples in a byte buffer. It must be created with a record descriptor.
     */
    private final IFrameTupleAccessor frameTupleAccessor;
    private final TuplePointer tuplePointer = new TuplePointer();
    /**
     * Buffer manager, responsable for allocating frames from a memory pool to partition buffers.<br>
     * It is shared among Build Partitions, Probe Partitions and Hash Table.
     */
    private final IPartitionedTupleBufferManager bufferManager;
    /**
     * File reader, gives writer access to a temporary file to store tuples spilled.
     */
    private RunFileWriter rfWriter;
    /**
     * File reader, gives reader access to a temporary file to store tuples spilled.
     */
    private RunFileReader rfReader;
    /**
     * Buffer used during reload fo partition from Disk, it is not managed by the buffer pool.
     */
    private final IFrame reloadBuffer;
    /**
     * Tuple appender used to store larger tuples.
     */
    private final IFrameTupleAppender tupleAppender;
    /**
     * Joblet context
     */
    private final IHyracksJobletContext context;

    //endregion

    //region [CONSTRUCTORS]
    public Partition(int id, IPartitionedTupleBufferManager bufferManager,
                     IHyracksJobletContext context,
                     IFrameTupleAccessor frameTupleAccessor,
                     IFrameTupleAppender tupleAppender,
                     IFrame reloadBuffer) {
        this.id = id;
        this.bufferManager = bufferManager;
        this.frameTupleAccessor = frameTupleAccessor;
        this.tupleAppender = tupleAppender;
        this.reloadBuffer = reloadBuffer;
        this.context = context;
    }

    //endregion

    //region [METHODS]

    /**
     * Insert Tuple into Partition's Buffer.
     * If insertion fails return <b>FALSE</b>, a failure happens means that the Buffer is Full.
     *
     * @param tupleId id of tuple in Buffer being inserted into Partition
     * @return <b>TRUE</b> if tuple was inserted successfully<br> <b>FALSE</b> If buffer is full.
     * @throws HyracksDataException Exception
     */
    public boolean insertTuple(int tupleId) throws HyracksDataException {
        int framesNeededForTuple = bufferManager.framesNeeded(frameTupleAccessor.getTupleLength(tupleId), 0);
        if (framesNeededForTuple * context.getInitialFrameSize() > bufferManager.getBufferPoolSize()) {
            throw HyracksDataException.create(ErrorCode.INSUFFICIENT_MEMORY);
        } else if (framesNeededForTuple > bufferManager.getConstrain().frameLimit(id)) {
            insertLargeTuple(tupleId);
            return true;
        } else if (bufferManager.insertTuple(id, frameTupleAccessor, tupleId, tuplePointer)) {
            tuplesInMemory++;
            return true;
        }
        return false;
    }

    /**
     * If Tuple is larger than the buffer can fit, automatically spill it to disk and mark the partition as spilled.
     *
     * @param tupleId Large Tuple that should be flushed to disk
     * @throws HyracksDataException Exception
     */
    private void insertLargeTuple(int tupleId) throws HyracksDataException {
        createFileWriterIfNotExist();
        rfWriter.open();
        if (!tupleAppender.append(frameTupleAccessor, tupleId)) {
            throw new HyracksDataException("The given tuple is too big");
        }
        tupleAppender.write(rfWriter, true);
        spilled = true;
        tuplesSpilled++;
    }

    /**
     * Spill Partition to Disk, writing all its tuples into a file on disk.
     *
     * @throws HyracksDataException Exception
     */
    public void spill() throws HyracksDataException {
        try {
            createFileWriterIfNotExist();
            rfWriter.open();
            bufferManager.flushPartition(id, rfWriter);
            bufferManager.clearPartition(id);
            tuplesSpilled += tuplesInMemory;
            tuplesInMemory = 0;
            this.spilled = true;
        } catch (Exception ex) {
            throw new HyracksDataException("Error spilling Partition");
        }

    }

    /**
     * Spill Partition and Close Temporary File.
     *
     * @throws HyracksDataException Exception
     */
    public void close() throws HyracksDataException {
        spill();
        rfWriter.close();
    }

    /**
     * Reload partition from Disk.
     *
     * @return <b>TRUE</b> if Partition was reloaded successfully.<br> <b>FALSE</b> if something goes wrong.
     * @throws HyracksDataException Exception
     */
    public boolean reload() throws HyracksDataException {
        if (!spilled)
            return true;
        createFileReaderIfNotExist();
        try {
            rfReader.open();
            while (rfReader.nextFrame(reloadBuffer)) {
                frameTupleAccessor.reset(reloadBuffer.getBuffer());
                for (int tid = 0; tid < frameTupleAccessor.getTupleCount(); tid++) {
                    if (!bufferManager.insertTuple(id, frameTupleAccessor, tid, tuplePointer)) {
                        // for some reason (e.g. fragmentation) if inserting fails, we need to clear the occupied frames
                        bufferManager.clearPartition(this.id);
                        return false;
                    }
                }
            }
            // Closes and deletes the run file if it is already loaded into memory.
            rfReader.setDeleteAfterClose(true);
        } catch (Exception ex) {
            throw new HyracksDataException(ex.getMessage());
        } finally {
            rfReader.close();
        }
        spilled = false;
        this.tuplesInMemory += this.tuplesSpilled;
        this.tuplesSpilled = 0;
        rfWriter = null;
        return true;
    }

    /**
     * Get Temporary File Size in <b>BYTES</b>
     *
     * @return Number containing the temporary File Size in <b>BYTES</b>
     */
    public long getFileSize() {
        if (rfWriter == null)
            return 0;
        return rfWriter.getFileSize();
    }

    /**
     * Reset all Properties, close temporary Files and Delete them.
     *
     * @throws HyracksDataException Exception
     */
    public void cleanUp() throws HyracksDataException {
        tuplesSpilled = 0;
        tuplesInMemory = 0;
        bufferManager.clearPartition(id);
        if (rfWriter != null) {
            CleanupUtils.fail(rfWriter, null);
        }
        rfReader.close();
        rfReader = null;
    }

    /**
     * Internal method to create a File Writer if it was not created yet.
     * @throws HyracksDataException
     */
    private void createFileWriterIfNotExist() throws HyracksDataException {
        if (rfWriter == null) {
            FileReference file = context.createManagedWorkspaceFile("RelS");
            rfWriter = new RunFileWriter(file, context.getIoManager());
        }
    }

    /**
     * Internal method to create a File Reader if it was not created yet.
     * @throws HyracksDataException
     */
    private void createFileReaderIfNotExist() throws HyracksDataException {
        rfReader = rfReader != null ? rfReader : rfWriter.createDeleteOnCloseReader();
    }
    //endregion
}

