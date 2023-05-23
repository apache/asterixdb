package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.*;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.*;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This Class describes a Partition of a Hybrid Hash Join Operator.
 */
public class Partition{
    //region [PROPERTIES]
    /**
     * Partition Id
     */
    private int id;
    /**
     * Return Partition's ID
     * @return Partition Id
     */
    public int getId(){return tuplesInMemory;}

    /**
     * Number of Tuples resident in memory.
     */
    private int tuplesInMemory = 0;

    /**
     * Return number of Tuples resident in memory
     * @return Number of Tuples in Memory
     */
    public  int getTuplesInMemory(){return tuplesInMemory;}

    /**
     * Number of Tuples Spilled to Disk
     */
    private int tuplesSpilled = 0;
    /**
     * Return number of Tuples spilled to disk
     * @return Number of Tuples spilled to disk
     */
    public  int getTuplesSpilled(){return tuplesSpilled;}

    /**
     * Spilled Status<br>
     * <b>TRUE</b> if Partition is spilled.<br>
     * <b>FALSE</b> if Partition is memory resident.
     */
    private boolean spilled = false;

    /**
     * Return the spilled status of Partition.
     * @return  <b>TRUE</b> if Partition is spilled.<br>
     *          <b>FALSE</b> if Partition is memory resident.
     */
    public boolean getSpilledStatus(){
        return spilled;
    }
    /**
     * Number of Bytes written to disk, used for statistics.
     */
    private int bytesSpilled = 0;
    /**
     * Get total number of Tuples processed in this Partition, considering both memory resident and spilled tuples.
     * @return Total number of processed tuples
     */
    public int getTuplesProcessed(){
        return tuplesInMemory+tuplesSpilled;
    }

    /**
     * Name of relation attatched to this Partition.
     */
    private String relationName;
    private IFrameTupleAccessor frameTupleAccessor;
    private TuplePointer tuplePointer = new TuplePointer();
    private IPartitionedTupleBufferManager bufferManager;
    private IFrameWriter rfWriter;
    private IFrameReader rfReader;
    private IFrame reloadBuffer;

    //endregion

    //region [CONSTRUCTORS]
    public Partition(int id,IPartitionedTupleBufferManager bufferManager,
                     IFrameWriter rfWriter,
                     String relationName,
                     IFrameTupleAccessor frameTupleAccessor){
        this.id = id;
        this.relationName = relationName;
        this.rfWriter = rfWriter;
        this.bufferManager = bufferManager;
        this.frameTupleAccessor = frameTupleAccessor;
    }
    //endregion

    //region [METHODS]
    /**
     * Insert Tuple into Partition's Buffer.
     * If insertion fails return <b>FALSE</b>, a failure happens means that the Buffer is Full.
     * @param tupleId
     * @return <b>TRUE</b> if tuple was inserted successfully<br> <b>FALSE</b> If buffer is full.
     * @throws HyracksDataException
     */
    public boolean insertTuple(int tupleId) throws HyracksDataException {
        if(bufferManager.insertTuple(id, frameTupleAccessor, tupleId, tuplePointer)){
            tuplesInMemory++;
            return true;
        }
        return false;
    }

    /**
     * Spill Partition to Disk, writing all its tuples into a file on disk.
     * @throws HyracksDataException
     */
    public void spill() throws HyracksDataException{
        bytesSpilled += bufferManager.flushPartition(id, rfWriter);
        bufferManager.clearPartition(id);
        tuplesSpilled = tuplesInMemory;
        tuplesInMemory =0;
        this.spilled = true;
    }

    /**
     * Reload partition from Disk.
     * @param rfReader Frame Reader
     * @param frame Auxiliary Frame Buffer
     * @return <b>TRUE</b> if Partition was reloaded successfully.<br> <b>FALSE</b> if something goes wrong.
     * @throws HyracksDataException
     */
    public boolean reload(IFrameReader rfReader,IFrame frame) throws HyracksDataException{
        this.rfReader = rfReader;
        reloadBuffer = frame;
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
        } finally {
            rfReader.close();
        }
        spilled = false;
        this.tuplesSpilled = 0;
        this.tuplesInMemory += frameTupleAccessor.getTupleCount();
        rfWriter= null;
        return true;
    }
    //endregion
}

//region [TEMPORARY FILES MANAGEMENT]
class TestPartition{

    @Test
    void TestInsertTupleSuccess(){
        Partition partition = new Partition(0,new bufferManagerFake(),new frameWriterFake(),"Build",new frameTupleAcessorFake());
        assertEquals(partition.getId(),0);
        try {
            partition.insertTuple(1);
            assertEquals(partition.getTuplesProcessed(),1);
            assertEquals(partition.getTuplesInMemory(),1);
        }
        catch (Exception ex){
            fail();
        }

    }
    @Test
    void TestInsertTupleFail(){
        Partition partition = new Partition(0,new bufferManagerFake2(),new frameWriterFake(),"Build",new frameTupleAcessorFake());
        try {
            partition.insertTuple(1);
            assertEquals(partition.getTuplesInMemory(),0);
        }
        catch (Exception ex){
            fail();
        }

    }

    @Test
    void TestSpill(){
        Partition partition = new Partition(0,new bufferManagerFake(),new frameWriterFake(),"Build",new frameTupleAcessorFake());
        try {
            partition.insertTuple(1);
            partition.insertTuple(2);
            assertEquals(partition.getTuplesInMemory(),2);
            partition.spill();
            assertEquals(partition.getSpilledStatus(),true);
            assertEquals(partition.getTuplesSpilled(),2);
            assertEquals(partition.getTuplesInMemory(),0);
            partition.insertTuple(3);
            assertEquals(partition.getTuplesInMemory(),1);
        }
        catch (Exception ex){
            fail();
        }
    }
    @Test
    void TestReload(){
        Partition partition = new Partition(0,new bufferManagerFake(),new frameWriterFake(),"Build",new frameTupleAcessorFake());
        try {
            partition.insertTuple(1);
            partition.insertTuple(2);
            partition.spill();
            partition.insertTuple(3);
            assertEquals(partition.getSpilledStatus(),true);
            partition.reload(new frameReaderFake(),new frameFake());
            assertEquals(partition.getSpilledStatus(),false);
            assertEquals(partition.getTuplesInMemory(),3);
            assertEquals(partition.getTuplesSpilled(),0);
        }
        catch (Exception ex){
            fail();
        }
    }
    @Test
    void TestReloadFail(){
        Partition partition = new Partition(0,new bufferManagerFake2(),new frameWriterFake(),"Build",new frameTupleAcessorFake());
        try {
            partition.insertTuple(1);
            partition.insertTuple(2);
            partition.spill();
            assertEquals(partition.getSpilledStatus(),true);
            partition.reload(new frameReaderFake(),new frameFake());
            assertEquals(partition.getSpilledStatus(),true);
            assertEquals(partition.getTuplesInMemory(),0);
            assertEquals(partition.getTuplesSpilled(),0);
        }
        catch (Exception ex){
            fail();
        }
    }
}
class frameWriterFake implements IFrameWriter{
    @Override
    public void open() throws HyracksDataException {

    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {

    }

    @Override
    public void fail() throws HyracksDataException {

    }

    @Override
    public void close() throws HyracksDataException {

    }
}
class frameReaderFake implements IFrameReader{

    private int numberOfFrames =2;
    private int iterated = 0;
    @Override
    public void open() throws HyracksDataException {
        iterated =0;
    }

    @Override
    public boolean nextFrame(IFrame frame) throws HyracksDataException {
        iterated++;
        return iterated <= numberOfFrames;
    }

    @Override
    public void close() throws HyracksDataException {
        iterated =0;
    }

    @Override
    public void setDeleteAfterClose(boolean set) {

    }
}
class bufferManagerFake implements IPartitionedTupleBufferManager {

    public bufferManagerFake(){

    }
    @Override
    public int getNumPartitions() {
        return 0;
    }

    @Override
    public int getNumTuples(int partition) {
        return 0;
    }

    @Override
    public int getPhysicalSize(int partition) {
        return 0;
    }

    @Override
    public boolean insertTuple(int partition, byte[] byteArray, int[] fieldEndOffsets, int start, int size, TuplePointer pointer) throws HyracksDataException {
        return true;
    }

    @Override
    public boolean insertTuple(int partition, IFrameTupleAccessor tupleAccessor, int tupleId, TuplePointer pointer) throws HyracksDataException {
        return true;
    }

    @Override
    public int framesNeeded(int tupleSize, int fieldCount) {
        return 0;
    }

    @Override
    public void cancelInsertTuple(int partition) throws HyracksDataException {

    }

    @Override
    public void setConstrain(IPartitionedMemoryConstrain constrain) {

    }

    @Override
    public void reset() throws HyracksDataException {

    }

    @Override
    public void close() {

    }

    @Override
    public ITuplePointerAccessor getTuplePointerAccessor(RecordDescriptor recordDescriptor) {
        return null;
    }

    @Override
    public int flushPartition(int pid, IFrameWriter writer) throws HyracksDataException {
        return 0;
    }

    @Override
    public void clearPartition(int partition) throws HyracksDataException {

    }

    @Override
    public IPartitionedMemoryConstrain getConstrain() {
        return null;
    }

    @Override
    public boolean updateMemoryBudget(int desiredSize) {
        return false;
    }
}
class bufferManagerFake2 extends bufferManagerFake{
    @Override
    public boolean insertTuple(int partition, byte[] byteArray, int[] fieldEndOffsets, int start, int size, TuplePointer pointer) throws HyracksDataException {
        return false;
    }

    @Override
    public boolean insertTuple(int partition, IFrameTupleAccessor tupleAccessor, int tupleId, TuplePointer pointer) throws HyracksDataException {
        return false;
    }
}
class frameTupleAcessorFake implements IFrameTupleAccessor{

    @Override
    public int getFieldCount() {
        return 0;
    }

    @Override
    public int getFieldSlotsLength() {
        return 0;
    }

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return 0;
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return 0;
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return 0;
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        return 0;
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return 0;
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        return 0;
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return 0;
    }

    @Override
    public int getTupleCount() {
        return 2;
    }

    @Override
    public ByteBuffer getBuffer() {
        return null;
    }

    @Override
    public void reset(ByteBuffer buffer) {

    }
}
class frameFake implements IFrame{

    @Override
    public ByteBuffer getBuffer() {
        return null;
    }

    @Override
    public void ensureFrameSize(int frameSize) throws HyracksDataException {

    }

    @Override
    public void resize(int frameSize) throws HyracksDataException {

    }

    @Override
    public int getFrameSize() {
        return 0;
    }

    @Override
    public int getMinSize() {
        return 0;
    }

    @Override
    public void reset() throws HyracksDataException {

    }
}
//endregion
