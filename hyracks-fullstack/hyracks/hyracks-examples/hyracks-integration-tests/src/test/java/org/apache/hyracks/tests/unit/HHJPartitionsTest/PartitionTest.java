package org.apache.hyracks.tests.unit.HHJPartitionsTest;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.buffermanager.*;
import org.apache.hyracks.dataflow.std.join.Partition;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Documented;
import java.util.BitSet;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

//region [TEMPORARY FILES MANAGEMENT]
class PartitionTest{
    int numberOfPartitions = 5;
    int frameSize = 32768;
    int totalNumberOfFrames = 10;
    IHyracksJobletContext context = TestUtils.create(frameSize).getJobletContext();
    RecordDescriptor recordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
    Partition partition;
    BitSet status;
    IFrameTupleAccessor tupleAccessor;
    private final Random rnd = new Random(50);
    public PartitionTest() throws HyracksDataException{
        String relationName= "RelS";
        VSizeFrame reloadBuffer = new VSizeFrame(context);
        IDeallocatableFramePool framePool = new DeallocatableFramePool(context, totalNumberOfFrames * frameSize,false);
        IPartitionedTupleBufferManager bufferManager = new VPartitionTupleBufferManager(null,numberOfPartitions, framePool);
        status = new BitSet(numberOfPartitions);
        bufferManager.setConstrain(PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(status));
        tupleAccessor = new FrameTupleAccessor(recordDescriptor);
        IFrameTupleAppender tupleAppender = new FrameTupleAppender(new VSizeFrame(context));
        tupleAccessor.reset(generateIntFrame().getBuffer());
        partition = new Partition(0,bufferManager,context,tupleAccessor,tupleAppender,reloadBuffer);
    }

    /**
     * Insert a single tuple into Partition and check the number of tuples in memory, spilled and processed.
     * @throws HyracksDataException
     */
    @Test
    void TestInsertTupleSuccess() throws HyracksDataException{
        partition.insertTuple(1);
        assertEquals(partition.getId(),0);
        assertEquals(partition.getTuplesSpilled(),0);
        assertEquals(partition.getTuplesInMemory(),1);
        assertEquals(partition.getTuplesProcessed(),1);
    }
    /**
     * Spill Partition and check the number of tuples in memory, spilled and processed.
     * @throws HyracksDataException
     */
    @Test
    void Spill() throws HyracksDataException{
        TestInsertTupleSuccess();
        partition.spill();
        assertEquals(partition.getTuplesSpilled(),1);
        assertEquals(partition.getTuplesInMemory(),0);
        assertEquals(partition.getTuplesProcessed(),1);
        assertEquals(partition.getFileSize(),frameSize);
        assertEquals(partition.getMemoryUsed(),frameSize);
    }
    /**
     * Reload the spilled Partition and check the numbers
     * @throws HyracksDataException
     */
    @Test
    void SpillAndReload() throws HyracksDataException{
        Spill();
        partition.reload();
        assertEquals(partition.getTuplesSpilled(),0);
        assertEquals(partition.getTuplesInMemory(),1);
        assertEquals(partition.getTuplesProcessed(),1);
        assertEquals(partition.getFileSize(),0);
    }
    /**
     * Spill the Partition again the spilled Partition and check the numbers
     * @throws HyracksDataException
     */
    @Test
    void ReloadAndSpillAgain() throws HyracksDataException{
        SpillAndReload();
        partition.spill();
        assertEquals(partition.getTuplesSpilled(),1);
        assertEquals(partition.getTuplesInMemory(),0);
        assertEquals(partition.getTuplesProcessed(),1);
        assertEquals(partition.getFileSize(),frameSize);
    }
    @Test
    void ClosePartition() throws HyracksDataException{
        SpillAndReload();
        partition.close();
        assertEquals(partition.getTuplesInMemory(),0);
        assertNotEquals(partition.getRfReader(),null);
    }

    @Test
    void CleanUpPartition() throws HyracksDataException{
        SpillAndReload();
        partition.cleanUp();
        assertEquals(partition.getTuplesInMemory(),0);
        assertEquals(partition.getRfReader(),null);
    }

    /**
     * Insert one frame and compare if the number of tuples inserted match.
     * @throws HyracksDataException
     */
    @Test
    void InsertSingleFrame() throws  HyracksDataException{
        assertEquals(InsertFrame(),partition.getTuplesProcessed());
    }
    @Test
    /**
     * Insert 10 frames to partition, there should be spills since the Buffer Size is 10 Frames.
     */
    void Insert20Frames() throws  HyracksDataException{
        int numberOfTuplesProcessed = 0;
        //It will only insert 10 frames due to the Frame Pool Size.
        for(int i=0;i<20;i++){
            tupleAccessor.reset(generateIntFrame().getBuffer());
            numberOfTuplesProcessed += InsertFrame();
        }
        assertEquals(numberOfTuplesProcessed,partition.getTuplesProcessed());
        assertEquals(partition.getMemoryUsed(),10 * frameSize);
        partition.spill();
        assertEquals(partition.getFileSize(),10*frameSize);
        assertEquals(partition.getMemoryUsed(),frameSize); //Memory Used is at least one frame large.
    }

    /**
     * @Title Insert a Large Object into Partition.<br>
     * @Summary Altough this Object can fit in memory (Object < Memory Budget) ,
     * we insert partitions to make sure this object can't fit in the available memory,
     * this will force the object to be spilled.
     * @throws HyracksDataException
     */
    @Test
    void InsertLargeObject() throws  HyracksDataException{
        int numberOfTuplesProcessed = InsertFrame(); //Insert Frame To Occupy memory.
        //This Frame is Large Enough not to fit in Buffer but smaller than the memory Budget.
        IFrame frame = generateStringFrame(9 * context.getInitialFrameSize());
        tupleAccessor.reset(frame.getBuffer());
        status.set(0);
        int numberOfOversizedObjects = InsertFrame();
        assertEquals(true,partition.getSpilledStatus());
        assertEquals(numberOfOversizedObjects,partition.getTuplesSpilled());
        assertEquals(numberOfTuplesProcessed+numberOfOversizedObjects,partition.getTuplesProcessed());
    }
    @Test
    void FailToInsertTupleLargerThanBudget() throws  HyracksDataException{
        //This Frame is Larger than Memory Budget.
        int frameSize = 10*context.getInitialFrameSize();
        tupleAccessor.reset(generateStringFrame(frameSize).getBuffer());
        try{
            InsertFrame();
            fail();
        }
        catch (Exception ex){
            return;
        }
    }


    /**
     * Insert tuples from Tuple Accessor into Partition,
     * If fails spill the partition.
     * @return
     * @throws HyracksDataException
     */
    int InsertFrame() throws HyracksDataException{
        int numberOfTuplesInFrame = tupleAccessor.getTupleCount();
        for(int i=0;i<numberOfTuplesInFrame;i++){
            while(!partition.insertTuple(i)){
                partition.spill();
            }
        }
        return  numberOfTuplesInFrame;
    }
    private IFrame generateIntFrame() throws HyracksDataException {
        VSizeFrame buffer = new VSizeFrame(context);
        int fieldCount = 1;
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(buffer, true);
        while (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            TupleUtils.createIntegerTuple(tb, tuple, rnd.nextInt());
            tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        }
        return buffer;
    }
    private IFrame generateStringFrame(int length) throws HyracksDataException {
        int fieldCount = 1;
        VSizeFrame frame = new VSizeFrame(context, context.getInitialFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(frame, true);
        ISerializerDeserializer[] fieldSerdes = { new UTF8StringSerializerDeserializer() };
        String data = "";
        for (int i = 0; i < length; i++) {
            data += "X";
        }
        TupleUtils.createTuple(tb, tuple, fieldSerdes, data);
        tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        return frame;
    }
}
