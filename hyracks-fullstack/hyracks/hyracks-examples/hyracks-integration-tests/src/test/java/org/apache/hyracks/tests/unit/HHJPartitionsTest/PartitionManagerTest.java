package org.apache.hyracks.tests.unit.HHJPartitionsTest;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.buffermanager.*;
import org.apache.hyracks.dataflow.std.join.PartitionManager;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class PartitionManagerTest{
    int numberOfPartitions = 5;
    int frameSize = 32768;
    int totalNumberOfFrames = 10;
    IHyracksJobletContext context = TestUtils.create(frameSize).getJobletContext();
    RecordDescriptor recordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
    PartitionManager partitionManager;
    BitSet status;
    IFrameTupleAccessor tupleAccessor;
    private final Random rnd = new Random(50);
    public PartitionManagerTest() throws HyracksDataException{
        IDeallocatableFramePool framePool = new DeallocatableFramePool(context, totalNumberOfFrames * frameSize,false);
        IPartitionedTupleBufferManager bufferManager = new VPartitionTupleBufferManager(null,numberOfPartitions, framePool);
        status = new BitSet(numberOfPartitions);
        bufferManager.setConstrain(PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(status));
        tupleAccessor = new FrameTupleAccessor(recordDescriptor);
        tupleAccessor.reset(generateIntFrame().getBuffer());
        IFrameTupleAppender tupleAppender = new FrameTupleAppender(new VSizeFrame(context));
        ITuplePartitionComputer partitionComputer =new simplePartitionComputer();
        partitionManager = new PartitionManager(numberOfPartitions,context,bufferManager,partitionComputer,tupleAccessor,tupleAppender,status);
    }
    @Test
    public void InvalidBufferSizeAndPartitionNumber() throws HyracksDataException{
        int largeNumberOfPartitions = 10;
        int smallFramePoolSize = 5;
        IDeallocatableFramePool framePool = new DeallocatableFramePool(context, smallFramePoolSize * frameSize,false);
        IPartitionedTupleBufferManager bufferManager = new VPartitionTupleBufferManager(null,largeNumberOfPartitions, framePool);
        status = new BitSet(largeNumberOfPartitions);
        bufferManager.setConstrain(PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(status));
        tupleAccessor = new FrameTupleAccessor(recordDescriptor);
        tupleAccessor.reset(generateIntFrame().getBuffer());
        IFrameTupleAppender tupleAppender = new FrameTupleAppender(new VSizeFrame(context));
        ITuplePartitionComputer partitionComputer =new simplePartitionComputer();
        try {
            partitionManager = new PartitionManager(largeNumberOfPartitions, context, bufferManager, partitionComputer, tupleAccessor, tupleAppender,status);
            fail();
        }
        catch (Exception ex){
            return;
        }
    }
    @Test
    public void ConstructorTest(){
        assertEquals(partitionManager.getTuplesInMemory(),0);
        assertEquals(partitionManager.getNumberOfPartitions(),5);
        assertEquals(partitionManager.getTotalMemory(),numberOfPartitions * frameSize);
    }
    @Test
    public void InsertPartitionTest() throws HyracksDataException{
        partitionManager.insertTuple(0);
        assertEquals(partitionManager.getTuplesInMemory(),1);
        partitionManager.insertTuple(1);
        assertEquals(partitionManager.getTuplesInMemory(),2);
    }
    @Test
    public void GetSpilledPartitionWithLargestBuffer() throws HyracksDataException{
        partitionManager.insertTuple(0);
        assertEquals(partitionManager.getTuplesInMemory(),1);
        partitionManager.spillPartition(1);
        assertEquals(partitionManager.getTuplesInMemory(),1);
        partitionManager.insertTuple(numberOfPartitions+1);
        assertEquals(partitionManager.areAllPartitionsMemoryResident(),false);
        assertEquals(partitionManager.getSpilledStatus().nextSetBit(0),1);
    }
    @Test
    public void insertFrame() throws HyracksDataException{
        IFrame frame = generateIntFrame();
        tupleAccessor.reset(frame.getBuffer());
        int tuplesToInsert = tupleAccessor.getTupleCount();
        for(int i = 0; i<tuplesToInsert ; i++){
            partitionManager.insertTuple(i);
        }
        assertEquals(partitionManager.getTuplesInMemory(),tuplesToInsert);
        assertEquals(partitionManager.getTotalMemory(),numberOfPartitions * frameSize);
    }
    @Test
    public void getResidentWithLargestBuffer() throws HyracksDataException{
        IFrame frame = generateIntFrame();
        tupleAccessor.reset(frame.getBuffer());
        insertFrameToPartition(0,numberOfPartitions);
        insertFrameToPartitionUntilFillBuffers(1,numberOfPartitions,5);
        assertEquals(partitionManager.getResidentWithLargerBuffer(0),1);
    }
    @Test
    public void getResidentWithLargestBufferStartingFrom() throws HyracksDataException{
        IFrame frame = generateIntFrame();
        tupleAccessor.reset(frame.getBuffer());
        insertFrameToPartition(0,numberOfPartitions);
        insertFrameToPartitionUntilFillBuffers(1,numberOfPartitions,5);
        assertEquals(partitionManager.getResidentWithLargerBuffer(1),1);
        assertEquals(partitionManager.getResidentWithLargerBuffer(2),2);
    }
    @Test
    public void getResidentWithSmallerBuffer() throws HyracksDataException{
        IFrame frame = generateIntFrame();
        tupleAccessor.reset(frame.getBuffer());
        insertFrameToPartition(0,numberOfPartitions);
        insertFrameToPartitionUntilFillBuffers(1,numberOfPartitions,2);
        assertEquals(partitionManager.getResidentWithSmallerBuffer(),2);
    }
    @Test
    public void getRSpilledWithLargestBuffer() throws HyracksDataException{
        IFrame frame = generateIntFrame();
        tupleAccessor.reset(frame.getBuffer());
        insertFrameToPartition(0,numberOfPartitions);
        insertFrameToPartitionUntilFillBuffers(1,numberOfPartitions,2);
        partitionManager.spillPartition(0);
        partitionManager.spillPartition(1);
        assertEquals(partitionManager.getSpilledWithLargerBuffer(),0);
    }
    @Test
    public void getRSpilledWithSmallerBuffer() throws HyracksDataException{
        IFrame frame = generateIntFrame();
        tupleAccessor.reset(frame.getBuffer());
        insertFrameToPartition(0,numberOfPartitions);
        insertFrameToPartitionUntilFillBuffers(1,numberOfPartitions,2);
        partitionManager.spillPartition(0);
        partitionManager.spillPartition(1);
        insertFrameToPartition(0,numberOfPartitions);
        assertEquals(partitionManager.getSpilledWithSmallerBuffer(),1);
    }
    public void insertFrameToPartitionUntilFillBuffers(int partitionId, int numberOfPartitions,int buffersToFill) throws HyracksDataException{
        while(partitionManager.getPartition(partitionId).getMemoryUsed() < buffersToFill*frameSize){
            insertFrameToPartition(partitionId,numberOfPartitions);
        }
    }
    public void insertFrameToPartition(int partitionId, int numberOfPartitions) throws HyracksDataException{
        IFrame frame = generateIntFrame();
        tupleAccessor.reset(frame.getBuffer());
        int tuplesToInsert = tupleAccessor.getTupleCount() - partitionId;
        for(int i = 0; i<tuplesToInsert ; i += numberOfPartitions){
            partitionManager.insertTuple(i+partitionId);
        }
    }
    private class simplePartitionComputer implements ITuplePartitionComputer{
        @Override
        public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
            return tIndex % nParts;
        }
    }
    private IFrame generateIntFrame() throws HyracksDataException {
        VSizeFrame buffer = new VSizeFrame(context);
        int fieldCount = 1;
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(buffer, true);
        while (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            TupleUtils.createIntegerTuple(tb, tuple,rnd.nextInt() );
            tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        }
        return buffer;
    }

}