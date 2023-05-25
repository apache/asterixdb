package org.apache.hyracks.tests.unit.HHJPartitionsTest;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.join.DynamicMemoryOptimizedHybridHashJoin;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoin;
import org.apache.hyracks.dataflow.std.join.PartitionManager;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.jupiter.api.Test;
import java.util.BitSet;
import java.util.Random;

import static org.junit.Assert.*;

public class DynamicMemoryOptimizedHHJTest {
    int numberOfPartitions = 5;
    int frameSize = 32768;
    int memorySizeInFrames = 10;
    IHyracksJobletContext context = TestUtils.create(frameSize).getJobletContext();
    RecordDescriptor buildRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
    RecordDescriptor probeRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
    PartitionManager partitionManager;
    BitSet status;
    IFrameTupleAccessor buildTupleAccessor;
    IMissingWriterFactory dummyMissingWriters[] = new dummyMissingWriterFactory[2];
    DynamicMemoryOptimizedHybridHashJoin dmhhj;
    OptimizedHybridHashJoin hhj;
    private final Random rnd = new Random(50);
    @Test
    public void contructorTest(){
        buildTupleAccessor = new FrameTupleAccessor(buildRecordDescriptor);
        try {
            dmhhj = new DynamicMemoryOptimizedHybridHashJoin(context, memorySizeInFrames,
                    numberOfPartitions, "RelS", "RelR", probeRecordDescriptor,
                    buildRecordDescriptor, new simplePartitionComputer(), new simplePartitionComputer(),
                    new dummyPredicateEvaluator(), new dummyPredicateEvaluator(), false, dummyMissingWriters);

            assertNotNull(dmhhj);
            hhj = new OptimizedHybridHashJoin(context, memorySizeInFrames,
                    numberOfPartitions, "RelS", "RelR", probeRecordDescriptor,
                    buildRecordDescriptor, new simplePartitionComputer(), new simplePartitionComputer(),
                    new dummyPredicateEvaluator(), new dummyPredicateEvaluator(), false, dummyMissingWriters);
            assertNotNull(hhj);
        }
        catch (Exception ex){
            fail();
        }
    }

    @Test
    public void initBuildTest() throws HyracksDataException{
        contructorTest();
        dmhhj.initBuild();
        hhj.initBuild();
        assertEquals(dmhhj.getPartitionStatus(), hhj.getPartitionStatus());
        assertEquals(dmhhj.getBuildPartitionSizeInTup(0),hhj.getBuildPartitionSizeInTup(0));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(1),hhj.getBuildPartitionSizeInTup(1));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(2),hhj.getBuildPartitionSizeInTup(2));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(3),hhj.getBuildPartitionSizeInTup(3));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(4),hhj.getBuildPartitionSizeInTup(4));
    }

    @Test
    public void buildNextTest() throws HyracksDataException{
        initBuildTest();
        IFrame frame = generateIntFrame();
        dmhhj.build(frame.getBuffer());
        hhj.build(frame.getBuffer());
        assertEquals(dmhhj.getPartitionStatus(), hhj.getPartitionStatus());
        assertEquals(dmhhj.getBuildPartitionSizeInTup(0),hhj.getBuildPartitionSizeInTup(0));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(1),hhj.getBuildPartitionSizeInTup(1));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(2),hhj.getBuildPartitionSizeInTup(2));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(3),hhj.getBuildPartitionSizeInTup(3));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(4),hhj.getBuildPartitionSizeInTup(4));
    }

    @Test
    public void buildNextWithSpillingsTest() throws HyracksDataException{
        initBuildTest();
        for(int i=0 ; i < memorySizeInFrames+ rnd.nextInt(30);i++) {
            IFrame frame = generateIntFrame();
            dmhhj.build(frame.getBuffer());
            hhj.build(frame.getBuffer());
            assertEquals(dmhhj.getBuildPartitionSizeInTup(0), hhj.getBuildPartitionSizeInTup(0));
            assertEquals(dmhhj.getBuildPartitionSizeInTup(1), hhj.getBuildPartitionSizeInTup(1));
            assertEquals(dmhhj.getBuildPartitionSizeInTup(2), hhj.getBuildPartitionSizeInTup(2));
            assertEquals(dmhhj.getBuildPartitionSizeInTup(3), hhj.getBuildPartitionSizeInTup(3));
            assertEquals(dmhhj.getBuildPartitionSizeInTup(4), hhj.getBuildPartitionSizeInTup(4));
            assertEquals(dmhhj.getPartitionStatus(), hhj.getPartitionStatus());
        }
    }

    @Test
    public void CloseBuildTest1() throws HyracksDataException{
        buildNextWithSpillingsTest();
        dmhhj.closeBuild();
        hhj.closeBuild();
        assertEquals(dmhhj.getBuildPartitionSizeInTup(0), hhj.getBuildPartitionSizeInTup(0));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(1), hhj.getBuildPartitionSizeInTup(1));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(2), hhj.getBuildPartitionSizeInTup(2));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(3), hhj.getBuildPartitionSizeInTup(3));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(4), hhj.getBuildPartitionSizeInTup(4));
        assertEquals(dmhhj.getPartitionStatus(), hhj.getPartitionStatus());
    }

    @Test
    public void CompleteBuild10Times() throws HyracksDataException{
        for(int i =0 ; i< 10;i++){
            CloseBuildTest1();
        }
    }

    private class simplePartitionComputer implements ITuplePartitionComputer {
        @Override
        public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
            return tIndex % nParts;
        }
    }
    private class dummyPredicateEvaluator implements IPredicateEvaluator {
        @Override
        public boolean evaluate(IFrameTupleAccessor fta, int tupId) {
            return true;
        }
    }
    private class dummyMissingWriterFactory implements IMissingWriterFactory {
        @Override
        public IMissingWriter createMissingWriter() {
            return null;
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
