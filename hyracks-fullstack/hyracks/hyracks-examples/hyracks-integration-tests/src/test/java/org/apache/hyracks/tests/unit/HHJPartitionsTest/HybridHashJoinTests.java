package org.apache.hyracks.tests.unit.HHJPartitionsTest;

import org.apache.hyracks.api.comm.*;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.join.*;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class HybridHashJoinTests {
    int numberOfPartitions = 5;
    int frameSize = 32768;
    int memorySizeInFrames = 10;
    IHyracksJobletContext context = TestUtils.create(frameSize).getJobletContext();
    RecordDescriptor buildRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[]{IntegerSerializerDeserializer.INSTANCE});
    IMissingWriterFactory dummyMissingWriters[] = new dummyMissingWriterFactory[2];
    IHybridHashJoin dmhhj;
    IHybridHashJoin hhj;
    private final Random rnd = new Random(10);

    @Test
    public void contructorTest() {
        try {
            dmhhj = new org.apache.hyracks.dataflow.std.join.HybridHashJoin(context, memorySizeInFrames,
                    numberOfPartitions, "RelS", "RelR", buildRecordDescriptor,
                    buildRecordDescriptor, new simplePartitionComputer(), new simplePartitionComputer(),
                    new dummyPredicateEvaluator(), new dummyPredicateEvaluator(), false, dummyMissingWriters);

            assertNotNull(dmhhj);
            hhj = new OptimizedHybridHashJoin(context, memorySizeInFrames,
                    numberOfPartitions, "RelS", "RelR", buildRecordDescriptor,
                    buildRecordDescriptor, new simplePartitionComputer(), new simplePartitionComputer(),
                    new dummyPredicateEvaluator(), new dummyPredicateEvaluator(), false, dummyMissingWriters);
            assertNotNull(hhj);
        } catch (Exception ex) {
            fail();
        }
    }

    public void testBuildPartitionSizes(IHybridHashJoin hhj1, IHybridHashJoin hhj2) {
        assertEquals(hhj1.getPartitionStatus(), hhj2.getPartitionStatus());
        for (int i = 0; i < numberOfPartitions; i++) {
            assertEquals(hhj1.getBuildPartitionSizeInTup(i), hhj2.getBuildPartitionSizeInTup(i));
        }
    }

    public void testBuildPartitionTemporaryFiles(IHybridHashJoin hhj1, IHybridHashJoin hhj2) throws HyracksDataException {
        RunFileReader reader1, reader2;
        for (int i = 0; i < numberOfPartitions; i++) {
            reader1 = hhj1.getBuildRFReader(i);
            reader2 = hhj2.getBuildRFReader(i);
            int tuples1 = reader1 != null ? getNumberOfTuplesInTemporaryFile(reader1) : 0;
            int tuples2 = reader2 != null ? getNumberOfTuplesInTemporaryFile(reader2) : 0;
            assertEquals(tuples1, tuples2);
        }
    }

    @Test
    public void initBuildTest() throws HyracksDataException {
        contructorTest();
        dmhhj.initBuild();
        hhj.initBuild();
        testBuildPartitionSizes(dmhhj, hhj);
    }

    @Test
    public void buildNextWithNoSpills() throws HyracksDataException {
        initBuildTest();
        IFrame frame = generateIntFrame();
        dmhhj.build(frame.getBuffer());
        hhj.build(frame.getBuffer());
        assertTrue(dmhhj.getPartitionStatus().cardinality() < numberOfPartitions);
        assertTrue(hhj.getPartitionStatus().cardinality() < numberOfPartitions);
        testBuildPartitionSizes(dmhhj, hhj);
        testBuildPartitionTemporaryFiles(dmhhj, hhj);
    }

    @Test
    public void buildNextWithFullSpillingsTest() throws HyracksDataException {
        initBuildTest();
        for (int i = 0; i < memorySizeInFrames + 30; i++) {
            IFrame frame = generateIntFrame();
            dmhhj.build(frame.getBuffer());
            hhj.build(frame.getBuffer());
            testBuildPartitionSizes(dmhhj, hhj);
        }
        assertEquals(dmhhj.getPartitionStatus().cardinality(), numberOfPartitions);
        assertEquals(hhj.getPartitionStatus().cardinality(), numberOfPartitions);
        testBuildPartitionTemporaryFiles(dmhhj, hhj);
    }

    @Test
    public void buildNextWithSpillingsTest() throws HyracksDataException {
        initBuildTest();
        for (int i = 0; i < memorySizeInFrames + 1; i++) {
            IFrame frame = generateIntFrame();
            dmhhj.build(frame.getBuffer());
            hhj.build(frame.getBuffer());
            testBuildPartitionSizes(dmhhj, hhj);
        }
        assertTrue(dmhhj.getPartitionStatus().cardinality() < numberOfPartitions);
        assertTrue(hhj.getPartitionStatus().cardinality() < numberOfPartitions);
        testBuildPartitionTemporaryFiles(dmhhj, hhj);
    }

    @Test
    public void CloseBuildTest() throws HyracksDataException {
        buildNextWithNoSpills();
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
    public void InitProbeTest() throws HyracksDataException {
        CloseBuildTest();
        dmhhj.initProbe(new simpleTupleComparator());
        hhj.initProbe(new simpleTupleComparator());
        assertEquals(dmhhj.getBuildPartitionSizeInTup(0), hhj.getBuildPartitionSizeInTup(0));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(1), hhj.getBuildPartitionSizeInTup(1));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(2), hhj.getBuildPartitionSizeInTup(2));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(3), hhj.getBuildPartitionSizeInTup(3));
        assertEquals(dmhhj.getBuildPartitionSizeInTup(4), hhj.getBuildPartitionSizeInTup(4));
        assertEquals(dmhhj.getPartitionStatus(), hhj.getPartitionStatus());
    }

    @Test
    public void ProbeTest() throws HyracksDataException {
        InitProbeTest();
        FileReference file1 = context.createManagedWorkspaceFile("OUTPUT_DYNAMIC-");
        RunFileWriter frameWriterDynamic = new RunFileWriter(file1, context.getIoManager());
        frameWriterDynamic.open();
        FileReference file2 = context.createManagedWorkspaceFile("RelS");
        RunFileWriter frameWriter = new RunFileWriter(file2, context.getIoManager());
        frameWriter.open();
        IFrame frame = generateIntFrame();
        dmhhj.probe(frame.getBuffer(), frameWriterDynamic);
        hhj.probe(frame.getBuffer(), frameWriter);
        assertEquals(dmhhj.getProbePartitionSizeInTup(0), hhj.getProbePartitionSizeInTup(0));
        assertEquals(dmhhj.getProbePartitionSizeInTup(1), hhj.getProbePartitionSizeInTup(1));
        assertEquals(dmhhj.getProbePartitionSizeInTup(2), hhj.getProbePartitionSizeInTup(2));
        assertEquals(dmhhj.getProbePartitionSizeInTup(3), hhj.getProbePartitionSizeInTup(3));
        assertEquals(dmhhj.getProbePartitionSizeInTup(4), hhj.getProbePartitionSizeInTup(4));
        assertEquals(file1.getFile().getTotalSpace(), file2.getFile().getTotalSpace());

        assertEquals(getNumberOfTuplesInTemporaryFile(frameWriterDynamic), getNumberOfTuplesInTemporaryFile(frameWriter));
    }

    @Test
    public void ProbeTestWithSpillings() throws HyracksDataException {
        InitProbeTest();
        FileReference file1 = context.createManagedWorkspaceFile("Output_Dynamic_");
        RunFileWriter frameWriterDynamic = new RunFileWriter(file1, context.getIoManager());
        frameWriterDynamic.open();
        FileReference file2 = context.createManagedWorkspaceFile("Output_");
        RunFileWriter frameWriter = new RunFileWriter(file2, context.getIoManager());
        frameWriter.open();
        IFrame frame;
        for (int i = 0; i < memorySizeInFrames + 300; i++) {
            frame = generateIntFrame();
            dmhhj.probe(frame.getBuffer(), frameWriterDynamic);
            hhj.probe(frame.getBuffer(), frameWriter);
            assertEquals(dmhhj.getProbePartitionSizeInTup(0), hhj.getProbePartitionSizeInTup(0));
            assertEquals(dmhhj.getProbePartitionSizeInTup(1), hhj.getProbePartitionSizeInTup(1));
            assertEquals(dmhhj.getProbePartitionSizeInTup(2), hhj.getProbePartitionSizeInTup(2));
            assertEquals(dmhhj.getProbePartitionSizeInTup(3), hhj.getProbePartitionSizeInTup(3));
            assertEquals(dmhhj.getProbePartitionSizeInTup(4), hhj.getProbePartitionSizeInTup(4));
            assertEquals(dmhhj.getMaxBuildPartitionSize(), dmhhj.getMaxBuildPartitionSize());
            assertEquals(file1.getFile().getTotalSpace(), file2.getFile().getTotalSpace());
        }
        assertEquals(getNumberOfTuplesInTemporaryFile(frameWriterDynamic), getNumberOfTuplesInTemporaryFile(frameWriter));
    }

    @Test
    public void CompleteProbeTest() throws HyracksDataException {
        ProbeTestWithSpillings();
        FileReference file1 = context.createManagedWorkspaceFile("RelS_Dynamic_Output-");
        RunFileWriter frameWriterDynamic = new RunFileWriter(file1, context.getIoManager());
        frameWriterDynamic.open();
        FileReference file2 = context.createManagedWorkspaceFile("RelS_Remaining_Output-");
        RunFileWriter frameWriter = new RunFileWriter(file2, context.getIoManager());
        frameWriter.open();
        dmhhj.completeProbe(frameWriterDynamic);
        hhj.completeProbe(frameWriter);
        assertEquals(file1.getFile().getTotalSpace(), file2.getFile().getTotalSpace());
        assertEquals(getNumberOfTuplesInTemporaryFile(frameWriterDynamic), getNumberOfTuplesInTemporaryFile(frameWriter));
    }

    @Test
    public void testCompare() throws HyracksDataException {
        ITuplePairComparator tupleComparator = new simpleTupleComparator();
        IFrame frame = generateIntFrame();
        IFrameTupleAccessor accessor = new FrameTupleAccessor(buildRecordDescriptor);
        accessor.reset(frame.getBuffer());
        IFrame frame2 = generateIntFrame();
        IFrameTupleAccessor accessor2 = new FrameTupleAccessor(buildRecordDescriptor);
        accessor2.reset(frame2.getBuffer());
        int ret = tupleComparator.compare(accessor, 0, accessor, 0);
        assertEquals(ret, 1);
        int ret2 = tupleComparator.compare(accessor, 0, accessor, 1);
        assertEquals(ret2, 0);

    }

    protected class simpleTupleComparator implements ITuplePairComparator {
        @Override
        public int compare(IFrameTupleAccessor outerRef, int outerIndex, IFrameTupleAccessor innerRef, int innerIndex) throws HyracksDataException {
            int tuple1 = tupleToInt(outerRef, outerIndex);
            int tuple2 = tupleToInt(innerRef, innerIndex);
            return tuple2 == tuple1 ? 1 : 0;
        }

    }

    private int tupleToInt(IFrameTupleAccessor accessor, int tupleId) {
        tupleId += 1;
        byte[] arr = accessor.getBuffer().array();
        int t1 = accessor.getFieldEndOffset(tupleId, 0) + accessor.getTupleStartOffset(tupleId);
        return ((arr[t1] & 0xFF) << 24) | ((arr[t1 + 1] & 0xFF) << 16) | ((arr[t1 + 2] & 0xFF) << 8) | ((arr[t1 + 3] & 0xFF) << 0);
    }

    protected class simplePartitionComputer implements ITuplePartitionComputer {
        @Override
        public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
            return tupleToInt(accessor, tIndex) % nParts;
        }
    }

    protected class dummyPredicateEvaluator implements IPredicateEvaluator {
        @Override
        public boolean evaluate(IFrameTupleAccessor fta, int tupId) {
            return true;
        }
    }

    protected class dummyMissingWriterFactory implements IMissingWriterFactory {
        @Override
        public IMissingWriter createMissingWriter() {
            return null;
        }
    }

    protected IFrame generateIntFrame() throws HyracksDataException {
        VSizeFrame buffer = new VSizeFrame(context);
        int fieldCount = 1;
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(buffer, true);
        while (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            int nextInt = rnd.nextInt(10000);
            TupleUtils.createIntegerTuple(tb, tuple, nextInt);
            tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        }
        return buffer;
    }

    protected int getNumberOfTuplesInTemporaryFile(RunFileWriter fileWriter) throws HyracksDataException {
        RunFileReader reader = fileWriter.createDeleteOnCloseReader();
        return getNumberOfTuplesInTemporaryFile(reader);
    }

    protected int getNumberOfTuplesInTemporaryFile(RunFileReader fileReader) throws HyracksDataException {
        IFrame frame1 = new VSizeFrame(context);
        fileReader.open();
        int tupleCnt = 0;
        while (fileReader.nextFrame(frame1)) {
            FrameTupleAccessor accessor2 = new FrameTupleAccessor(buildRecordDescriptor);
            accessor2.reset(frame1.getBuffer());
            tupleCnt += accessor2.getTupleCount();
        }
        return tupleCnt;
    }
}
