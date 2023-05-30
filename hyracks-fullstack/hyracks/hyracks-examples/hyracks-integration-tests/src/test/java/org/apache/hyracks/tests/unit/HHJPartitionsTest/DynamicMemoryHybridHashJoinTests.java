package org.apache.hyracks.tests.unit.HHJPartitionsTest;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.join.HybridHashJoin;
import org.apache.hyracks.dataflow.std.join.MemoryContentionResponsiveHHJ;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.*;

public class DynamicMemoryHybridHashJoinTests extends HybridHashJoinTests {

    private static final Logger LOGGER = LogManager.getLogger();
    @Test
    public void contructorTest() {
        try {
            dmhhj = new MemoryContentionResponsiveHHJ(context, memorySizeInFrames,
                    numberOfPartitions, "RelS", "RelR", buildRecordDescriptor,
                    buildRecordDescriptor, new simplePartitionComputer(), new simplePartitionComputer(),
                    new dummyPredicateEvaluator(), new dummyPredicateEvaluator(), false, dummyMissingWriters);

            assertNotNull(dmhhj);
            hhj = new HybridHashJoin(context, memorySizeInFrames,
                    numberOfPartitions, "RelS", "RelR", buildRecordDescriptor,
                    buildRecordDescriptor, new simplePartitionComputer(), new simplePartitionComputer(),
                    new dummyPredicateEvaluator(), new dummyPredicateEvaluator(), false, dummyMissingWriters);
            assertNotNull(hhj);
        } catch (Exception ex) {
            fail();
        }
    }
    @Test void memoryExpansionDuringBuild() throws HyracksDataException {
        initBuildTest();
        for (int i = 0; i < 15; i++) {
            IFrame frame = generateIntFrame();
            dmhhj.build(frame.getBuffer());
            hhj.build(frame.getBuffer());
            dmhhj.updateMemoryBudget(20);
        }
        assertTrue(dmhhj.getPartitionStatus().cardinality() < hhj.getPartitionStatus().cardinality());
    }
    @Test void memoryContentionDuringBuild() throws HyracksDataException {
        initBuildTest();
        for (int i = 0; i < 15; i++) {
            IFrame frame = generateIntFrame();
            dmhhj.build(frame.getBuffer());
            hhj.build(frame.getBuffer());
            dmhhj.updateMemoryBudget(7);
        }
        assertTrue(dmhhj.getPartitionStatus().cardinality() > hhj.getPartitionStatus().cardinality());
    }

    @Test
    void memoryContentionDuringProbe() throws  HyracksDataException{
        initBuildTest();
        for (int i = 0; i < 15; i++) {
            IFrame frame = generateIntFrame();
            dmhhj.build(frame.getBuffer());
        }
        FileReference file1 = context.createManagedWorkspaceFile("Output_Dynamic_");
        RunFileWriter frameWriterDynamic = new RunFileWriter(file1, context.getIoManager());
        frameWriterDynamic.open();
        dmhhj.closeBuild();
        int spilledStatusBuild = dmhhj.getPartitionStatus().cardinality();
        dmhhj.initProbe(new simpleTupleComparator());
        IFrame frame;
        for (int i = 0; i < memorySizeInFrames; i++) {
            frame = generateIntFrame();
            dmhhj.probe(frame.getBuffer(), frameWriterDynamic);
        }
        //30 so it can fit 5 frames for each build Partition, 5 frames for each probe Partition,
        // plus the Hash Table and some space to bring a partition back.
        dmhhj.updateMemoryBudgetProbe(memorySizeInFrames + 30);
        assertTrue(dmhhj.getPartitionStatus().cardinality() < spilledStatusBuild);
    }
}
