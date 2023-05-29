package org.apache.hyracks.tests.unit.HHJPartitionsTest;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.join.HybridHashJoin;
import org.apache.hyracks.dataflow.std.join.MemoryContentionResponsiveHHJ;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoin;
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
}
