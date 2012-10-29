package edu.uci.ics.hyracks.storage.am.lsm.btree;

import org.junit.Test;

import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.AbstractModificationOperationCallbackTest;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpOperationTrackerFactory;

public class LSMBTreeModificationOperationCallbackTest extends AbstractModificationOperationCallbackTest {
    private static final int NUM_TUPLES = 11;

    private final LSMBTreeTestHarness harness;
    private final BlockingIOOperationCallback ioOpCallback;

    public LSMBTreeModificationOperationCallbackTest() {
        super();
        this.ioOpCallback = new BlockingIOOperationCallback();
        harness = new LSMBTreeTestHarness();
    }

    @Override
    protected void createIndexInstance() throws Exception {
        index = LSMBTreeUtils.createLSMTree(harness.getMemBufferCache(), harness.getMemFreePageManager(),
                harness.getIOManager(), harness.getFileReference(), harness.getDiskBufferCache(),
                harness.getDiskFileMapProvider(), SerdeUtils.serdesToTypeTraits(keySerdes),
                SerdeUtils.serdesToComparatorFactories(keySerdes, keySerdes.length), harness.getFlushController(),
                harness.getMergePolicy(), NoOpOperationTrackerFactory.INSTANCE, harness.getIOScheduler());
    }

    @Override
    public void setup() throws Exception {
        harness.setUp();
        super.setup();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        harness.tearDown();
    }

    @Test
    public void modificationCallbackTest() throws Exception {
        IIndexAccessor accessor = index.createAccessor(cb, NoOpOperationCallback.INSTANCE);
        ILSMIOOperation flushOp = ((ILSMIndexAccessor) accessor).createFlushOperation(ioOpCallback);

        for (int j = 0; j < 2; j++) {
            isFoundNull = true;
            for (int i = 0; i < NUM_TUPLES; i++) {
                TupleUtils.createIntegerTuple(builder, tuple, i);
                accessor.insert(tuple);
            }

            if (j == 1) {
                harness.getIOScheduler().scheduleOperation(flushOp);
                ioOpCallback.waitForIO();
                isFoundNull = true;
            } else {
                isFoundNull = false;
            }

            for (int i = 0; i < NUM_TUPLES; i++) {
                TupleUtils.createIntegerTuple(builder, tuple, i);
                accessor.upsert(tuple);
            }

            if (j == 1) {
                harness.getIOScheduler().scheduleOperation(flushOp);
                ioOpCallback.waitForIO();
                isFoundNull = true;
            } else {
                isFoundNull = false;
            }

            for (int i = 0; i < NUM_TUPLES; i++) {
                TupleUtils.createIntegerTuple(builder, tuple, i);
                accessor.delete(tuple);
            }

            harness.getIOScheduler().scheduleOperation(flushOp);
            ioOpCallback.waitForIO();
        }
    }
}
