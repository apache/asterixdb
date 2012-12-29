package edu.uci.ics.hyracks.storage.am.lsm.btree;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.OrderedIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.IIndexTestContext;
import edu.uci.ics.hyracks.storage.am.common.TreeIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;

public class LSMBTreeLifecycleTest extends AbstractIndexLifecycleTest {

    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE };
    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();
    private final TreeIndexTestUtils titu = new OrderedIndexTestUtils();

    @SuppressWarnings("rawtypes")
    private IIndexTestContext<? extends CheckTuple> testCtx;

    @Override
    protected boolean persistentStateExists() throws Exception {
        // make sure all of the directories exist
        for (IODeviceHandle handle : harness.getIOManager().getIODevices()) {
            if (!new FileReference(handle, harness.getFileReference().getFile().getPath()).getFile().exists()) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean isEmptyIndex() throws Exception {
        return ((LSMBTree) index).isEmptyIndex();
    }

    @Override
    public void setup() throws Exception {
        harness.setUp();
        testCtx = LSMBTreeTestContext.create(harness.getMemBufferCache(), harness.getMemFreePageManager(),
                harness.getIOManager(), harness.getFileReference(), harness.getDiskBufferCache(),
                harness.getDiskFileMapProvider(), fieldSerdes, fieldSerdes.length, harness.getMergePolicy(),
                harness.getOperationTrackerFactory(), harness.getIOScheduler());
        index = testCtx.getIndex();
    }

    @Override
    public void tearDown() throws Exception {
        index.deactivate();
        index.destroy();
        harness.tearDown();
    }

    @Override
    protected void performInsertions() throws Exception {
        titu.insertIntTuples(testCtx, 10, harness.getRandom());
    }

    @Override
    protected void checkInsertions() throws Exception {
        titu.checkScan(testCtx);
    }

    @Override
    protected void clearCheckableInsertions() throws Exception {
        testCtx.getCheckTuples().clear();
    }

}
