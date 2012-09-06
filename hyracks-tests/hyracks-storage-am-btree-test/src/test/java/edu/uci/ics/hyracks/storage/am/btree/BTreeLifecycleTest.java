package edu.uci.ics.hyracks.storage.am.btree;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestContext;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.IIndexTestContext;
import edu.uci.ics.hyracks.storage.am.common.TreeIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;

public class BTreeLifecycleTest extends AbstractIndexLifecycleTest {
    private final BTreeTestHarness harness = new BTreeTestHarness();
    private final TreeIndexTestUtils titu = new OrderedIndexTestUtils();

    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE };

    private ITreeIndexFrame frame = null;

    @SuppressWarnings("rawtypes")
    private IIndexTestContext<? extends CheckTuple> testCtx;

    @Override
    public void setup() throws Exception {
        harness.setUp();
        testCtx = BTreeTestContext.create(harness.getBufferCache(), harness.getFileMapProvider(),
                harness.getFileReference(), fieldSerdes, fieldSerdes.length, BTreeLeafFrameType.REGULAR_NSM);
        index = testCtx.getIndex();
    }

    @Override
    public void tearDown() throws HyracksDataException {
        testCtx.getIndex().deactivate();
        testCtx.getIndex().destroy();
        harness.tearDown();
    }

    @Override
    protected boolean persistentStateExists() {
        return harness.getFileReference().getFile().exists();
    }

    @Override
    protected boolean isEmptyIndex() throws HyracksDataException {
        BTree btree = (BTree) testCtx.getIndex();
        if (frame == null) {
            frame = btree.getInteriorFrameFactory().createFrame();
        }
        return btree.isEmptyTree(frame);
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
