package edu.uci.ics.hyracks.storage.am.btree;

import java.util.Random;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestContext;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.ITreeIndexTestContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;

public class BTreeLifecycleTest extends AbstractIndexLifecycleTest {
    private final BTreeTestHarness harness = new BTreeTestHarness();

    private final ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE };

    private ITreeIndexFrame frame = null;

    private ITreeIndexTestContext<? extends CheckTuple> testCtx;

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
    protected Random getRandom() {
        return harness.getRandom();
    }
}
