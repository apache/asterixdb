package edu.uci.ics.hyracks.storage.am.btree;

import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;

public class BTreeModificationOperationCallbackTest extends AbstractModificationOperationCallbackTest {
    private final BTreeTestHarness harness;

    public BTreeModificationOperationCallbackTest() {
        harness = new BTreeTestHarness();
    }

    @Override
    protected void createIndexInstance() throws Exception {
        index = BTreeUtils.createBTree(harness.getBufferCache(), harness.getFileMapProvider(),
                SerdeUtils.serdesToTypeTraits(keySerdes),
                SerdeUtils.serdesToComparatorFactories(keySerdes, keySerdes.length), BTreeLeafFrameType.REGULAR_NSM,
                harness.getFileReference());
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

}
