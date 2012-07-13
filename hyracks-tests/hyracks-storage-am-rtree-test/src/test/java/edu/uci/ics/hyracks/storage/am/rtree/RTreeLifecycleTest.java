package edu.uci.ics.hyracks.storage.am.rtree;

import java.util.Random;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;
import edu.uci.ics.hyracks.storage.am.rtree.utils.RTreeTestContext;
import edu.uci.ics.hyracks.storage.am.rtree.utils.RTreeTestHarness;

public class RTreeLifecycleTest extends AbstractIndexLifecycleTest {
    private final RTreeTestHarness harness = new RTreeTestHarness();

    private final ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
    private final IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils
            .createPrimitiveValueProviderFactories(4, IntegerPointable.FACTORY);
    private final int numKeys = 4;

    private ITreeIndexFrame frame = null;

    @Override
    public void setup() throws Exception {
        harness.setUp();
        testCtx = RTreeTestContext.create(harness.getBufferCache(), harness.getFileMapProvider(),
                harness.getFileReference(), fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RTREE);
        index = testCtx.getIndex();
        titu = new RTreeTestUtils();
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
        RTree rtree = (RTree) testCtx.getIndex();
        if (frame == null) {
            frame = rtree.getInteriorFrameFactory().createFrame();
        }
        return rtree.isEmptyTree(frame);
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }
}
