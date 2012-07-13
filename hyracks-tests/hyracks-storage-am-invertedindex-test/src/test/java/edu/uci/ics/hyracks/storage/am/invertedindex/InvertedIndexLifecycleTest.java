package edu.uci.ics.hyracks.storage.am.invertedindex;

import java.util.Random;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;

public class InvertedIndexLifecycleTest extends AbstractIndexLifecycleTest {

    private final InvertedIndexTestHarness harness = new InvertedIndexTestHarness();
    private ITreeIndexFrame frame = null;

    @Override
    protected boolean persistentStateExists() throws Exception {
        return harness.getFileReference().getFile().exists()
                && ((InvertedIndex) index).getBTree().getFileReference().getFile().exists();
    }

    @Override
    protected boolean isEmptyIndex() throws Exception {
        if (frame == null) {
            frame = ((InvertedIndex) index).getBTree().getLeafFrameFactory().createFrame();
        }
        return ((InvertedIndex) index).getBTree().isEmptyTree(frame);
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }

    @Override
    public void setup() throws Exception {
        harness.setUp();
        ITypeTraits[] tokenTypeTraits = new ITypeTraits[] { UTF8StringPointable.TYPE_TRAITS };
        IBinaryComparatorFactory[] tokenCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                .of(UTF8StringPointable.FACTORY) };
        ITypeTraits[] invListTypeTraits = new ITypeTraits[] { IntegerPointable.TYPE_TRAITS };
        IBinaryComparatorFactory[] invListCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                .of(IntegerPointable.FACTORY) };
        IInvertedListBuilder invListBuilder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        index = new InvertedIndex(harness.getBufferCache(), harness.getFileMapProvider(), invListBuilder,
                invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories, harness.getFileReference());

    }

    @Override
    public void tearDown() throws Exception {
        index.deactivate();
        index.destroy();
        harness.tearDown();
    }
}
