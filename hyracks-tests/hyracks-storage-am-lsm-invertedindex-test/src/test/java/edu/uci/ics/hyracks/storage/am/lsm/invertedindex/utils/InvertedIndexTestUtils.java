package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.utils;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.InMemoryBtreeInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.inverteredindex.LSMInvertedIndexTestHarness;

public class InvertedIndexTestUtils {
    public static InvertedIndex createTestInvertedIndex(LSMInvertedIndexTestHarness harness, IBinaryTokenizer tokenizer)
            throws HyracksDataException {
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        ITypeTraits[] btreeTypeTraits = new ITypeTraits[] { UTF8StringPointable.TYPE_TRAITS,
                IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS,
                IntegerPointable.TYPE_TRAITS };
        ITreeIndexTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(btreeTypeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        IFreePageManager freePageManager = new LinkedListFreePageManager(harness.getDiskBufferCache(), 0,
                metaFrameFactory);
        IBinaryComparatorFactory[] btreeCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                .of(UTF8StringPointable.FACTORY) };
        BTree btree = new BTree(harness.getDiskBufferCache(), 5, btreeCmpFactories, freePageManager,
                interiorFrameFactory, leafFrameFactory);
        btree.create(harness.getDiskBtreeFileId());
        btree.open(harness.getDiskBtreeFileId());
        return LSMInvertedIndexUtils.createInvertedIndex(harness.getDiskBufferCache(), btree,
                harness.getInvertedListTypeTraits(), harness.getInvertedListBinaryComparatorFactories(), tokenizer);
    }

    public static InMemoryBtreeInvertedIndex createTestInMemoryBTreeInvertedIndex(LSMInvertedIndexTestHarness harness,
            IBinaryTokenizer tokenizer) {
        return LSMInvertedIndexUtils.createInMemoryBTreeInvertedindex(harness.getMemBufferCache(),
                harness.getMemFreePageManager(), harness.getTokenTypeTraits(), harness.getInvertedListTypeTraits(),
                harness.getTokenBinaryComparatorFactories(), harness.getInvertedListBinaryComparatorFactories(),
                tokenizer);
    }

    public static LSMInvertedIndex createTestLSMInvertedIndex(LSMInvertedIndexTestHarness harness,
            IBinaryTokenizer tokenizer) {
        return LSMInvertedIndexUtils.createLSMInvertedIndex(harness.getMemBufferCache(),
                harness.getMemFreePageManager(), harness.getTokenTypeTraits(), harness.getInvertedListTypeTraits(),
                harness.getTokenBinaryComparatorFactories(), harness.getInvertedListBinaryComparatorFactories(),
                tokenizer, harness.getDiskBufferCache(),
                new LinkedListFreePageManagerFactory(harness.getDiskBufferCache(), new LIFOMetaDataFrameFactory()),
                harness.getIOManager(), harness.getOnDiskDir(), harness.getDiskFileMapProvider());
    }
}
