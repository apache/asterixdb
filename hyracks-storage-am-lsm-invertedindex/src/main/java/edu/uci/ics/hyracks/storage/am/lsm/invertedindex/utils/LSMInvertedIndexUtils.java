package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.utils;

import java.util.Arrays;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.InvertedIndexFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.LSMInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.LSMInvertedIndexFileManager;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMInvertedIndexUtils {
    public static InMemoryInvertedIndex createInMemoryBTreeInvertedindex(InMemoryBufferCache memBufferCache,
            InMemoryFreePageManager memFreePageManager, ITypeTraits[] tokenTypeTraits, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryComparatorFactory[] invListCmpFactories,
            IBinaryTokenizer tokenizer) {
        return new InMemoryInvertedIndex(btree, invListTypeTraits, invListCmpFactories, tokenizer);
    }

    public static InvertedIndex createInvertedIndex(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizer tokenizer) {
        IInvertedListBuilder builder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        return new InvertedIndex(bufferCache, fileMapProvider, invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories, builder, tokenizer);
    }

    public static LSMInvertedIndex createLSMInvertedIndex(InMemoryBufferCache memBufferCache,
            InMemoryFreePageManager memFreePageManager, ITypeTraits[] tokenTypeTraits, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryComparatorFactory[] invListCmpFactories,
            IBinaryTokenizer tokenizer, IBufferCache diskBufferCache,
            LinkedListFreePageManagerFactory diskFreePageManagerFactory, IOManager ioManager, String onDiskDir,
            IFileMapProvider diskFileMapProvider) {
        InMemoryInvertedIndex memoryInvertedIndex = LSMInvertedIndexUtils.createInMemoryBTreeInvertedindex(
                memBufferCache, memFreePageManager, tokenTypeTraits, invListTypeTraits, tokenCmpFactories,
                invListCmpFactories, tokenizer);
        ITypeTraits[] combinedTraits = concatArrays(tokenTypeTraits, new ITypeTraits[] { IntegerPointable.TYPE_TRAITS,
                IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS });
        ITreeIndexTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(combinedTraits);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        BTreeFactory diskBTreeFactory = new BTreeFactory(diskBufferCache, diskFreePageManagerFactory,
                tokenCmpFactories, tokenCmpFactories.length + 4, interiorFrameFactory, leafFrameFactory);
        LSMInvertedIndexFileManager fileManager = new LSMInvertedIndexFileManager(ioManager, diskFileMapProvider,
                onDiskDir);
        IInvertedListBuilder invListBuilder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        InvertedIndexFactory diskInvertedIndexFactory = new InvertedIndexFactory(diskBufferCache, invListTypeTraits,
                invListCmpFactories, invListBuilder, tokenizer, fileManager);
        return new LSMInvertedIndex(memoryInvertedIndex, diskBTreeFactory, diskInvertedIndexFactory, fileManager,
                diskFileMapProvider);
    }

    private static <T> T[] concatArrays(T[] first, T[] last) {
        T[] concatenated = Arrays.copyOf(first, first.length + last.length);
        System.arraycopy(last, 0, concatenated, first.length, last.length);
        return concatenated;
    }
}
