/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util;

import java.io.File;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class InvertedIndexUtils {
    
    public static InMemoryInvertedIndex createInMemoryBTreeInvertedindex(IBufferCache memBufferCache,
            IFreePageManager memFreePageManager, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory) throws BTreeException {
        return new InMemoryInvertedIndex(memBufferCache, memFreePageManager, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, tokenizerFactory);
    }

    public static OnDiskInvertedIndex createOnDiskInvertedIndex(IBufferCache bufferCache,
            IFileMapProvider fileMapProvider, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, FileReference invListsFile) throws IndexException {
        IInvertedListBuilder builder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        FileReference btreeFile = getBTreeFile(invListsFile);
        return new OnDiskInvertedIndex(bufferCache, fileMapProvider, builder, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, btreeFile, invListsFile);
    }

    public static FileReference getBTreeFile(FileReference invListsFile) {
        return new FileReference(new File(invListsFile.getFile().getPath() + "_btree"));
    }
    
    /*
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
        OnDiskInvertedIndexFactory diskInvertedIndexFactory = new OnDiskInvertedIndexFactory(diskBufferCache, invListTypeTraits,
                invListCmpFactories, invListBuilder, tokenizer, fileManager);
        return new LSMInvertedIndex(memoryInvertedIndex, diskBTreeFactory, diskInvertedIndexFactory, fileManager,
                diskFileMapProvider);
    }

    private static <T> T[] concatArrays(T[] first, T[] last) {
        T[] concatenated = Arrays.copyOf(first, first.length + last.length);
        System.arraycopy(last, 0, concatenated, first.length, last.length);
        return concatenated;
    }
    */
}
