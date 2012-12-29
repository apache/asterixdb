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
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IInMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilderFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.PartitionedLSMInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.PartitionedInMemoryInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeElementInvertedListBuilderFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.PartitionedOnDiskInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.PartitionedOnDiskInvertedIndexFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class InvertedIndexUtils {

    public static InMemoryInvertedIndex createInMemoryBTreeInvertedindex(IBufferCache memBufferCache,
            IFreePageManager memFreePageManager, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory)
            throws BTreeException {
        return new InMemoryInvertedIndex(memBufferCache, memFreePageManager, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, tokenizerFactory);
    }

    public static InMemoryInvertedIndex createPartitionedInMemoryBTreeInvertedindex(IBufferCache memBufferCache,
            IFreePageManager memFreePageManager, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory)
            throws BTreeException {
        return new PartitionedInMemoryInvertedIndex(memBufferCache, memFreePageManager, invListTypeTraits,
                invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory);
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

    public static PartitionedOnDiskInvertedIndex createPartitionedOnDiskInvertedIndex(IBufferCache bufferCache,
            IFileMapProvider fileMapProvider, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, FileReference invListsFile) throws IndexException {
        IInvertedListBuilder builder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        FileReference btreeFile = getBTreeFile(invListsFile);
        return new PartitionedOnDiskInvertedIndex(bufferCache, fileMapProvider, builder, invListTypeTraits,
                invListCmpFactories, tokenTypeTraits, tokenCmpFactories, btreeFile, invListsFile);
    }

    public static FileReference getBTreeFile(FileReference invListsFile) {
        return new FileReference(new File(invListsFile.getFile().getPath() + "_btree"));
    }

    public static BTreeFactory createDeletedKeysBTreeFactory(IFileMapProvider diskFileMapProvider,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            IBufferCache diskBufferCache) throws BTreeException {
        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(invListTypeTraits);
        ITreeIndexFrameFactory leafFrameFactory = BTreeUtils.getLeafFrameFactory(tupleWriterFactory,
                BTreeLeafFrameType.REGULAR_NSM);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        LinkedListFreePageManagerFactory freePageManagerFactory = new LinkedListFreePageManagerFactory(diskBufferCache,
                metaFrameFactory);
        BTreeFactory deletedKeysBTreeFactory = new BTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, interiorFrameFactory, leafFrameFactory, invListCmpFactories,
                invListCmpFactories.length);
        return deletedKeysBTreeFactory;
    }

    public static LSMInvertedIndex createLSMInvertedIndex(IInMemoryBufferCache memBufferCache,
            IInMemoryFreePageManager memFreePageManager, IFileMapProvider diskFileMapProvider,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, IBufferCache diskBufferCache, IIOManager ioManager,
            String onDiskDir, ILSMMergePolicy mergePolicy, ILSMOperationTrackerFactory opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler) throws IndexException {
        return createLSMInvertedIndex(memBufferCache, memFreePageManager, diskFileMapProvider, invListTypeTraits,
                invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory, diskBufferCache, ioManager,
                onDiskDir, mergePolicy, opTrackerFactory, ioScheduler, 0);
    }

    public static LSMInvertedIndex createLSMInvertedIndex(IInMemoryBufferCache memBufferCache,
            IInMemoryFreePageManager memFreePageManager, IFileMapProvider diskFileMapProvider,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, IBufferCache diskBufferCache, IIOManager ioManager,
            String onDiskDir, ILSMMergePolicy mergePolicy, ILSMOperationTrackerFactory opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, int startIODeviceIndex) throws IndexException {

        BTreeFactory deletedKeysBTreeFactory = createDeletedKeysBTreeFactory(diskFileMapProvider, invListTypeTraits,
                invListCmpFactories, diskBufferCache);

        FileReference onDiskDirFileRef = new FileReference(new File(onDiskDir));
        LSMInvertedIndexFileManager fileManager = new LSMInvertedIndexFileManager(ioManager, diskFileMapProvider,
                onDiskDirFileRef, deletedKeysBTreeFactory, startIODeviceIndex);

        IInvertedListBuilderFactory invListBuilderFactory = new FixedSizeElementInvertedListBuilderFactory(
                invListTypeTraits);
        OnDiskInvertedIndexFactory invIndexFactory = new OnDiskInvertedIndexFactory(diskBufferCache,
                diskFileMapProvider, invListBuilderFactory, invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                tokenCmpFactories, fileManager);

        LSMInvertedIndex invIndex = new LSMInvertedIndex(memBufferCache, memFreePageManager, invIndexFactory,
                deletedKeysBTreeFactory, fileManager, diskFileMapProvider, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, tokenizerFactory, mergePolicy, opTrackerFactory, ioScheduler);
        return invIndex;
    }

    public static PartitionedLSMInvertedIndex createPartitionedLSMInvertedIndex(IInMemoryBufferCache memBufferCache,
            IInMemoryFreePageManager memFreePageManager, IFileMapProvider diskFileMapProvider,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, IBufferCache diskBufferCache, IIOManager ioManager,
            String onDiskDir, ILSMMergePolicy mergePolicy, ILSMOperationTrackerFactory opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler) throws IndexException {
        return createPartitionedLSMInvertedIndex(memBufferCache, memFreePageManager, diskFileMapProvider,
                invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory,
                diskBufferCache, ioManager, onDiskDir, mergePolicy, opTrackerFactory, ioScheduler, 0);
    }

    public static PartitionedLSMInvertedIndex createPartitionedLSMInvertedIndex(IInMemoryBufferCache memBufferCache,
            IInMemoryFreePageManager memFreePageManager, IFileMapProvider diskFileMapProvider,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, IBufferCache diskBufferCache, IIOManager ioManager,
            String onDiskDir, ILSMMergePolicy mergePolicy, ILSMOperationTrackerFactory opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, int startIODeviceIndex) throws IndexException {

        BTreeFactory deletedKeysBTreeFactory = createDeletedKeysBTreeFactory(diskFileMapProvider, invListTypeTraits,
                invListCmpFactories, diskBufferCache);

        FileReference onDiskDirFileRef = new FileReference(new File(onDiskDir));
        LSMInvertedIndexFileManager fileManager = new LSMInvertedIndexFileManager(ioManager, diskFileMapProvider,
                onDiskDirFileRef, deletedKeysBTreeFactory, startIODeviceIndex);

        IInvertedListBuilderFactory invListBuilderFactory = new FixedSizeElementInvertedListBuilderFactory(
                invListTypeTraits);
        PartitionedOnDiskInvertedIndexFactory invIndexFactory = new PartitionedOnDiskInvertedIndexFactory(
                diskBufferCache, diskFileMapProvider, invListBuilderFactory, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, fileManager);

        PartitionedLSMInvertedIndex invIndex = new PartitionedLSMInvertedIndex(memBufferCache, memFreePageManager,
                invIndexFactory, deletedKeysBTreeFactory, fileManager, diskFileMapProvider, invListTypeTraits,
                invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory, mergePolicy,
                opTrackerFactory, ioScheduler);
        return invIndex;
    }
}
