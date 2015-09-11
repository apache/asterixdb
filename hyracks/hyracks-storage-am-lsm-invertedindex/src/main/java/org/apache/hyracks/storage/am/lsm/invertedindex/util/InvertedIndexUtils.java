/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.lsm.invertedindex.util;

import java.io.File;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.btree.exceptions.BTreeException;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.IFreePageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.LinkedListFreePageManagerFactory;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.frames.LSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilderFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexFileManager;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.PartitionedLSMInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.inmemory.PartitionedInMemoryInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeElementInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeElementInvertedListBuilderFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.PartitionedOnDiskInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.PartitionedOnDiskInvertedIndexFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class InvertedIndexUtils {

    public static InMemoryInvertedIndex createInMemoryBTreeInvertedindex(IBufferCache memBufferCache,
            IFreePageManager virtualFreePageManager, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            FileReference btreeFileRef) throws BTreeException {
        return new InMemoryInvertedIndex(memBufferCache, virtualFreePageManager, invListTypeTraits,
                invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory, btreeFileRef);
    }

    public static InMemoryInvertedIndex createPartitionedInMemoryBTreeInvertedindex(IBufferCache memBufferCache,
            IFreePageManager virtualFreePageManager, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            FileReference btreeFileRef) throws BTreeException {
        return new PartitionedInMemoryInvertedIndex(memBufferCache, virtualFreePageManager, invListTypeTraits,
                invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory, btreeFileRef);
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

    public static LSMInvertedIndex createLSMInvertedIndex(List<IVirtualBufferCache> virtualBufferCaches,
            IFileMapProvider diskFileMapProvider, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            IBufferCache diskBufferCache, String onDiskDir, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, int[] invertedIndexFields, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields, int[] filterFieldsForNonBulkLoadOps,
            int[] invertedIndexFieldsForNonBulkLoadOps, boolean durable) throws IndexException {

        BTreeFactory deletedKeysBTreeFactory = createDeletedKeysBTreeFactory(diskFileMapProvider, invListTypeTraits,
                invListCmpFactories, diskBufferCache);

        int[] bloomFilterKeyFields = new int[invListCmpFactories.length];
        for (int i = 0; i < invListCmpFactories.length; i++) {
            bloomFilterKeyFields[i] = i;
        }
        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, diskFileMapProvider,
                bloomFilterKeyFields);

        FileReference onDiskDirFileRef = new FileReference(new File(onDiskDir));
        LSMInvertedIndexFileManager fileManager = new LSMInvertedIndexFileManager(diskFileMapProvider,
                onDiskDirFileRef, deletedKeysBTreeFactory);

        IInvertedListBuilderFactory invListBuilderFactory = new FixedSizeElementInvertedListBuilderFactory(
                invListTypeTraits);
        OnDiskInvertedIndexFactory invIndexFactory = new OnDiskInvertedIndexFactory(diskBufferCache,
                diskFileMapProvider, invListBuilderFactory, invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                tokenCmpFactories, fileManager);

        LSMComponentFilterFactory filterFactory = null;
        LSMComponentFilterFrameFactory filterFrameFactory = null;
        LSMComponentFilterManager filterManager = null;
        if (filterCmpFactories != null) {
            TypeAwareTupleWriterFactory filterTupleWriterFactory = new TypeAwareTupleWriterFactory(filterTypeTraits);
            filterFactory = new LSMComponentFilterFactory(filterTupleWriterFactory, filterCmpFactories);
            filterFrameFactory = new LSMComponentFilterFrameFactory(filterTupleWriterFactory,
                    diskBufferCache.getPageSize());
            filterManager = new LSMComponentFilterManager(diskBufferCache, filterFrameFactory);
        }
        LSMInvertedIndex invIndex = new LSMInvertedIndex(virtualBufferCaches, invIndexFactory, deletedKeysBTreeFactory,
                bloomFilterFactory, filterFactory, filterFrameFactory, filterManager, bloomFilterFalsePositiveRate,
                fileManager, diskFileMapProvider, invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                tokenCmpFactories, tokenizerFactory, mergePolicy, opTracker, ioScheduler, ioOpCallback,
                invertedIndexFields, filterFields, filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps,
                durable);
        return invIndex;
    }

    public static PartitionedLSMInvertedIndex createPartitionedLSMInvertedIndex(
            List<IVirtualBufferCache> virtualBufferCaches, IFileMapProvider diskFileMapProvider,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, IBufferCache diskBufferCache, String onDiskDir,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback, int[] invertedIndexFields,
            ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps, boolean durable)
            throws IndexException {

        BTreeFactory deletedKeysBTreeFactory = createDeletedKeysBTreeFactory(diskFileMapProvider, invListTypeTraits,
                invListCmpFactories, diskBufferCache);

        int[] bloomFilterKeyFields = new int[invListCmpFactories.length];
        for (int i = 0; i < invListCmpFactories.length; i++) {
            bloomFilterKeyFields[i] = i;
        }
        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, diskFileMapProvider,
                bloomFilterKeyFields);

        FileReference onDiskDirFileRef = new FileReference(new File(onDiskDir));
        LSMInvertedIndexFileManager fileManager = new LSMInvertedIndexFileManager(diskFileMapProvider,
                onDiskDirFileRef, deletedKeysBTreeFactory);

        IInvertedListBuilderFactory invListBuilderFactory = new FixedSizeElementInvertedListBuilderFactory(
                invListTypeTraits);
        PartitionedOnDiskInvertedIndexFactory invIndexFactory = new PartitionedOnDiskInvertedIndexFactory(
                diskBufferCache, diskFileMapProvider, invListBuilderFactory, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, fileManager);

        LSMComponentFilterFactory filterFactory = null;
        LSMComponentFilterFrameFactory filterFrameFactory = null;
        LSMComponentFilterManager filterManager = null;
        if (filterCmpFactories != null) {
            TypeAwareTupleWriterFactory filterTupleWriterFactory = new TypeAwareTupleWriterFactory(filterTypeTraits);
            filterFactory = new LSMComponentFilterFactory(filterTupleWriterFactory, filterCmpFactories);
            filterFrameFactory = new LSMComponentFilterFrameFactory(filterTupleWriterFactory,
                    diskBufferCache.getPageSize());
            filterManager = new LSMComponentFilterManager(diskBufferCache, filterFrameFactory);
        }
        PartitionedLSMInvertedIndex invIndex = new PartitionedLSMInvertedIndex(virtualBufferCaches, invIndexFactory,
                deletedKeysBTreeFactory, bloomFilterFactory, filterFactory, filterFrameFactory, filterManager,
                bloomFilterFalsePositiveRate, fileManager, diskFileMapProvider, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, tokenizerFactory, mergePolicy, opTracker, ioScheduler,
                ioOpCallback, invertedIndexFields, filterFields, filterFieldsForNonBulkLoadOps,
                invertedIndexFieldsForNonBulkLoadOps, durable);
        return invIndex;
    }
}
