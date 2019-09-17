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

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.IPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.frames.LSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilderFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexDiskComponentFactory;
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
import org.apache.hyracks.util.trace.ITracer;

public class InvertedIndexUtils {

    public static InMemoryInvertedIndex createInMemoryBTreeInvertedindex(IBufferCache memBufferCache,
            IPageManager virtualFreePageManager, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            FileReference btreeFileRef) throws HyracksDataException {
        return new InMemoryInvertedIndex(memBufferCache, virtualFreePageManager, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, tokenizerFactory, btreeFileRef);
    }

    public static InMemoryInvertedIndex createPartitionedInMemoryBTreeInvertedindex(IBufferCache memBufferCache,
            IPageManager virtualFreePageManager, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            FileReference btreeFileRef) throws HyracksDataException {
        return new PartitionedInMemoryInvertedIndex(memBufferCache, virtualFreePageManager, invListTypeTraits,
                invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory, btreeFileRef);
    }

    public static OnDiskInvertedIndex createOnDiskInvertedIndex(IIOManager ioManager, IBufferCache bufferCache,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories, FileReference invListsFile,
            IPageManagerFactory pageManagerFactory) throws HyracksDataException {
        IInvertedListBuilder builder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        FileReference btreeFile = getBTreeFile(ioManager, invListsFile);
        return new OnDiskInvertedIndex(bufferCache, builder, invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                tokenCmpFactories, btreeFile, invListsFile, pageManagerFactory);
    }

    public static PartitionedOnDiskInvertedIndex createPartitionedOnDiskInvertedIndex(IIOManager ioManager,
            IBufferCache bufferCache, ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories, FileReference invListsFile,
            IPageManagerFactory pageManagerFactory) throws HyracksDataException {
        IInvertedListBuilder builder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        FileReference btreeFile = getBTreeFile(ioManager, invListsFile);
        return new PartitionedOnDiskInvertedIndex(bufferCache, builder, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, btreeFile, invListsFile, pageManagerFactory);
    }

    public static FileReference getBTreeFile(IIOManager ioManager, FileReference invListsFile)
            throws HyracksDataException {
        return ioManager.resolveAbsolutePath(invListsFile.getFile().getPath() + "_btree");
    }

    public static BTreeFactory createDeletedKeysBTreeFactory(IIOManager ioManager, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, IBufferCache diskBufferCache,
            IPageManagerFactory freePageManagerFactory) throws HyracksDataException {
        BTreeTypeAwareTupleWriterFactory tupleWriterFactory =
                new BTreeTypeAwareTupleWriterFactory(invListTypeTraits, false);
        ITreeIndexFrameFactory leafFrameFactory =
                BTreeUtils.getLeafFrameFactory(tupleWriterFactory, BTreeLeafFrameType.REGULAR_NSM);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        return new BTreeFactory(ioManager, diskBufferCache, freePageManagerFactory, interiorFrameFactory,
                leafFrameFactory, invListCmpFactories, invListCmpFactories.length);
    }

    public static LSMInvertedIndex createLSMInvertedIndex(IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            IBufferCache diskBufferCache, String absoluteOnDiskDir, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            int[] invertedIndexFields, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] filterFields, int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps,
            boolean durable, IMetadataPageManagerFactory pageManagerFactory, ITracer tracer)
            throws HyracksDataException {

        BTreeFactory deletedKeysBTreeFactory = createDeletedKeysBTreeFactory(ioManager, invListTypeTraits,
                invListCmpFactories, diskBufferCache, pageManagerFactory);

        int[] bloomFilterKeyFields = new int[invListCmpFactories.length];
        for (int i = 0; i < invListCmpFactories.length; i++) {
            bloomFilterKeyFields[i] = i;
        }
        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, bloomFilterKeyFields);

        FileReference onDiskDirFileRef = ioManager.resolveAbsolutePath(absoluteOnDiskDir);
        LSMInvertedIndexFileManager fileManager =
                new LSMInvertedIndexFileManager(ioManager, onDiskDirFileRef, deletedKeysBTreeFactory);

        IInvertedListBuilderFactory invListBuilderFactory =
                new FixedSizeElementInvertedListBuilderFactory(invListTypeTraits);
        OnDiskInvertedIndexFactory invIndexFactory =
                new OnDiskInvertedIndexFactory(ioManager, diskBufferCache, invListBuilderFactory, invListTypeTraits,
                        invListCmpFactories, tokenTypeTraits, tokenCmpFactories, fileManager, pageManagerFactory);

        ComponentFilterHelper filterHelper = null;
        LSMComponentFilterFrameFactory filterFrameFactory = null;
        LSMComponentFilterManager filterManager = null;
        if (filterCmpFactories != null) {
            TypeAwareTupleWriterFactory filterTupleWriterFactory = new TypeAwareTupleWriterFactory(filterTypeTraits);
            filterHelper = new ComponentFilterHelper(filterTupleWriterFactory, filterCmpFactories);
            filterFrameFactory = new LSMComponentFilterFrameFactory(filterTupleWriterFactory);
            filterManager = new LSMComponentFilterManager(filterFrameFactory);
        }
        ILSMDiskComponentFactory componentFactory = new LSMInvertedIndexDiskComponentFactory(invIndexFactory,
                deletedKeysBTreeFactory, bloomFilterFactory, filterHelper);

        return new LSMInvertedIndex(ioManager, virtualBufferCaches, componentFactory, filterHelper, filterFrameFactory,
                filterManager, bloomFilterFalsePositiveRate, diskBufferCache, fileManager, invListTypeTraits,
                invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory, mergePolicy, opTracker,
                ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, invertedIndexFields, filterFields,
                filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, durable, tracer);
    }

    public static PartitionedLSMInvertedIndex createPartitionedLSMInvertedIndex(IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            IBufferCache diskBufferCache, String absoluteOnDiskDir, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            int[] invertedIndexFields, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] filterFields, int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps,
            boolean durable, IPageManagerFactory pageManagerFactory, ITracer tracer) throws HyracksDataException {

        BTreeFactory deletedKeysBTreeFactory = createDeletedKeysBTreeFactory(ioManager, invListTypeTraits,
                invListCmpFactories, diskBufferCache, pageManagerFactory);

        int[] bloomFilterKeyFields = new int[invListCmpFactories.length];
        for (int i = 0; i < invListCmpFactories.length; i++) {
            bloomFilterKeyFields[i] = i;
        }
        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, bloomFilterKeyFields);

        FileReference onDiskDirFileRef = ioManager.resolveAbsolutePath(absoluteOnDiskDir);
        LSMInvertedIndexFileManager fileManager =
                new LSMInvertedIndexFileManager(ioManager, onDiskDirFileRef, deletedKeysBTreeFactory);

        IInvertedListBuilderFactory invListBuilderFactory =
                new FixedSizeElementInvertedListBuilderFactory(invListTypeTraits);
        PartitionedOnDiskInvertedIndexFactory invIndexFactory = new PartitionedOnDiskInvertedIndexFactory(ioManager,
                diskBufferCache, invListBuilderFactory, invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                tokenCmpFactories, fileManager, pageManagerFactory);

        ComponentFilterHelper filterHelper = null;
        LSMComponentFilterFrameFactory filterFrameFactory = null;
        LSMComponentFilterManager filterManager = null;
        if (filterCmpFactories != null) {
            TypeAwareTupleWriterFactory filterTupleWriterFactory = new TypeAwareTupleWriterFactory(filterTypeTraits);
            filterHelper = new ComponentFilterHelper(filterTupleWriterFactory, filterCmpFactories);
            filterFrameFactory = new LSMComponentFilterFrameFactory(filterTupleWriterFactory);
            filterManager = new LSMComponentFilterManager(filterFrameFactory);
        }
        ILSMDiskComponentFactory componentFactory = new LSMInvertedIndexDiskComponentFactory(invIndexFactory,
                deletedKeysBTreeFactory, bloomFilterFactory, filterHelper);

        return new PartitionedLSMInvertedIndex(ioManager, virtualBufferCaches, componentFactory, filterHelper,
                filterFrameFactory, filterManager, bloomFilterFalsePositiveRate, diskBufferCache, fileManager,
                invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory,
                mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, invertedIndexFields,
                filterFields, filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, durable, tracer);
    }
}
