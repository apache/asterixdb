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

package org.apache.hyracks.storage.am.lsm.btree.utils;

import java.util.List;

import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.DiskBTree;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.ExternalBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.ExternalBTreeWithBuddy;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeFileManager;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeWithBloomFilterDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeWithBuddyDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeWithBuddyFileManager;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeCopyTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.frames.LSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskBTreeFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.trace.ITracer;

public class LSMBTreeUtil {

    private LSMBTreeUtil() {
    }

    public static LSMBTree createLSMTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            FileReference file, IBufferCache diskBufferCache, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, int[] bloomFilterKeyFields, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            boolean needKeyDupCheck, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] btreeFields, int[] filterFields, boolean durable, IMetadataPageManagerFactory freePageManagerFactory,
            boolean updateAware, ITracer tracer, ICompressorDecompressorFactory compressorDecompressorFactory,
            boolean hasBloomFilter) throws HyracksDataException {
        LSMBTreeTupleWriterFactory insertTupleWriterFactory =
                new LSMBTreeTupleWriterFactory(typeTraits, cmpFactories.length, false, updateAware);
        LSMBTreeTupleWriterFactory deleteTupleWriterFactory =
                new LSMBTreeTupleWriterFactory(typeTraits, cmpFactories.length, true, updateAware);
        LSMBTreeCopyTupleWriterFactory copyTupleWriterFactory =
                new LSMBTreeCopyTupleWriterFactory(typeTraits, cmpFactories.length, updateAware);
        LSMBTreeTupleWriterFactory bulkLoadTupleWriterFactory =
                new LSMBTreeTupleWriterFactory(typeTraits, cmpFactories.length, false, updateAware);

        ITreeIndexFrameFactory insertLeafFrameFactory = new BTreeNSMLeafFrameFactory(insertTupleWriterFactory);
        ITreeIndexFrameFactory copyTupleLeafFrameFactory = new BTreeNSMLeafFrameFactory(copyTupleWriterFactory);
        ITreeIndexFrameFactory deleteLeafFrameFactory = new BTreeNSMLeafFrameFactory(deleteTupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);
        ITreeIndexFrameFactory bulkLoadLeafFrameFactory = new BTreeNSMLeafFrameFactory(bulkLoadTupleWriterFactory);

        TreeIndexFactory<DiskBTree> diskBTreeFactory =
                new DiskBTreeFactory(ioManager, diskBufferCache, freePageManagerFactory, interiorFrameFactory,
                        copyTupleLeafFrameFactory, cmpFactories, typeTraits.length);
        TreeIndexFactory<DiskBTree> bulkLoadBTreeFactory =
                new DiskBTreeFactory(ioManager, diskBufferCache, freePageManagerFactory, interiorFrameFactory,
                        bulkLoadLeafFrameFactory, cmpFactories, typeTraits.length);

        ComponentFilterHelper filterHelper = null;
        LSMComponentFilterFrameFactory filterFrameFactory = null;
        LSMComponentFilterManager filterManager = null;
        if (filterCmpFactories != null) {
            TypeAwareTupleWriterFactory filterTupleWriterFactory = new TypeAwareTupleWriterFactory(filterTypeTraits);
            filterHelper = new ComponentFilterHelper(filterTupleWriterFactory, filterCmpFactories);
            filterFrameFactory = new LSMComponentFilterFrameFactory(filterTupleWriterFactory);
            filterManager = new LSMComponentFilterManager(filterFrameFactory);
        }
        ILSMIndexFileManager fileNameManager = new LSMBTreeFileManager(ioManager, file, diskBTreeFactory,
                hasBloomFilter, compressorDecompressorFactory);

        ILSMDiskComponentFactory componentFactory;
        ILSMDiskComponentFactory bulkLoadComponentFactory;
        if (hasBloomFilter) {
            BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, bloomFilterKeyFields);
            componentFactory =
                    new LSMBTreeWithBloomFilterDiskComponentFactory(diskBTreeFactory, bloomFilterFactory, filterHelper);
            bulkLoadComponentFactory = new LSMBTreeWithBloomFilterDiskComponentFactory(bulkLoadBTreeFactory,
                    bloomFilterFactory, filterHelper);
        } else {
            componentFactory = new LSMBTreeDiskComponentFactory(diskBTreeFactory, filterHelper);
            bulkLoadComponentFactory = new LSMBTreeDiskComponentFactory(bulkLoadBTreeFactory, filterHelper);
        }

        return new LSMBTree(ioManager, virtualBufferCaches, interiorFrameFactory, insertLeafFrameFactory,
                deleteLeafFrameFactory, diskBufferCache, fileNameManager, componentFactory, bulkLoadComponentFactory,
                filterHelper, filterFrameFactory, filterManager, bloomFilterFalsePositiveRate, typeTraits.length,
                cmpFactories, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory,
                needKeyDupCheck, hasBloomFilter, btreeFields, filterFields, durable, updateAware, tracer);
    }

    public static ExternalBTree createExternalBTree(IIOManager ioManager, FileReference file,
            IBufferCache diskBufferCache, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            boolean durable, IMetadataPageManagerFactory freePageManagerFactory, ITracer tracer)
            throws HyracksDataException {
        LSMBTreeTupleWriterFactory insertTupleWriterFactory =
                new LSMBTreeTupleWriterFactory(typeTraits, cmpFactories.length, false, false);
        LSMBTreeTupleWriterFactory deleteTupleWriterFactory =
                new LSMBTreeTupleWriterFactory(typeTraits, cmpFactories.length, true, false);
        LSMBTreeCopyTupleWriterFactory copyTupleWriterFactory =
                new LSMBTreeCopyTupleWriterFactory(typeTraits, cmpFactories.length, false);
        ITreeIndexFrameFactory insertLeafFrameFactory = new BTreeNSMLeafFrameFactory(insertTupleWriterFactory);
        ITreeIndexFrameFactory copyTupleLeafFrameFactory = new BTreeNSMLeafFrameFactory(copyTupleWriterFactory);
        ITreeIndexFrameFactory deleteLeafFrameFactory = new BTreeNSMLeafFrameFactory(deleteTupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);
        // This is the tuple writer that can do both inserts and deletes
        LSMBTreeTupleWriterFactory transactionTupleWriterFactory =
                new LSMBTreeTupleWriterFactory(typeTraits, cmpFactories.length, false, false);
        // This is the leaf frame factory for transaction components since it
        // can be used for both inserts and deletes
        ITreeIndexFrameFactory transactionLeafFrameFactory =
                new BTreeNSMLeafFrameFactory(transactionTupleWriterFactory);
        TreeIndexFactory<DiskBTree> diskBTreeFactory =
                new DiskBTreeFactory(ioManager, diskBufferCache, freePageManagerFactory, interiorFrameFactory,
                        copyTupleLeafFrameFactory, cmpFactories, typeTraits.length);
        TreeIndexFactory<DiskBTree> bulkLoadBTreeFactory = new DiskBTreeFactory(ioManager, diskBufferCache,
                freePageManagerFactory, interiorFrameFactory, insertLeafFrameFactory, cmpFactories, typeTraits.length);
        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, bloomFilterKeyFields);
        // This is the component factory for transactions
        TreeIndexFactory<DiskBTree> transactionBTreeFactory =
                new DiskBTreeFactory(ioManager, diskBufferCache, freePageManagerFactory, interiorFrameFactory,
                        transactionLeafFrameFactory, cmpFactories, typeTraits.length);
        //TODO remove BloomFilter from external dataset's secondary LSMBTree index
        ILSMIndexFileManager fileNameManager = new LSMBTreeFileManager(ioManager, file, diskBTreeFactory, true);
        ILSMDiskComponentFactory componentFactory =
                new LSMBTreeWithBloomFilterDiskComponentFactory(diskBTreeFactory, bloomFilterFactory, null);
        ILSMDiskComponentFactory bulkLoadComponentFactory =
                new LSMBTreeWithBloomFilterDiskComponentFactory(bulkLoadBTreeFactory, bloomFilterFactory, null);
        ILSMDiskComponentFactory transactionComponentFactory =
                new LSMBTreeWithBloomFilterDiskComponentFactory(transactionBTreeFactory, bloomFilterFactory, null);
        // the disk only index uses an empty ArrayList for virtual buffer caches
        return new ExternalBTree(ioManager, interiorFrameFactory, insertLeafFrameFactory, deleteLeafFrameFactory,
                diskBufferCache, fileNameManager, componentFactory, bulkLoadComponentFactory,
                transactionComponentFactory, bloomFilterFalsePositiveRate, cmpFactories, mergePolicy, opTracker,
                ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, durable, tracer);
    }

    public static ExternalBTreeWithBuddy createExternalBTreeWithBuddy(IIOManager ioManager, FileReference file,
            IBufferCache diskBufferCache, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory, int[] buddyBTreeFields, boolean durable,
            IMetadataPageManagerFactory freePageManagerFactory, ITracer tracer) throws HyracksDataException {
        ITypeTraits[] buddyBtreeTypeTraits = new ITypeTraits[buddyBTreeFields.length];
        IBinaryComparatorFactory[] buddyBtreeCmpFactories = new IBinaryComparatorFactory[buddyBTreeFields.length];
        for (int i = 0; i < buddyBtreeTypeTraits.length; i++) {
            buddyBtreeTypeTraits[i] = typeTraits[buddyBTreeFields[i]];
            buddyBtreeCmpFactories[i] = cmpFactories[buddyBTreeFields[i]];
        }
        BTreeTypeAwareTupleWriterFactory buddyBtreeTupleWriterFactory =
                new BTreeTypeAwareTupleWriterFactory(buddyBtreeTypeTraits, false);
        ITreeIndexFrameFactory buddyBtreeInteriorFrameFactory =
                new BTreeNSMInteriorFrameFactory(buddyBtreeTupleWriterFactory);
        ITreeIndexFrameFactory buddyBtreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(buddyBtreeTupleWriterFactory);

        LSMBTreeTupleWriterFactory insertTupleWriterFactory =
                new LSMBTreeTupleWriterFactory(typeTraits, cmpFactories.length, false, false);
        LSMBTreeCopyTupleWriterFactory copyTupleWriterFactory =
                new LSMBTreeCopyTupleWriterFactory(typeTraits, cmpFactories.length, false);
        ITreeIndexFrameFactory insertLeafFrameFactory = new BTreeNSMLeafFrameFactory(insertTupleWriterFactory);
        ITreeIndexFrameFactory copyTupleLeafFrameFactory = new BTreeNSMLeafFrameFactory(copyTupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);
        TreeIndexFactory<BTree> diskBTreeFactory = new BTreeFactory(ioManager, diskBufferCache, freePageManagerFactory,
                interiorFrameFactory, copyTupleLeafFrameFactory, cmpFactories, typeTraits.length);

        TreeIndexFactory<BTree> bulkLoadBTreeFactory = new BTreeFactory(ioManager, diskBufferCache,
                freePageManagerFactory, interiorFrameFactory, insertLeafFrameFactory, cmpFactories, typeTraits.length);

        int[] bloomFilterKeyFields = new int[buddyBtreeCmpFactories.length];
        for (int i = 0; i < buddyBtreeCmpFactories.length; i++) {
            bloomFilterKeyFields[i] = i;
        }
        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, bloomFilterKeyFields);

        // buddy b-tree factory
        TreeIndexFactory<BTree> diskBuddyBTreeFactory =
                new BTreeFactory(ioManager, diskBufferCache, freePageManagerFactory, buddyBtreeInteriorFrameFactory,
                        buddyBtreeLeafFrameFactory, buddyBtreeCmpFactories, buddyBtreeTypeTraits.length);

        ILSMIndexFileManager fileNameManager =
                new LSMBTreeWithBuddyFileManager(ioManager, file, diskBTreeFactory, diskBuddyBTreeFactory);

        ILSMDiskComponentFactory componentFactory = new LSMBTreeWithBuddyDiskComponentFactory(diskBTreeFactory,
                diskBuddyBTreeFactory, bloomFilterFactory, null);
        ILSMDiskComponentFactory bulkLoadComponentFactory = new LSMBTreeWithBuddyDiskComponentFactory(
                bulkLoadBTreeFactory, diskBuddyBTreeFactory, bloomFilterFactory, null);

        // the disk only index uses an empty ArrayList for virtual buffer caches
        return new ExternalBTreeWithBuddy(ioManager, interiorFrameFactory, insertLeafFrameFactory,
                buddyBtreeLeafFrameFactory, diskBufferCache, fileNameManager, componentFactory,
                bulkLoadComponentFactory, bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler,
                ioOpCallbackFactory, pageWriteCallbackFactory, cmpFactories, buddyBtreeCmpFactories, buddyBTreeFields,
                durable, tracer);
    }
}
