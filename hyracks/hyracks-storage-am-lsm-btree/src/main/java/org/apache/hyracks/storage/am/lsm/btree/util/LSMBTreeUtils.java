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

package org.apache.hyracks.storage.am.lsm.btree.util;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IFreePageManagerFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.LinkedListFreePageManagerFactory;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.ExternalBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.ExternalBTreeWithBuddy;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeFileManager;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeWithBuddyFileManager;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeCopyTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeRefrencingTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.frames.LSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class LSMBTreeUtils {
    public static LSMBTree createLSMTree(List<IVirtualBufferCache> virtualBufferCaches, FileReference file,
            IBufferCache diskBufferCache, IFileMapProvider diskFileMapProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, int[] bloomFilterKeyFields, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, boolean needKeyDupCheck, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] btreeFields, int[] filterFields, boolean durable) {
        LSMBTreeTupleWriterFactory insertTupleWriterFactory = new LSMBTreeTupleWriterFactory(typeTraits,
                cmpFactories.length, false);
        LSMBTreeTupleWriterFactory deleteTupleWriterFactory = new LSMBTreeTupleWriterFactory(typeTraits,
                cmpFactories.length, true);
        LSMBTreeCopyTupleWriterFactory copyTupleWriterFactory = new LSMBTreeCopyTupleWriterFactory(typeTraits,
                cmpFactories.length);
        ITreeIndexFrameFactory insertLeafFrameFactory = new BTreeNSMLeafFrameFactory(insertTupleWriterFactory);
        ITreeIndexFrameFactory copyTupleLeafFrameFactory = new BTreeNSMLeafFrameFactory(copyTupleWriterFactory);
        ITreeIndexFrameFactory deleteLeafFrameFactory = new BTreeNSMLeafFrameFactory(deleteTupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        IFreePageManagerFactory freePageManagerFactory = new LinkedListFreePageManagerFactory(diskBufferCache,
                metaFrameFactory);

        TreeIndexFactory<BTree> diskBTreeFactory = new BTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, interiorFrameFactory, copyTupleLeafFrameFactory, cmpFactories,
                typeTraits.length);
        TreeIndexFactory<BTree> bulkLoadBTreeFactory = new BTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, interiorFrameFactory, insertLeafFrameFactory, cmpFactories, typeTraits.length);

        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, diskFileMapProvider,
                bloomFilterKeyFields);

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

        ILSMIndexFileManager fileNameManager = new LSMBTreeFileManager(diskFileMapProvider, file, diskBTreeFactory);

        LSMBTree lsmTree = new LSMBTree(virtualBufferCaches, interiorFrameFactory, insertLeafFrameFactory,
                deleteLeafFrameFactory, fileNameManager, diskBTreeFactory, bulkLoadBTreeFactory, bloomFilterFactory,
                filterFactory, filterFrameFactory, filterManager, bloomFilterFalsePositiveRate, diskFileMapProvider,
                typeTraits.length, cmpFactories, mergePolicy, opTracker, ioScheduler, ioOpCallback, needKeyDupCheck,
                btreeFields, filterFields, durable);
        return lsmTree;
    }

    public static ExternalBTree createExternalBTree(FileReference file, IBufferCache diskBufferCache,
            IFileMapProvider diskFileMapProvider, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback,
            int startWithVersion, boolean durable) {
        LSMBTreeTupleWriterFactory insertTupleWriterFactory = new LSMBTreeTupleWriterFactory(typeTraits,
                cmpFactories.length, false);
        LSMBTreeTupleWriterFactory deleteTupleWriterFactory = new LSMBTreeTupleWriterFactory(typeTraits,
                cmpFactories.length, true);
        LSMBTreeCopyTupleWriterFactory copyTupleWriterFactory = new LSMBTreeCopyTupleWriterFactory(typeTraits,
                cmpFactories.length);
        ITreeIndexFrameFactory insertLeafFrameFactory = new BTreeNSMLeafFrameFactory(insertTupleWriterFactory);
        ITreeIndexFrameFactory copyTupleLeafFrameFactory = new BTreeNSMLeafFrameFactory(copyTupleWriterFactory);
        ITreeIndexFrameFactory deleteLeafFrameFactory = new BTreeNSMLeafFrameFactory(deleteTupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        IFreePageManagerFactory freePageManagerFactory = new LinkedListFreePageManagerFactory(diskBufferCache,
                metaFrameFactory);
        // This is the tuple writer that can do both inserts and deletes
        LSMBTreeRefrencingTupleWriterFactory referencingTupleWriterFactory = new LSMBTreeRefrencingTupleWriterFactory(
                typeTraits, cmpFactories.length, false);
        // This is the leaf frame factory for transaction components since it
        // can be used for both inserts and deletes
        ITreeIndexFrameFactory dualLeafFrameFactory = new BTreeNSMLeafFrameFactory(referencingTupleWriterFactory);

        TreeIndexFactory<BTree> diskBTreeFactory = new BTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, interiorFrameFactory, copyTupleLeafFrameFactory, cmpFactories,
                typeTraits.length);
        TreeIndexFactory<BTree> bulkLoadBTreeFactory = new BTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, interiorFrameFactory, insertLeafFrameFactory, cmpFactories, typeTraits.length);

        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, diskFileMapProvider,
                bloomFilterKeyFields);

        // This is the component factory for transactions
        TreeIndexFactory<BTree> transactionBTreeFactory = new BTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, interiorFrameFactory, dualLeafFrameFactory, cmpFactories, typeTraits.length);

        ILSMIndexFileManager fileNameManager = new LSMBTreeFileManager(diskFileMapProvider, file, diskBTreeFactory);

        // the disk only index uses an empty ArrayList for virtual buffer caches
        ExternalBTree lsmTree = new ExternalBTree(interiorFrameFactory, insertLeafFrameFactory, deleteLeafFrameFactory,
                fileNameManager, diskBTreeFactory, bulkLoadBTreeFactory, bloomFilterFactory,
                bloomFilterFalsePositiveRate, diskFileMapProvider, typeTraits.length, cmpFactories, mergePolicy,
                opTracker, ioScheduler, ioOpCallback, transactionBTreeFactory, startWithVersion, durable);
        return lsmTree;
    }

    public static ExternalBTreeWithBuddy createExternalBTreeWithBuddy(FileReference file, IBufferCache diskBufferCache,
            IFileMapProvider diskFileMapProvider, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback, int[] buddyBTreeFields,
            int startWithVersion, boolean durable) {

        ITypeTraits[] buddyBtreeTypeTraits = new ITypeTraits[buddyBTreeFields.length];
        IBinaryComparatorFactory[] buddyBtreeCmpFactories = new IBinaryComparatorFactory[buddyBTreeFields.length];
        for (int i = 0; i < buddyBtreeTypeTraits.length; i++) {
            buddyBtreeTypeTraits[i] = typeTraits[buddyBTreeFields[i]];
            buddyBtreeCmpFactories[i] = cmpFactories[buddyBTreeFields[i]];
        }

        TypeAwareTupleWriterFactory buddyBtreeTupleWriterFactory = new TypeAwareTupleWriterFactory(buddyBtreeTypeTraits);
        ITreeIndexFrameFactory buddyBtreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(
                buddyBtreeTupleWriterFactory);
        ITreeIndexFrameFactory buddyBtreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(buddyBtreeTupleWriterFactory);

        LSMBTreeTupleWriterFactory insertTupleWriterFactory = new LSMBTreeTupleWriterFactory(typeTraits,
                cmpFactories.length, false);
        LSMBTreeCopyTupleWriterFactory copyTupleWriterFactory = new LSMBTreeCopyTupleWriterFactory(typeTraits,
                cmpFactories.length);
        ITreeIndexFrameFactory insertLeafFrameFactory = new BTreeNSMLeafFrameFactory(insertTupleWriterFactory);
        ITreeIndexFrameFactory copyTupleLeafFrameFactory = new BTreeNSMLeafFrameFactory(copyTupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        IFreePageManagerFactory freePageManagerFactory = new LinkedListFreePageManagerFactory(diskBufferCache,
                metaFrameFactory);

        TreeIndexFactory<BTree> diskBTreeFactory = new BTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, interiorFrameFactory, copyTupleLeafFrameFactory, cmpFactories,
                typeTraits.length);

        TreeIndexFactory<BTree> bulkLoadBTreeFactory = new BTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, interiorFrameFactory, insertLeafFrameFactory, cmpFactories, typeTraits.length);

        int[] bloomFilterKeyFields = new int[buddyBtreeCmpFactories.length];
        for (int i = 0; i < buddyBtreeCmpFactories.length; i++) {
            bloomFilterKeyFields[i] = i;
        }
        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, diskFileMapProvider,
                bloomFilterKeyFields);

        // buddy b-tree factory
        TreeIndexFactory<BTree> diskBuddyBTreeFactory = new BTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, buddyBtreeInteriorFrameFactory, buddyBtreeLeafFrameFactory,
                buddyBtreeCmpFactories, buddyBtreeTypeTraits.length);

        ILSMIndexFileManager fileNameManager = new LSMBTreeWithBuddyFileManager(diskFileMapProvider, file,
                diskBTreeFactory, diskBuddyBTreeFactory);

        // the disk only index uses an empty ArrayList for virtual buffer caches
        ExternalBTreeWithBuddy lsmTree = new ExternalBTreeWithBuddy(interiorFrameFactory, insertLeafFrameFactory,
                buddyBtreeLeafFrameFactory, diskBufferCache, fileNameManager, bulkLoadBTreeFactory, diskBTreeFactory,
                diskBuddyBTreeFactory, bloomFilterFactory, diskFileMapProvider, bloomFilterFalsePositiveRate,
                mergePolicy, opTracker, ioScheduler, ioOpCallback, cmpFactories, buddyBtreeCmpFactories,
                buddyBTreeFields, startWithVersion, durable);
        return lsmTree;
    }
}
