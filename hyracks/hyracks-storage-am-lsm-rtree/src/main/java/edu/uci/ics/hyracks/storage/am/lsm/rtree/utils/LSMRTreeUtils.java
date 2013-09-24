/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.utils;

import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTree;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTreeFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTreeWithAntiMatterTuples;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTreeWithAntiMatterTuplesFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.RTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.tuples.LSMRTreeCopyTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.tuples.LSMRTreeTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.tuples.LSMTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.linearize.HilbertDoubleComparatorFactory;
import edu.uci.ics.hyracks.storage.am.rtree.linearize.ZCurveDoubleComparatorFactory;
import edu.uci.ics.hyracks.storage.am.rtree.linearize.ZCurveIntComparatorFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMRTreeUtils {
    public static LSMRTree createLSMTree(List<IVirtualBufferCache> virtualBufferCaches, FileReference file,
            IBufferCache diskBufferCache, IFileMapProvider diskFileMapProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback,
            ILinearizeComparatorFactory linearizeCmpFactory) throws TreeIndexException {
        LSMTypeAwareTupleWriterFactory rtreeTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, false);
        LSMTypeAwareTupleWriterFactory btreeTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, true);

        ITreeIndexFrameFactory rtreeInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories, rtreePolicyType);
        ITreeIndexFrameFactory rtreeLeafFrameFactory = new RTreeNSMLeafFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories, rtreePolicyType);

        ITreeIndexFrameFactory btreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(btreeTupleWriterFactory);
        ITreeIndexFrameFactory btreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(btreeTupleWriterFactory);

        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        LinkedListFreePageManagerFactory freePageManagerFactory = new LinkedListFreePageManagerFactory(diskBufferCache,
                metaFrameFactory);

        TreeIndexFactory<RTree> diskRTreeFactory = new RTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, rtreeInteriorFrameFactory, rtreeLeafFrameFactory, rtreeCmpFactories,
                typeTraits.length);
        TreeIndexFactory<BTree> diskBTreeFactory = new BTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, btreeInteriorFrameFactory, btreeLeafFrameFactory, btreeCmpFactories,
                typeTraits.length);

        int[] comparatorFields = { 0 };
        IBinaryComparatorFactory[] linearizerArray = { linearizeCmpFactory };

        int[] bloomFilterKeyFields = new int[btreeCmpFactories.length];
        for (int i = 0; i < btreeCmpFactories.length; i++) {
            bloomFilterKeyFields[i] = i;
        }
        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, diskFileMapProvider,
                bloomFilterKeyFields);

        ILSMIndexFileManager fileNameManager = new LSMRTreeFileManager(diskFileMapProvider, file, diskRTreeFactory,
                diskBTreeFactory);
        LSMRTree lsmTree = new LSMRTree(virtualBufferCaches, rtreeInteriorFrameFactory, rtreeLeafFrameFactory,
                btreeInteriorFrameFactory, btreeLeafFrameFactory, fileNameManager, diskRTreeFactory, diskBTreeFactory,
                bloomFilterFactory, bloomFilterFalsePositiveRate, diskFileMapProvider, typeTraits.length,
                rtreeCmpFactories, btreeCmpFactories, linearizeCmpFactory, comparatorFields, linearizerArray,
                mergePolicy, opTracker, ioScheduler, ioOpCallback);
        return lsmTree;
    }

    public static LSMRTreeWithAntiMatterTuples createLSMTreeWithAntiMatterTuples(
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            IFileMapProvider diskFileMapProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, ILinearizeComparatorFactory linearizerCmpFactory)
            throws TreeIndexException {
        LSMRTreeTupleWriterFactory rtreeTupleWriterFactory = new LSMRTreeTupleWriterFactory(typeTraits, false);
        LSMRTreeTupleWriterFactory btreeTupleWriterFactory = new LSMRTreeTupleWriterFactory(typeTraits, true);

        LSMRTreeCopyTupleWriterFactory copyTupleWriterFactory = new LSMRTreeCopyTupleWriterFactory(typeTraits);

        ITreeIndexFrameFactory rtreeInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories, rtreePolicyType);
        ITreeIndexFrameFactory rtreeLeafFrameFactory = new RTreeNSMLeafFrameFactory(rtreeTupleWriterFactory,
                valueProviderFactories, rtreePolicyType);

        ITreeIndexFrameFactory btreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(btreeTupleWriterFactory);
        ITreeIndexFrameFactory btreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(btreeTupleWriterFactory);

        ITreeIndexFrameFactory copyTupleLeafFrameFactory = new RTreeNSMLeafFrameFactory(copyTupleWriterFactory,
                valueProviderFactories, rtreePolicyType);

        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        LinkedListFreePageManagerFactory freePageManagerFactory = new LinkedListFreePageManagerFactory(diskBufferCache,
                metaFrameFactory);

        TreeIndexFactory<RTree> diskRTreeFactory = new RTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, rtreeInteriorFrameFactory, copyTupleLeafFrameFactory, rtreeCmpFactories,
                typeTraits.length);

        TreeIndexFactory<RTree> bulkLoadRTreeFactory = new RTreeFactory(diskBufferCache, diskFileMapProvider,
                freePageManagerFactory, rtreeInteriorFrameFactory, rtreeLeafFrameFactory, rtreeCmpFactories,
                typeTraits.length);

        // The first field is for the sorted curve (e.g. Hilbert curve), and the
        // second field is for the primary key.
        int[] comparatorFields = { 0, btreeCmpFactories.length - 1 };
        IBinaryComparatorFactory[] linearizerArray = { linearizerCmpFactory,
                btreeCmpFactories[btreeCmpFactories.length - 1] };

        ILSMIndexFileManager fileNameManager = new LSMRTreeWithAntiMatterTuplesFileManager(diskFileMapProvider, file,
                diskRTreeFactory);
        LSMRTreeWithAntiMatterTuples lsmTree = new LSMRTreeWithAntiMatterTuples(virtualBufferCaches,
                rtreeInteriorFrameFactory, rtreeLeafFrameFactory, btreeInteriorFrameFactory, btreeLeafFrameFactory,
                fileNameManager, diskRTreeFactory, bulkLoadRTreeFactory, diskFileMapProvider, typeTraits.length,
                rtreeCmpFactories, btreeCmpFactories, linearizerCmpFactory, comparatorFields, linearizerArray,
                mergePolicy, opTracker, ioScheduler, ioOpCallback);
        return lsmTree;
    }

    public static ILinearizeComparatorFactory proposeBestLinearizer(ITypeTraits[] typeTraits, int numKeyFields)
            throws TreeIndexException {
        for (int i = 0; i < numKeyFields; i++) {
            if (!(typeTraits[i].getClass().equals(typeTraits[0].getClass()))) {
                throw new TreeIndexException("Cannot propose linearizer if dimensions have different types");
            }
        }

        if (numKeyFields / 2 == 2 && (typeTraits[0].getClass() == DoublePointable.TYPE_TRAITS.getClass())) {
            return new HilbertDoubleComparatorFactory(2);
        } else if (typeTraits[0].getClass() == DoublePointable.TYPE_TRAITS.getClass()) {
            return new ZCurveDoubleComparatorFactory(numKeyFields / 2);
        } else if (typeTraits[0].getClass() == IntegerPointable.TYPE_TRAITS.getClass()) {
            return new ZCurveIntComparatorFactory(numKeyFields / 2);
        }

        throw new TreeIndexException("Cannot propose linearizer");
    }
}
