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

package org.apache.hyracks.storage.am.lsm.rtree.utils;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
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
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.am.lsm.rtree.impls.ExternalRTree;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTree;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTreeDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTreeFileManager;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTreeWithAntiMatterTuples;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTreeWithAntiMatterTuplesDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTreeWithAntiMatterTuplesFileManager;
import org.apache.hyracks.storage.am.lsm.rtree.impls.RTreeFactory;
import org.apache.hyracks.storage.am.lsm.rtree.tuples.LSMRTreeCopyTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.rtree.tuples.LSMRTreeTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.rtree.tuples.LSMRTreeTupleWriterFactoryForPointMBR;
import org.apache.hyracks.storage.am.rtree.frames.RTreeFrameFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.linearize.HilbertDoubleComparatorFactory;
import org.apache.hyracks.storage.am.rtree.linearize.ZCurveDoubleComparatorFactory;
import org.apache.hyracks.storage.am.rtree.linearize.ZCurveIntComparatorFactory;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.trace.ITracer;

public class LSMRTreeUtils {
    public static LSMRTree createLSMTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            FileReference file, IBufferCache diskBufferCache, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory, ILinearizeComparatorFactory linearizeCmpFactory,
            int[] rtreeFields, int[] buddyBTreeFields, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields, boolean durable, boolean isPointMBR,
            IMetadataPageManagerFactory freePageManagerFactory) throws HyracksDataException {
        int valueFieldCount = buddyBTreeFields.length;
        int keyFieldCount = typeTraits.length - valueFieldCount;
        ITypeTraits[] btreeTypeTraits = new ITypeTraits[valueFieldCount];
        for (int i = 0; i < valueFieldCount; i++) {
            btreeTypeTraits[i] = typeTraits[buddyBTreeFields[i]];
        }
        RTreeTypeAwareTupleWriterFactory rtreeInteriorFrameTupleWriterFactory =
                new RTreeTypeAwareTupleWriterFactory(typeTraits);
        RTreeTypeAwareTupleWriterFactory rtreeLeafFrameTupleWriterFactory = null;
        if (isPointMBR) {
            rtreeLeafFrameTupleWriterFactory =
                    new LSMRTreeTupleWriterFactoryForPointMBR(typeTraits, keyFieldCount, valueFieldCount, false, false);
        } else {
            rtreeLeafFrameTupleWriterFactory = rtreeInteriorFrameTupleWriterFactory;
        }
        BTreeTypeAwareTupleWriterFactory btreeTupleWriterFactory =
                new BTreeTypeAwareTupleWriterFactory(btreeTypeTraits, false);
        RTreeFrameFactory rtreeInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(
                rtreeInteriorFrameTupleWriterFactory, valueProviderFactories, rtreePolicyType, isPointMBR);
        RTreeFrameFactory rtreeLeafFrameFactory = new RTreeNSMLeafFrameFactory(rtreeLeafFrameTupleWriterFactory,
                valueProviderFactories, rtreePolicyType, isPointMBR);
        ITreeIndexFrameFactory btreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(btreeTupleWriterFactory);
        ITreeIndexFrameFactory btreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(btreeTupleWriterFactory);
        TreeIndexFactory<RTree> diskRTreeFactory = new RTreeFactory(ioManager, diskBufferCache, freePageManagerFactory,
                rtreeInteriorFrameFactory, rtreeLeafFrameFactory, rtreeCmpFactories, typeTraits.length, isPointMBR);
        TreeIndexFactory<BTree> diskBTreeFactory = new BTreeFactory(ioManager, diskBufferCache, freePageManagerFactory,
                btreeInteriorFrameFactory, btreeLeafFrameFactory, btreeCmpFactories, btreeTypeTraits.length);

        int[] comparatorFields = { 0 };
        IBinaryComparatorFactory[] linearizerArray = { linearizeCmpFactory };

        int[] bloomFilterKeyFields = new int[btreeCmpFactories.length];
        for (int i = 0; i < btreeCmpFactories.length; i++) {
            bloomFilterKeyFields[i] = i;
        }
        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, bloomFilterKeyFields);

        ComponentFilterHelper filterHelper = null;
        LSMComponentFilterFrameFactory filterFrameFactory = null;
        LSMComponentFilterManager filterManager = null;
        if (filterCmpFactories != null) {
            TypeAwareTupleWriterFactory filterTupleWriterFactory = new TypeAwareTupleWriterFactory(filterTypeTraits);
            filterHelper = new ComponentFilterHelper(filterTupleWriterFactory, filterCmpFactories);
            filterFrameFactory = new LSMComponentFilterFrameFactory(filterTupleWriterFactory);
            filterManager = new LSMComponentFilterManager(filterFrameFactory);
        }
        ILSMIndexFileManager fileNameManager =
                new LSMRTreeFileManager(ioManager, file, diskRTreeFactory, diskBTreeFactory);
        ILSMDiskComponentFactory componentFactory =
                new LSMRTreeDiskComponentFactory(diskRTreeFactory, diskBTreeFactory, bloomFilterFactory, filterHelper);

        return new LSMRTree(ioManager, virtualBufferCaches, rtreeInteriorFrameFactory, rtreeLeafFrameFactory,
                btreeInteriorFrameFactory, btreeLeafFrameFactory, diskBufferCache, fileNameManager, componentFactory,
                filterHelper, filterFrameFactory, filterManager, bloomFilterFalsePositiveRate, typeTraits.length,
                rtreeCmpFactories, btreeCmpFactories, linearizeCmpFactory, comparatorFields, linearizerArray,
                mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, rtreeFields,
                buddyBTreeFields, filterFields, durable, isPointMBR);
    }

    public static LSMRTreeWithAntiMatterTuples createLSMTreeWithAntiMatterTuples(IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeComparatorFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            ILinearizeComparatorFactory linearizerCmpFactory, int[] rtreeFields, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields, boolean durable, boolean isPointMBR,
            IMetadataPageManagerFactory freePageManagerFactory) throws HyracksDataException {
        RTreeTypeAwareTupleWriterFactory rtreeInteriorFrameTupleWriterFactory =
                new LSMRTreeTupleWriterFactory(typeTraits, false);
        RTreeTypeAwareTupleWriterFactory rtreeLeafFrameTupleWriterFactory;
        RTreeTypeAwareTupleWriterFactory rtreeLeafFrameCopyTupleWriterFactory;
        RTreeTypeAwareTupleWriterFactory rtreeLeafFrameBulkLoadWriterFactory;
        if (isPointMBR) {
            int keyFieldCount = rtreeCmpFactories.length;
            int valueFieldCount = btreeComparatorFactories.length - keyFieldCount;
            rtreeLeafFrameTupleWriterFactory =
                    new LSMRTreeTupleWriterFactoryForPointMBR(typeTraits, keyFieldCount, valueFieldCount, true, false);
            rtreeLeafFrameCopyTupleWriterFactory =
                    new LSMRTreeTupleWriterFactoryForPointMBR(typeTraits, keyFieldCount, valueFieldCount, true, false);
            rtreeLeafFrameBulkLoadWriterFactory =
                    new LSMRTreeTupleWriterFactoryForPointMBR(typeTraits, keyFieldCount, valueFieldCount, true, false);
        } else {
            rtreeLeafFrameTupleWriterFactory = new LSMRTreeTupleWriterFactory(typeTraits, false);
            rtreeLeafFrameCopyTupleWriterFactory = new LSMRTreeCopyTupleWriterFactory(typeTraits);
            rtreeLeafFrameBulkLoadWriterFactory = new LSMRTreeTupleWriterFactory(typeTraits, false);
        }

        LSMRTreeTupleWriterFactory btreeTupleWriterFactory = new LSMRTreeTupleWriterFactory(typeTraits, true);

        RTreeFrameFactory rtreeInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(
                rtreeInteriorFrameTupleWriterFactory, valueProviderFactories, rtreePolicyType, isPointMBR);
        RTreeFrameFactory rtreeLeafFrameFactory = new RTreeNSMLeafFrameFactory(rtreeLeafFrameTupleWriterFactory,
                valueProviderFactories, rtreePolicyType, isPointMBR);
        RTreeFrameFactory rtreeLeafFrameBulkLoadFactory = new RTreeNSMLeafFrameFactory(
                rtreeLeafFrameBulkLoadWriterFactory, valueProviderFactories, rtreePolicyType, isPointMBR);

        ITreeIndexFrameFactory btreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(btreeTupleWriterFactory);
        ITreeIndexFrameFactory btreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(btreeTupleWriterFactory);

        ITreeIndexFrameFactory copyTupleLeafFrameFactory = new RTreeNSMLeafFrameFactory(
                rtreeLeafFrameCopyTupleWriterFactory, valueProviderFactories, rtreePolicyType, isPointMBR);
        TreeIndexFactory<RTree> diskRTreeFactory = new RTreeFactory(ioManager, diskBufferCache, freePageManagerFactory,
                rtreeInteriorFrameFactory, copyTupleLeafFrameFactory, rtreeCmpFactories, typeTraits.length, isPointMBR);

        TreeIndexFactory<RTree> bulkLoadRTreeFactory =
                new RTreeFactory(ioManager, diskBufferCache, freePageManagerFactory, rtreeInteriorFrameFactory,
                        rtreeLeafFrameBulkLoadFactory, rtreeCmpFactories, typeTraits.length, isPointMBR);

        // The first field is for the sorted curve (e.g. Hilbert curve), and the
        // second field is for the primary key.
        int[] comparatorFields = new int[btreeComparatorFactories.length - rtreeCmpFactories.length + 1];
        IBinaryComparatorFactory[] linearizerArray =
                new IBinaryComparatorFactory[btreeComparatorFactories.length - rtreeCmpFactories.length + 1];

        comparatorFields[0] = 0;
        for (int i = 1; i < comparatorFields.length; i++) {
            comparatorFields[i] = rtreeCmpFactories.length - 1 + i;
        }
        linearizerArray[0] = linearizerCmpFactory;
        int j = 1;
        for (int i = rtreeCmpFactories.length; i < btreeComparatorFactories.length; i++) {
            linearizerArray[j] = btreeComparatorFactories[i];
            j++;
        }

        ComponentFilterHelper filterHelper = null;
        LSMComponentFilterFrameFactory filterFrameFactory = null;
        LSMComponentFilterManager filterManager = null;
        if (filterCmpFactories != null) {
            TypeAwareTupleWriterFactory filterTupleWriterFactory = new TypeAwareTupleWriterFactory(filterTypeTraits);
            filterHelper = new ComponentFilterHelper(filterTupleWriterFactory, filterCmpFactories);
            filterFrameFactory = new LSMComponentFilterFrameFactory(filterTupleWriterFactory);
            filterManager = new LSMComponentFilterManager(filterFrameFactory);
        }
        ILSMIndexFileManager fileNameManager =
                new LSMRTreeWithAntiMatterTuplesFileManager(ioManager, file, diskRTreeFactory);

        ILSMDiskComponentFactory componentFactory =
                new LSMRTreeWithAntiMatterTuplesDiskComponentFactory(diskRTreeFactory, filterHelper);
        ILSMDiskComponentFactory bulkLoadComponentFactory =
                new LSMRTreeWithAntiMatterTuplesDiskComponentFactory(bulkLoadRTreeFactory, filterHelper);

        return new LSMRTreeWithAntiMatterTuples(ioManager, virtualBufferCaches, rtreeInteriorFrameFactory,
                rtreeLeafFrameFactory, btreeInteriorFrameFactory, btreeLeafFrameFactory, diskBufferCache,
                fileNameManager, componentFactory, bulkLoadComponentFactory, filterHelper, filterFrameFactory,
                filterManager, typeTraits.length, rtreeCmpFactories, btreeComparatorFactories, linearizerCmpFactory,
                comparatorFields, linearizerArray, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory,
                pageWriteCallbackFactory, rtreeFields, filterFields, durable, isPointMBR);
    }

    public static ExternalRTree createExternalRTree(IIOManager ioManager, FileReference file,
            IBufferCache diskBufferCache, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            ILinearizeComparatorFactory linearizeCmpFactory, int[] buddyBTreeFields, boolean durable,
            boolean isPointMBR, IMetadataPageManagerFactory freePageManagerFactory, ITracer tracer)
            throws HyracksDataException {

        int keyFieldCount = rtreeCmpFactories.length;
        int valueFieldCount = typeTraits.length - keyFieldCount;
        ITypeTraits[] btreeTypeTraits = new ITypeTraits[valueFieldCount];
        for (int i = 0; i < buddyBTreeFields.length; i++) {
            btreeTypeTraits[i] = typeTraits[buddyBTreeFields[i]];
        }
        RTreeTypeAwareTupleWriterFactory rtreeInteriorFrameTupleWriterFactory =
                new RTreeTypeAwareTupleWriterFactory(typeTraits);
        RTreeTypeAwareTupleWriterFactory rtreeLeafFrameTupleWriterFactory = null;
        if (isPointMBR) {
            rtreeLeafFrameTupleWriterFactory =
                    new LSMRTreeTupleWriterFactoryForPointMBR(typeTraits, keyFieldCount, valueFieldCount, false, false);
        } else {
            rtreeLeafFrameTupleWriterFactory = rtreeInteriorFrameTupleWriterFactory;
        }
        BTreeTypeAwareTupleWriterFactory btreeTupleWriterFactory =
                new BTreeTypeAwareTupleWriterFactory(btreeTypeTraits, false);
        RTreeFrameFactory rtreeInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(
                rtreeInteriorFrameTupleWriterFactory, valueProviderFactories, rtreePolicyType, isPointMBR);
        RTreeFrameFactory rtreeLeafFrameFactory = new RTreeNSMLeafFrameFactory(rtreeLeafFrameTupleWriterFactory,
                valueProviderFactories, rtreePolicyType, isPointMBR);
        ITreeIndexFrameFactory btreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(btreeTupleWriterFactory);
        ITreeIndexFrameFactory btreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(btreeTupleWriterFactory);
        TreeIndexFactory<RTree> diskRTreeFactory = new RTreeFactory(ioManager, diskBufferCache, freePageManagerFactory,
                rtreeInteriorFrameFactory, rtreeLeafFrameFactory, rtreeCmpFactories, typeTraits.length, isPointMBR);
        TreeIndexFactory<BTree> diskBTreeFactory = new BTreeFactory(ioManager, diskBufferCache, freePageManagerFactory,
                btreeInteriorFrameFactory, btreeLeafFrameFactory, btreeCmpFactories, btreeTypeTraits.length);
        int[] comparatorFields = { 0 };
        IBinaryComparatorFactory[] linearizerArray = { linearizeCmpFactory };

        int[] bloomFilterKeyFields = new int[btreeCmpFactories.length];
        for (int i = 0; i < btreeCmpFactories.length; i++) {
            bloomFilterKeyFields[i] = i;
        }
        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, bloomFilterKeyFields);

        ILSMIndexFileManager fileNameManager =
                new LSMRTreeFileManager(ioManager, file, diskRTreeFactory, diskBTreeFactory);
        ILSMDiskComponentFactory componentFactory =
                new LSMRTreeDiskComponentFactory(diskRTreeFactory, diskBTreeFactory, bloomFilterFactory, null);

        return new ExternalRTree(ioManager, rtreeInteriorFrameFactory, rtreeLeafFrameFactory, btreeInteriorFrameFactory,
                btreeLeafFrameFactory, diskBufferCache, fileNameManager, componentFactory, bloomFilterFalsePositiveRate,
                typeTraits.length, rtreeCmpFactories, btreeCmpFactories, linearizeCmpFactory, comparatorFields,
                linearizerArray, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory,
                buddyBTreeFields, durable, isPointMBR, tracer);
    }

    public static ILinearizeComparatorFactory proposeBestLinearizer(ITypeTraits[] typeTraits, int numKeyFields)
            throws HyracksDataException {
        for (int i = 0; i < numKeyFields; i++) {
            if (!(typeTraits[i].getClass().equals(typeTraits[0].getClass()))) {
                throw HyracksDataException.create(ErrorCode.CANNOT_PROPOSE_LINEARIZER_DIFF_DIMENSIONS);
            }
        }

        if (numKeyFields / 2 == 2 && (typeTraits[0].getClass() == DoublePointable.TYPE_TRAITS.getClass())) {
            return new HilbertDoubleComparatorFactory(2);
        } else if (typeTraits[0].getClass() == DoublePointable.TYPE_TRAITS.getClass()) {
            return new ZCurveDoubleComparatorFactory(numKeyFields / 2);
        } else if (typeTraits[0].getClass() == IntegerPointable.TYPE_TRAITS.getClass()) {
            return new ZCurveIntComparatorFactory(numKeyFields / 2);
        }

        throw HyracksDataException.create(ErrorCode.CANNOT_PROPOSE_LINEARIZER_FOR_TYPE,
                typeTraits[0].getClass().getSimpleName());
    }
}
