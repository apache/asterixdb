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
package org.apache.hyracks.storage.am.lsm.btree.impl;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.btree.impls.DiskBTree;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeFileManager;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeWithBloomFilterDiskComponentFactory;
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
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskBTreeFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.trace.ITracer;

public class TestLsmBtreeUtil {

    private TestLsmBtreeUtil() {
    }

    public static LSMBTree createLSMTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            FileReference file, IBufferCache diskBufferCache, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, int[] bloomFilterKeyFields, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            boolean needKeyDupCheck, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] btreeFields, int[] filterFields, boolean durable, IMetadataPageManagerFactory freePageManagerFactory,
            boolean updateAware, ITracer tracer) throws HyracksDataException {
        LSMBTreeTupleWriterFactory insertTupleWriterFactory =
                new LSMBTreeTupleWriterFactory(typeTraits, cmpFactories.length, false, updateAware);
        LSMBTreeTupleWriterFactory deleteTupleWriterFactory =
                new LSMBTreeTupleWriterFactory(typeTraits, cmpFactories.length, true, updateAware);
        LSMBTreeCopyTupleWriterFactory copyTupleWriterFactory =
                new LSMBTreeCopyTupleWriterFactory(typeTraits, cmpFactories.length, updateAware);
        ITreeIndexFrameFactory insertLeafFrameFactory = new BTreeNSMLeafFrameFactory(insertTupleWriterFactory);
        ITreeIndexFrameFactory copyTupleLeafFrameFactory = new BTreeNSMLeafFrameFactory(copyTupleWriterFactory);
        ITreeIndexFrameFactory deleteLeafFrameFactory = new BTreeNSMLeafFrameFactory(deleteTupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);

        TreeIndexFactory<DiskBTree> diskBTreeFactory =
                new DiskBTreeFactory(ioManager, diskBufferCache, freePageManagerFactory, interiorFrameFactory,
                        copyTupleLeafFrameFactory, cmpFactories, typeTraits.length);
        TreeIndexFactory<DiskBTree> bulkLoadBTreeFactory = new DiskBTreeFactory(ioManager, diskBufferCache,
                freePageManagerFactory, interiorFrameFactory, insertLeafFrameFactory, cmpFactories, typeTraits.length);

        ComponentFilterHelper filterHelper = null;
        LSMComponentFilterFrameFactory filterFrameFactory = null;
        LSMComponentFilterManager filterManager = null;
        if (filterCmpFactories != null) {
            TypeAwareTupleWriterFactory filterTupleWriterFactory = new TypeAwareTupleWriterFactory(filterTypeTraits);
            filterHelper = new ComponentFilterHelper(filterTupleWriterFactory, filterCmpFactories);
            filterFrameFactory = new LSMComponentFilterFrameFactory(filterTupleWriterFactory);
            filterManager = new LSMComponentFilterManager(filterFrameFactory);
        }

        boolean hasBloomFilter = bloomFilterKeyFields != null;
        //Primary and Primary Key LSMBTree index has a BloomFilter.
        ILSMIndexFileManager fileNameManager =
                new LSMBTreeFileManager(ioManager, file, diskBTreeFactory, hasBloomFilter);

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

        return new TestLsmBtree(ioManager, virtualBufferCaches, interiorFrameFactory, insertLeafFrameFactory,
                deleteLeafFrameFactory, diskBufferCache, fileNameManager, componentFactory, bulkLoadComponentFactory,
                filterHelper, filterFrameFactory, filterManager, bloomFilterFalsePositiveRate, typeTraits.length,
                cmpFactories, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory,
                needKeyDupCheck, hasBloomFilter, btreeFields, filterFields, durable, updateAware, tracer);
    }
}
