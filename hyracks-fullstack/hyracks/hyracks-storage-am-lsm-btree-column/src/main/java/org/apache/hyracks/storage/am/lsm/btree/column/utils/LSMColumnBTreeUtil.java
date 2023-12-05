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
package org.apache.hyracks.storage.am.lsm.btree.column.utils;

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
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeLeafFrameFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTreeWithBloomFilterDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeFileManager;
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
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.trace.ITracer;

public class LSMColumnBTreeUtil {

    public static LSMBTree createLSMTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            FileReference file, IBufferCache diskBufferCache, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, int[] bloomFilterKeyFields, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            int[] btreeFields, IMetadataPageManagerFactory freePageManagerFactory, boolean updateAware, ITracer tracer,
            ICompressorDecompressorFactory compressorDecompressorFactory, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector, IColumnManagerFactory columnManagerFactory, boolean atomic)
            throws HyracksDataException {

        //Tuple writers
        LSMBTreeTupleWriterFactory insertTupleWriterFactory = new LSMBTreeTupleWriterFactory(typeTraits,
                cmpFactories.length, false, updateAware, nullTypeTraits, nullIntrospector);
        LSMBTreeTupleWriterFactory deleteTupleWriterFactory = new LSMBTreeTupleWriterFactory(typeTraits,
                cmpFactories.length, true, updateAware, nullTypeTraits, nullIntrospector);
        LSMBTreeCopyTupleWriterFactory copyTupleWriterFactory = new LSMBTreeCopyTupleWriterFactory(typeTraits,
                cmpFactories.length, updateAware, nullTypeTraits, nullIntrospector);
        LSMBTreeTupleWriterFactory bulkLoadTupleWriterFactory = new LSMBTreeTupleWriterFactory(typeTraits,
                cmpFactories.length, false, updateAware, nullTypeTraits, nullIntrospector);

        //Leaf frames
        ITreeIndexFrameFactory flushLeafFrameFactory = new ColumnBTreeLeafFrameFactory(copyTupleWriterFactory,
                columnManagerFactory.getFlushColumnTupleReaderWriterFactory());
        ITreeIndexFrameFactory mergeLeafFrameFactory = new ColumnBTreeLeafFrameFactory(copyTupleWriterFactory,
                columnManagerFactory.createMergeColumnTupleReaderWriterFactory());
        ITreeIndexFrameFactory bulkLoadLeafFrameFactory = new ColumnBTreeLeafFrameFactory(bulkLoadTupleWriterFactory,
                columnManagerFactory.getLoadColumnTupleReaderWriterFactory(cmpFactories));
        ITreeIndexFrameFactory insertLeafFrameFactory = new BTreeNSMLeafFrameFactory(insertTupleWriterFactory);
        ITreeIndexFrameFactory deleteLeafFrameFactory = new BTreeNSMLeafFrameFactory(deleteTupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);

        //BTree factory
        TreeIndexFactory<ColumnBTree> flushBTreeFactory = new ColumnBTreeFactory(ioManager, diskBufferCache,
                freePageManagerFactory, interiorFrameFactory, flushLeafFrameFactory, cmpFactories, typeTraits.length);
        TreeIndexFactory<ColumnBTree> mergeBTreeFactory = new ColumnBTreeFactory(ioManager, diskBufferCache,
                freePageManagerFactory, interiorFrameFactory, mergeLeafFrameFactory, cmpFactories, typeTraits.length);
        TreeIndexFactory<ColumnBTree> bulkloadBTreeFactory =
                new ColumnBTreeFactory(ioManager, diskBufferCache, freePageManagerFactory, interiorFrameFactory,
                        bulkLoadLeafFrameFactory, cmpFactories, typeTraits.length);

        ILSMIndexFileManager fileNameManager =
                new LSMBTreeFileManager(ioManager, file, flushBTreeFactory, true, compressorDecompressorFactory);

        BloomFilterFactory bloomFilterFactory = new BloomFilterFactory(diskBufferCache, bloomFilterKeyFields);
        ILSMDiskComponentFactory flushComponentFactory =
                new LSMColumnBTreeWithBloomFilterDiskComponentFactory(flushBTreeFactory, bloomFilterFactory);
        ILSMDiskComponentFactory mergeComponentFactory =
                new LSMColumnBTreeWithBloomFilterDiskComponentFactory(mergeBTreeFactory, bloomFilterFactory);
        ILSMDiskComponentFactory bulkLoadComponentFactory =
                new LSMColumnBTreeWithBloomFilterDiskComponentFactory(bulkloadBTreeFactory, bloomFilterFactory);

        return new LSMColumnBTree(ioManager, virtualBufferCaches, interiorFrameFactory, insertLeafFrameFactory,
                deleteLeafFrameFactory, diskBufferCache, fileNameManager, flushComponentFactory, mergeComponentFactory,
                bulkLoadComponentFactory, bloomFilterFalsePositiveRate, typeTraits.length, cmpFactories, mergePolicy,
                opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, btreeFields, tracer,
                columnManagerFactory.createColumnManager(), atomic);
    }
}
