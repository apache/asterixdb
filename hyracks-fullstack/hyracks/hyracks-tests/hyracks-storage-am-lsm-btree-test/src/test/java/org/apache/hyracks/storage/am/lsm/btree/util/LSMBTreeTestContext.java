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

import java.util.Collection;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestContext;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.utils.LSMBTreeUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.util.trace.ITraceCategoryRegistry;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.hyracks.util.trace.TraceCategoryRegistry;
import org.apache.hyracks.util.trace.Tracer;

@SuppressWarnings("rawtypes")
public final class LSMBTreeTestContext extends OrderedIndexTestContext {

    public LSMBTreeTestContext(ISerializerDeserializer[] fieldSerdes, ITreeIndex treeIndex, boolean filtered)
            throws HyracksDataException {
        super(fieldSerdes, treeIndex, filtered);
    }

    @Override
    public int getKeyFieldCount() {
        LSMBTree lsmTree = (LSMBTree) index;
        return lsmTree.getComparatorFactories().length;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        LSMBTree lsmTree = (LSMBTree) index;
        return lsmTree.getComparatorFactories();
    }

    /**
     * Override to provide upsert semantics for the check tuples.
     */
    @Override
    public void insertCheckTuple(CheckTuple checkTuple, Collection<CheckTuple> checkTuples) {
        upsertCheckTuple(checkTuple, checkTuples);
    }

    public static LSMBTreeTestContext create(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            FileReference file, IBufferCache diskBufferCache, ISerializerDeserializer[] fieldSerdes, int numKeyFields,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, boolean filtered, boolean needKeyDupCheck,
            boolean updateAware) throws HyracksDataException {
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] cmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeyFields);
        int[] bloomFilterKeyFields = new int[numKeyFields];
        for (int i = 0; i < numKeyFields; ++i) {
            bloomFilterKeyFields[i] = i;
        }
        LSMBTree lsmTree;
        if (filtered) {
            ITypeTraits[] filterTypeTraits = new ITypeTraits[1];
            filterTypeTraits[0] = typeTraits[0];
            int[] btreefields = new int[typeTraits.length];
            for (int i = 0; i < btreefields.length; i++) {
                btreefields[i] = i;
            }
            int[] filterfields = { btreefields.length };
            IBinaryComparatorFactory[] filterCmp = { cmpFactories[0] };
            lsmTree = LSMBTreeUtil.createLSMTree(ioManager, virtualBufferCaches, file, diskBufferCache, typeTraits,
                    cmpFactories, bloomFilterKeyFields, bloomFilterFalsePositiveRate, mergePolicy, opTracker,
                    ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, needKeyDupCheck, filterTypeTraits,
                    filterCmp, btreefields, filterfields, true, metadataPageManagerFactory, updateAware, ITracer.NONE,
                    NoOpCompressorDecompressorFactory.INSTANCE);
        } else {
            lsmTree = LSMBTreeUtil.createLSMTree(ioManager, virtualBufferCaches, file, diskBufferCache, typeTraits,
                    cmpFactories, bloomFilterKeyFields, bloomFilterFalsePositiveRate, mergePolicy, opTracker,
                    ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, needKeyDupCheck, null, null, null, null,
                    true, metadataPageManagerFactory,
                    updateAware, new Tracer(LSMBTreeTestContext.class.getSimpleName(),
                            ITraceCategoryRegistry.CATEGORIES_ALL, new TraceCategoryRegistry()),
                    NoOpCompressorDecompressorFactory.INSTANCE);
        }
        LSMBTreeTestContext testCtx = new LSMBTreeTestContext(fieldSerdes, lsmTree, filtered);
        return testCtx;
    }
}
