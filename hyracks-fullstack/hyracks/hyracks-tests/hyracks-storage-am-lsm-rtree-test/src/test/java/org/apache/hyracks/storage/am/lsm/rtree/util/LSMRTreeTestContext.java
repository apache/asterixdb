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

package org.apache.hyracks.storage.am.lsm.rtree.util;

import java.util.Collection;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTree;
import org.apache.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import org.apache.hyracks.storage.am.rtree.AbstractRTreeTestContext;
import org.apache.hyracks.storage.am.rtree.RTreeCheckTuple;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

@SuppressWarnings("rawtypes")
public final class LSMRTreeTestContext extends AbstractRTreeTestContext {

    public LSMRTreeTestContext(ISerializerDeserializer[] fieldSerdes, ITreeIndex treeIndex)
            throws HyracksDataException {
        super(fieldSerdes, treeIndex);
    }

    @Override
    public int getKeyFieldCount() {
        LSMRTree lsmTree = (LSMRTree) index;
        return lsmTree.getComparatorFactories().length;
    }

    /**
     * Override to provide delete semantics for the check tuples.
     */
    @Override
    public void deleteCheckTuple(RTreeCheckTuple checkTuple, Collection<RTreeCheckTuple> checkTuples) {
        while (checkTuples.remove(checkTuple)) {
        }
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        LSMRTree lsmTree = (LSMRTree) index;
        return lsmTree.getComparatorFactories();
    }

    public static LSMRTreeTestContext create(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            FileReference file, IBufferCache diskBufferCache, ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeyFields, RTreePolicyType rtreePolicyType,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory) throws Exception {
        return create(ioManager, virtualBufferCaches, file, diskBufferCache, fieldSerdes, valueProviderFactories,
                numKeyFields, rtreePolicyType, bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler,
                ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory, false);
    }

    public static LSMRTreeTestContext create(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            FileReference file, IBufferCache diskBufferCache, ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeyFields, RTreePolicyType rtreePolicyType,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, boolean isPointMBR) throws Exception {
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] rtreeCmpFactories =
                SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeyFields);
        int numBtreeFields = fieldSerdes.length - numKeyFields;
        ISerializerDeserializer[] btreeFieldSerdes = new ISerializerDeserializer[numBtreeFields];
        int[] btreeFields = new int[numBtreeFields];
        for (int i = 0; i < numBtreeFields; i++) {
            btreeFields[i] = numKeyFields + i;
            btreeFieldSerdes[i] = fieldSerdes[numKeyFields + i];
        }
        IBinaryComparatorFactory[] btreeCmpFactories =
                SerdeUtils.serdesToComparatorFactories(btreeFieldSerdes, numBtreeFields);
        LSMRTree lsmTree = LSMRTreeUtils.createLSMTree(ioManager, virtualBufferCaches, file, diskBufferCache,
                typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories, rtreePolicyType,
                bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory,
                pageWriteCallbackFactory, LSMRTreeUtils.proposeBestLinearizer(typeTraits, rtreeCmpFactories.length),
                null, btreeFields, null, null, null, true, isPointMBR, metadataPageManagerFactory);
        LSMRTreeTestContext testCtx = new LSMRTreeTestContext(fieldSerdes, lsmTree);
        return testCtx;
    }
}
