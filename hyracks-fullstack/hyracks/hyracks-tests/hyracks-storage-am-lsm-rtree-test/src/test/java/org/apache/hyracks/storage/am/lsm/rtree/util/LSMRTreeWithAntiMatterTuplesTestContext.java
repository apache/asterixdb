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
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTreeWithAntiMatterTuples;
import org.apache.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import org.apache.hyracks.storage.am.rtree.AbstractRTreeTestContext;
import org.apache.hyracks.storage.am.rtree.RTreeCheckTuple;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

@SuppressWarnings("rawtypes")
public final class LSMRTreeWithAntiMatterTuplesTestContext extends AbstractRTreeTestContext {

    public LSMRTreeWithAntiMatterTuplesTestContext(ISerializerDeserializer[] fieldSerdes, ITreeIndex treeIndex)
            throws HyracksDataException {
        super(fieldSerdes, treeIndex);
    }

    @Override
    public int getKeyFieldCount() {
        LSMRTreeWithAntiMatterTuples lsmTree = (LSMRTreeWithAntiMatterTuples) index;
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
        LSMRTreeWithAntiMatterTuples lsmTree = (LSMRTreeWithAntiMatterTuples) index;
        return lsmTree.getComparatorFactories();
    }

    public static LSMRTreeWithAntiMatterTuplesTestContext create(IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ISerializerDeserializer[] fieldSerdes, IPrimitiveValueProviderFactory[] valueProviderFactories,
            int numKeyFields, RTreePolicyType rtreePolicyType, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory) throws Exception {
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] rtreeCmpFactories =
                SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeyFields);
        IBinaryComparatorFactory[] btreeCmpFactories =
                SerdeUtils.serdesToComparatorFactories(fieldSerdes, fieldSerdes.length);
        LSMRTreeWithAntiMatterTuples lsmTree = LSMRTreeUtils.createLSMTreeWithAntiMatterTuples(ioManager,
                virtualBufferCaches, file, diskBufferCache, typeTraits, rtreeCmpFactories, btreeCmpFactories,
                valueProviderFactories, rtreePolicyType, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory,
                pageWriteCallbackFactory, LSMRTreeUtils.proposeBestLinearizer(typeTraits, rtreeCmpFactories.length),
                null, null, null, null, true, false, metadataPageManagerFactory);
        LSMRTreeWithAntiMatterTuplesTestContext testCtx =
                new LSMRTreeWithAntiMatterTuplesTestContext(fieldSerdes, lsmTree);
        return testCtx;
    }
}
