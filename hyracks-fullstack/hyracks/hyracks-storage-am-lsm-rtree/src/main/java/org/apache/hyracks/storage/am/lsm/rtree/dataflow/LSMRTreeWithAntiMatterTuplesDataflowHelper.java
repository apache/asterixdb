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

package org.apache.hyracks.storage.am.lsm.rtree.dataflow;

import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class LSMRTreeWithAntiMatterTuplesDataflowHelper extends AbstractLSMRTreeDataflowHelper {
    public LSMRTreeWithAntiMatterTuplesDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, List<IVirtualBufferCache> virtualBufferCaches,
            IBinaryComparatorFactory[] btreeComparatorFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerProvider opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILinearizeComparatorFactory linearizeCmpFactory, int[] rtreeFields, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields, boolean durable) {
        super(opDesc, ctx, partition, virtualBufferCaches, btreeComparatorFactories, valueProviderFactories,
                rtreePolicyType, mergePolicy, opTrackerFactory, ioScheduler, ioOpCallbackFactory, linearizeCmpFactory,
                rtreeFields, filterTypeTraits, filterCmpFactories, filterFields, durable);
    }

    @Override
    protected ITreeIndex createLSMTree(List<IVirtualBufferCache> virtualBufferCaches, FileReference file,
            IBufferCache diskBufferCache, IFileMapProvider diskFileMapProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            ILSMOperationTracker opTracker, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, ILinearizeComparatorFactory linearizeCmpFactory, int[] rtreeFields,
            ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields)
            throws HyracksDataException {
        try {
            return LSMRTreeUtils.createLSMTreeWithAntiMatterTuples(virtualBufferCaches, file, diskBufferCache,
                    diskFileMapProvider, typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories,
                    rtreePolicyType, mergePolicy, opTracker, ioScheduler,
                    ioOpCallbackFactory.createIOOperationCallback(), linearizeCmpFactory, rtreeFields,
                    filterTypeTraits, filterCmpFactories, filterFields, durable);
        } catch (TreeIndexException e) {
            throw new HyracksDataException(e);
        }
    }
}
