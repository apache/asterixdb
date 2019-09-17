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

import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LsmResourceFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.IStorageManager;

public class LSMRTreeLocalResourceFactory extends LsmResourceFactory {

    private static final long serialVersionUID = 1L;

    protected final IBinaryComparatorFactory[] btreeCmpFactories;
    protected final IPrimitiveValueProviderFactory[] valueProviderFactories;
    protected final RTreePolicyType rtreePolicyType;
    protected final ILinearizeComparatorFactory linearizeCmpFactory;
    protected final int[] rtreeFields;
    protected final int[] buddyBTreeFields;
    protected final boolean isPointMBR;
    protected final double bloomFilterFalsePositiveRate;

    public LSMRTreeLocalResourceFactory(IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable, IBinaryComparatorFactory[] btreeCmpFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            ILinearizeComparatorFactory linearizeCmpFactory, int[] rtreeFields, int[] buddyBTreeFields,
            boolean isPointMBR, double bloomFilterFalsePositiveRate) {
        super(storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerProvider, ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, durable);
        this.btreeCmpFactories = btreeCmpFactories;
        this.valueProviderFactories = valueProviderFactories;
        this.rtreePolicyType = rtreePolicyType;
        this.linearizeCmpFactory = linearizeCmpFactory;
        this.rtreeFields = rtreeFields;
        this.buddyBTreeFields = buddyBTreeFields;
        this.isPointMBR = isPointMBR;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
    }

    @Override
    public IResource createResource(FileReference fileRef) {
        return new LSMRTreeLocalResource(fileRef.getRelativePath(), storageManager, typeTraits, cmpFactories,
                filterTypeTraits, filterCmpFactories, filterFields, opTrackerProvider, ioOpCallbackFactory,
                pageWriteCallbackFactory, metadataPageManagerFactory, vbcProvider, ioSchedulerProvider,
                mergePolicyFactory, mergePolicyProperties, durable, btreeCmpFactories, valueProviderFactories,
                rtreePolicyType, linearizeCmpFactory, rtreeFields, buddyBTreeFields, isPointMBR,
                bloomFilterFalsePositiveRate);
    }
}
