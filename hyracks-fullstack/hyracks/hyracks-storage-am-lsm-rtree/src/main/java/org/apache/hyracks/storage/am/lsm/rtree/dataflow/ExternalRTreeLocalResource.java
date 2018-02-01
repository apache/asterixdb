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

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IStorageManager;

/**
 * The local resource class for disk only lsm r-tree
 */
public class ExternalRTreeLocalResource extends LSMRTreeLocalResource {

    private static final long serialVersionUID = 1L;

    public ExternalRTreeLocalResource(String path, IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] rtreeCmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable, IBinaryComparatorFactory[] btreeCmpFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            ILinearizeComparatorFactory linearizeCmpFactory, int[] rtreeFields, int[] buddyBTreeFields,
            boolean isPointMBR, double bloomFilterFalsePositiveRate) {
        super(path, storageManager, typeTraits, rtreeCmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerProvider, ioOpCallbackFactory, metadataPageManagerFactory, null, ioSchedulerProvider,
                mergePolicyFactory, mergePolicyProperties, durable, btreeCmpFactories, valueProviderFactories,
                rtreePolicyType, linearizeCmpFactory, rtreeFields, buddyBTreeFields, isPointMBR,
                bloomFilterFalsePositiveRate);
    }

    @Override
    public IIndex createInstance(INCServiceContext ncServiceCtx) throws HyracksDataException {
        IIOManager ioManager = ncServiceCtx.getIoManager();
        FileReference fileRef = ioManager.resolve(path);
        ioOpCallbackFactory.initialize(ncServiceCtx, this);
        return LSMRTreeUtils.createExternalRTree(ioManager, fileRef, storageManager.getBufferCache(ncServiceCtx),
                typeTraits, cmpFactories, btreeCmpFactories, valueProviderFactories, rtreePolicyType,
                bloomFilterFalsePositiveRate, mergePolicyFactory.createMergePolicy(mergePolicyProperties, ncServiceCtx),
                opTrackerProvider.getOperationTracker(ncServiceCtx, this),
                ioSchedulerProvider.getIoScheduler(ncServiceCtx), ioOpCallbackFactory, linearizeCmpFactory,
                buddyBTreeFields, durable, isPointMBR, metadataPageManagerFactory, ncServiceCtx.getTracer());

    }
}
