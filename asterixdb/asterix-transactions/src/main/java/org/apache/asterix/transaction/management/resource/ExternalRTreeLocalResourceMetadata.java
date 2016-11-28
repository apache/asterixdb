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
package org.apache.asterix.transaction.management.resource;

import java.util.Map;

import org.apache.asterix.common.context.BaseOperationTracker;
import org.apache.asterix.common.ioopcallbacks.LSMRTreeIOOperationCallbackFactory;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.common.file.LocalResource;

/**
 * The local resource class for disk only lsm r-tree
 */
public class ExternalRTreeLocalResourceMetadata extends LSMRTreeLocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    public ExternalRTreeLocalResourceMetadata(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, ILinearizeComparatorFactory linearizeCmpFactory, int datasetID,
            int partition, ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
            int[] btreeFields, boolean isPointMBR) {
        super(typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories, rtreePolicyType,
                linearizeCmpFactory, datasetID, partition, mergePolicyFactory, mergePolicyProperties, null, null, null,
                btreeFields, null, isPointMBR);
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider,
            LocalResource resource) throws HyracksDataException {
        IIOManager ioManager = runtimeContextProvider.getIOManager();
        FileReference file = ioManager.resolve(resource.getPath());
        try {
            return LSMRTreeUtils.createExternalRTree(ioManager, file, runtimeContextProvider.getBufferCache(),
                    runtimeContextProvider.getFileMapManager(), typeTraits, rtreeCmpFactories, btreeCmpFactories,
                    valueProviderFactories, rtreePolicyType, runtimeContextProvider.getBloomFilterFalsePositiveRate(),
                    mergePolicyFactory.createMergePolicy(mergePolicyProperties,
                            runtimeContextProvider.getDatasetLifecycleManager()),
                    new BaseOperationTracker(datasetId(),
                            runtimeContextProvider.getDatasetLifecycleManager().getDatasetInfo(datasetId())),
                    runtimeContextProvider.getLSMIOScheduler(),
                    LSMRTreeIOOperationCallbackFactory.INSTANCE.createIOOperationCallback(), linearizeCmpFactory,
                    btreeFields, -1, true, isPointMBR);
        } catch (TreeIndexException e) {
            throw new HyracksDataException(e);
        }
    }
}
