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

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.context.BaseOperationTracker;
import org.apache.asterix.common.ioopcallbacks.LSMRTreeIOOperationCallbackFactory;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;

public class LSMRTreeLocalResourceMetadata extends AbstractLSMLocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    protected final ITypeTraits[] typeTraits;
    protected final IBinaryComparatorFactory[] rtreeCmpFactories;
    protected final IBinaryComparatorFactory[] btreeCmpFactories;
    protected final IPrimitiveValueProviderFactory[] valueProviderFactories;
    protected final RTreePolicyType rtreePolicyType;
    protected final ILinearizeComparatorFactory linearizeCmpFactory;
    protected final ILSMMergePolicyFactory mergePolicyFactory;
    protected final Map<String, String> mergePolicyProperties;
    protected final int[] rtreeFields;
    protected final int[] btreeFields;

    public LSMRTreeLocalResourceMetadata(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, ILinearizeComparatorFactory linearizeCmpFactory, int datasetID,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
            ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories, int[] rtreeFields,
            int[] btreeFields, int[] filterFields) {
        super(datasetID, filterTypeTraits, filterCmpFactories, filterFields);
        this.typeTraits = typeTraits;
        this.rtreeCmpFactories = rtreeCmpFactories;
        this.btreeCmpFactories = btreeCmpFactories;
        this.valueProviderFactories = valueProviderFactories;
        this.rtreePolicyType = rtreePolicyType;
        this.linearizeCmpFactory = linearizeCmpFactory;
        this.mergePolicyFactory = mergePolicyFactory;
        this.mergePolicyProperties = mergePolicyProperties;
        this.rtreeFields = rtreeFields;
        this.btreeFields = btreeFields;
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider, String filePath,
            int partition) throws HyracksDataException {
        FileReference file = new FileReference(new File(filePath));
        List<IVirtualBufferCache> virtualBufferCaches = runtimeContextProvider.getVirtualBufferCaches(datasetID);
        try {
            return LSMRTreeUtils.createLSMTree(virtualBufferCaches, file, runtimeContextProvider.getBufferCache(),
                    runtimeContextProvider.getFileMapManager(), typeTraits, rtreeCmpFactories, btreeCmpFactories,
                    valueProviderFactories, rtreePolicyType, runtimeContextProvider.getBloomFilterFalsePositiveRate(),
                    mergePolicyFactory.createMergePolicy(mergePolicyProperties,
                            runtimeContextProvider.getDatasetLifecycleManager()),
                    new BaseOperationTracker(datasetID,
                            runtimeContextProvider.getDatasetLifecycleManager().getDatasetInfo(datasetID)),
                    runtimeContextProvider.getLSMIOScheduler(),
                    LSMRTreeIOOperationCallbackFactory.INSTANCE.createIOOperationCallback(), linearizeCmpFactory,
                    rtreeFields, btreeFields, filterTypeTraits, filterCmpFactories, filterFields, true);
        } catch (TreeIndexException e) {
            throw new HyracksDataException(e);
        }
    }
}
