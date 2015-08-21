/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.transaction.management.resource;

import java.io.File;
import java.util.Map;

import edu.uci.ics.asterix.common.context.BaseOperationTracker;
import edu.uci.ics.asterix.common.context.DatasetLifecycleManager;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMRTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;

/**
 * The local resource class for disk only lsm r-tree
 */
public class ExternalRTreeLocalResourceMetadata extends LSMRTreeLocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    public ExternalRTreeLocalResourceMetadata(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, ILinearizeComparatorFactory linearizeCmpFactory, int datasetID,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties, int[] btreeFields) {
        super(typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories, rtreePolicyType,
                linearizeCmpFactory, datasetID, mergePolicyFactory, mergePolicyProperties, null, null, null,
                btreeFields, null);
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider, String filePath,
            int partition) throws HyracksDataException {
        FileReference file = new FileReference(new File(filePath));
        try {
            return LSMRTreeUtils.createExternalRTree(
                    file,
                    runtimeContextProvider.getBufferCache(),
                    runtimeContextProvider.getFileMapManager(),
                    typeTraits,
                    rtreeCmpFactories,
                    btreeCmpFactories,
                    valueProviderFactories,
                    rtreePolicyType,
                    runtimeContextProvider.getBloomFilterFalsePositiveRate(),
                    mergePolicyFactory.createMergePolicy(mergePolicyProperties,
                            runtimeContextProvider.getIndexLifecycleManager()),
                    new BaseOperationTracker((DatasetLifecycleManager) runtimeContextProvider
                            .getIndexLifecycleManager(), datasetID, ((DatasetLifecycleManager) runtimeContextProvider
                            .getIndexLifecycleManager()).getDatasetInfo(datasetID)), runtimeContextProvider
                            .getLSMIOScheduler(), LSMRTreeIOOperationCallbackFactory.INSTANCE
                            .createIOOperationCallback(), linearizeCmpFactory, btreeFields, -1, true);
        } catch (TreeIndexException e) {
            throw new HyracksDataException(e);
        }
    }

}
