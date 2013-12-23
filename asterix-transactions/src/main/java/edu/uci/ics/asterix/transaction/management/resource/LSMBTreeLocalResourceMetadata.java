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
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.common.context.BaseOperationTracker;
import edu.uci.ics.asterix.common.context.DatasetLifecycleManager;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;

public class LSMBTreeLocalResourceMetadata extends AbstractLSMLocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] typeTraits;
    private final IBinaryComparatorFactory[] cmpFactories;
    private final int[] bloomFilterKeyFields;
    private final boolean isPrimary;
    private final ILSMMergePolicyFactory mergePolicyFactory;
    private final Map<String, String> mergePolicyProperties;

    public LSMBTreeLocalResourceMetadata(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, boolean isPrimary, int datasetID, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties) {
        super(datasetID);
        this.typeTraits = typeTraits;
        this.cmpFactories = cmpFactories;
        this.bloomFilterKeyFields = bloomFilterKeyFields;
        this.isPrimary = isPrimary;
        this.mergePolicyFactory = mergePolicyFactory;
        this.mergePolicyProperties = mergePolicyProperties;
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider, String filePath,
            int partition) {
        FileReference file = new FileReference(new File(filePath));
        List<IVirtualBufferCache> virtualBufferCaches = runtimeContextProvider.getVirtualBufferCaches(datasetID);
        LSMBTree lsmBTree = LSMBTreeUtils.createLSMTree(virtualBufferCaches, file, runtimeContextProvider
                .getBufferCache(), runtimeContextProvider.getFileMapManager(), typeTraits, cmpFactories,
                bloomFilterKeyFields, runtimeContextProvider.getBloomFilterFalsePositiveRate(), mergePolicyFactory
                        .createMergePolicy(mergePolicyProperties),
                isPrimary ? runtimeContextProvider.getLSMBTreeOperationTracker(datasetID) : new BaseOperationTracker(
                        (DatasetLifecycleManager) runtimeContextProvider.getIndexLifecycleManager(), datasetID),
                runtimeContextProvider.getLSMIOScheduler(), LSMBTreeIOOperationCallbackFactory.INSTANCE
                        .createIOOperationCallback());
        return lsmBTree;
    }

}
