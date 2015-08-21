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
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

public class ExternalBTreeLocalResourceMetadata extends LSMBTreeLocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    public ExternalBTreeLocalResourceMetadata(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, boolean isPrimary, int datasetID, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties) {
        super(typeTraits, cmpFactories, bloomFilterKeyFields, isPrimary, datasetID, mergePolicyFactory,
                mergePolicyProperties, null, null, null, null);
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider, String filePath,
            int partition) {
        FileReference file = new FileReference(new File(filePath));
        LSMBTree lsmBTree = LSMBTreeUtils.createExternalBTree(
                file,
                runtimeContextProvider.getBufferCache(),
                runtimeContextProvider.getFileMapManager(),
                typeTraits,
                cmpFactories,
                bloomFilterKeyFields,
                runtimeContextProvider.getBloomFilterFalsePositiveRate(),
                mergePolicyFactory.createMergePolicy(mergePolicyProperties,
                        runtimeContextProvider.getIndexLifecycleManager()),
                new BaseOperationTracker((DatasetLifecycleManager) runtimeContextProvider.getIndexLifecycleManager(),
                        datasetID, ((DatasetLifecycleManager) runtimeContextProvider.getIndexLifecycleManager())
                                .getDatasetInfo(datasetID)), runtimeContextProvider.getLSMIOScheduler(),
                LSMBTreeIOOperationCallbackFactory.INSTANCE.createIOOperationCallback(), -1, true);
        return lsmBTree;
    }
}
