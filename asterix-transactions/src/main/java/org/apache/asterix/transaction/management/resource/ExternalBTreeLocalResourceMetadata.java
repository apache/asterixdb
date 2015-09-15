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
import java.util.Map;

import org.apache.asterix.common.context.BaseOperationTracker;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

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
