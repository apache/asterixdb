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
import org.apache.asterix.common.ioopcallbacks.LSMBTreeWithBuddyIOOperationCallbackFactory;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

/**
 * The local resource for disk only lsm btree with buddy tree
 */
public class ExternalBTreeWithBuddyLocalResourceMetadata extends AbstractLSMLocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] typeTraits;
    private final IBinaryComparatorFactory[] btreeCmpFactories;
    private final ILSMMergePolicyFactory mergePolicyFactory;
    private final Map<String, String> mergePolicyProperties;
    private final int[] buddyBtreeFields;

    public ExternalBTreeWithBuddyLocalResourceMetadata(int datasetID, IBinaryComparatorFactory[] btreeCmpFactories,
            ITypeTraits[] typeTraits, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, int[] buddyBtreeFields) {
        super(datasetID, null, null, null);
        this.btreeCmpFactories = btreeCmpFactories;
        this.typeTraits = typeTraits;
        this.mergePolicyFactory = mergePolicyFactory;
        this.mergePolicyProperties = mergePolicyProperties;
        this.buddyBtreeFields = buddyBtreeFields;
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider, String filePath,
            int partition) throws HyracksDataException {
        FileReference file = new FileReference(new File(filePath));
        return LSMBTreeUtils.createExternalBTreeWithBuddy(file, runtimeContextProvider.getBufferCache(),
                runtimeContextProvider.getFileMapManager(), typeTraits, btreeCmpFactories,
                runtimeContextProvider.getBloomFilterFalsePositiveRate(),
                mergePolicyFactory.createMergePolicy(mergePolicyProperties,
                        runtimeContextProvider.getDatasetLifecycleManager()),
                new BaseOperationTracker(datasetID,
                        runtimeContextProvider.getDatasetLifecycleManager().getDatasetInfo(datasetID)),
                runtimeContextProvider.getLSMIOScheduler(),
                LSMBTreeWithBuddyIOOperationCallbackFactory.INSTANCE.createIOOperationCallback(), buddyBtreeFields, -1,
                true);
    }
}
