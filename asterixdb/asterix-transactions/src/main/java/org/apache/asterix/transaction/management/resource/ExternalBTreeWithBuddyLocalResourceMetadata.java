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

import org.apache.asterix.common.api.IAppRuntimeContext;
import org.apache.asterix.common.transactions.Resource;
import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.utils.LSMBTreeUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.common.file.LocalResource;

/**
 * The local resource for disk only lsm btree with buddy tree
 */
public class ExternalBTreeWithBuddyLocalResourceMetadata extends Resource {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] typeTraits;
    private final IBinaryComparatorFactory[] btreeCmpFactories;
    private final ILSMMergePolicyFactory mergePolicyFactory;
    private final Map<String, String> mergePolicyProperties;
    private final int[] buddyBtreeFields;

    public ExternalBTreeWithBuddyLocalResourceMetadata(int datasetID, int partition,
            IBinaryComparatorFactory[] btreeCmpFactories, ITypeTraits[] typeTraits,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
            int[] buddyBtreeFields, ILSMOperationTrackerFactory opTrackerProvider,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory) {
        super(datasetID, partition, null, null, null, opTrackerProvider, ioOpCallbackFactory,
                metadataPageManagerFactory);
        this.btreeCmpFactories = btreeCmpFactories;
        this.typeTraits = typeTraits;
        this.mergePolicyFactory = mergePolicyFactory;
        this.mergePolicyProperties = mergePolicyProperties;
        this.buddyBtreeFields = buddyBtreeFields;
    }

    @Override
    public ILSMIndex createIndexInstance(INCApplicationContext appCtx, LocalResource resource)
            throws HyracksDataException {
        IAppRuntimeContext appRuntimeCtx = (IAppRuntimeContext) appCtx.getApplicationObject();
        IIOManager ioManager = appCtx.getIoManager();
        FileReference file = ioManager.resolve(resource.getPath());
        return LSMBTreeUtil.createExternalBTreeWithBuddy(ioManager, file, appRuntimeCtx.getBufferCache(),
                appRuntimeCtx.getFileMapManager(), typeTraits, btreeCmpFactories,
                appRuntimeCtx.getBloomFilterFalsePositiveRate(),
                mergePolicyFactory.createMergePolicy(mergePolicyProperties,
                        appRuntimeCtx.getDatasetLifecycleManager()),
                opTrackerProvider.getOperationTracker(appCtx), appRuntimeCtx.getLSMIOScheduler(),
                ioOpCallbackFactory.createIoOpCallback(), buddyBtreeFields, -1, true, metadataPageManagerFactory);
    }
}
