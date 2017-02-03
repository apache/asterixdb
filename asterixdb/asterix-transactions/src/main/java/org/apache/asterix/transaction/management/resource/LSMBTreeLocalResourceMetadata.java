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
import org.apache.asterix.common.api.IDatasetLifecycleManager;
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

public class LSMBTreeLocalResourceMetadata extends Resource {

    private static final long serialVersionUID = 1L;

    protected final ITypeTraits[] typeTraits;
    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final int[] bloomFilterKeyFields;
    protected final boolean isPrimary;
    protected final ILSMMergePolicyFactory mergePolicyFactory;
    protected final Map<String, String> mergePolicyProperties;
    protected final int[] btreeFields;

    public LSMBTreeLocalResourceMetadata(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, boolean isPrimary, int datasetID, int partition,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
            ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories, int[] btreeFields,
            int[] filterFields, ILSMOperationTrackerFactory opTrackerProvider,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory) {
        super(datasetID, partition, filterTypeTraits, filterCmpFactories, filterFields, opTrackerProvider,
                ioOpCallbackFactory, metadataPageManagerFactory);
        this.typeTraits = typeTraits;
        this.cmpFactories = cmpFactories;
        this.bloomFilterKeyFields = bloomFilterKeyFields;
        this.isPrimary = isPrimary;
        this.mergePolicyFactory = mergePolicyFactory;
        this.mergePolicyProperties = mergePolicyProperties;
        this.btreeFields = btreeFields;
    }

    @Override
    public String toString() {
        return new StringBuilder().append(" { \"").append(LSMBTreeLocalResourceMetadata.class.getName())
                .append("\" : {").append("\"datasetId\" : ").append(datasetId()).append(", \"partition\" : ")
                .append(partition()).append(" } ").append(" }").toString();
    }

    @Override
    public ILSMIndex createIndexInstance(INCApplicationContext appCtx, LocalResource resource)
            throws HyracksDataException {
        IAppRuntimeContext appRuntimeCtx = (IAppRuntimeContext) appCtx.getApplicationObject();
        IIOManager ioManager = appRuntimeCtx.getIOManager();
        FileReference file = ioManager.resolve(resource.getPath());
        int ioDeviceNum = Resource.getIoDeviceNum(ioManager, file.getDeviceHandle());
        final IDatasetLifecycleManager datasetLifecycleManager = appRuntimeCtx.getDatasetLifecycleManager();
        return LSMBTreeUtil.createLSMTree(ioManager,
                datasetLifecycleManager.getVirtualBufferCaches(datasetId(), ioDeviceNum), file,
                appRuntimeCtx.getBufferCache(), appRuntimeCtx.getFileMapManager(), typeTraits, cmpFactories,
                bloomFilterKeyFields, appRuntimeCtx.getBloomFilterFalsePositiveRate(),
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, datasetLifecycleManager),
                opTrackerProvider.getOperationTracker(appCtx), appRuntimeCtx.getLSMIOScheduler(),
                ioOpCallbackFactory.createIoOpCallback(), isPrimary, filterTypeTraits, filterCmpFactories, btreeFields,
                filterFields, true, metadataPageManagerFactory);
    }
}
