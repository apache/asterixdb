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

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.context.BaseOperationTracker;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.Resource;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
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
            ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] btreeFields, int[] filterFields) {
        super(datasetID, partition, filterTypeTraits, filterCmpFactories, filterFields);
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
        return new StringBuilder().append(" { \"").append(LSMBTreeLocalResourceMetadata.class.getName()).append(
                "\" : {").append("\"datasetId\" : ").append(datasetId()).append(", \"partition\" : ").append(
                        partition()).append(" } ").append(" }").toString();
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider,
            LocalResource resource) throws HyracksDataException {
        IIOManager ioManager = runtimeContextProvider.getIOManager();
        FileReference file = ioManager.getFileRef(resource.getPath(), true);
        List<IODeviceHandle> ioDevices = ioManager.getIODevices();
        int ioDeviceNum = 0;
        for (int i = 0; i < ioDevices.size(); i++) {
            IODeviceHandle device = ioDevices.get(i);
            if (device == file.getDeviceHandle()) {
                ioDeviceNum = i;
                break;
            }
        }
        final IDatasetLifecycleManager datasetLifecycleManager = runtimeContextProvider.getDatasetLifecycleManager();
        LSMBTree lsmBTree = LSMBTreeUtils.createLSMTree(ioManager, datasetLifecycleManager.getVirtualBufferCaches(
                datasetId(),
                ioDeviceNum), file, runtimeContextProvider.getBufferCache(), runtimeContextProvider.getFileMapManager(),
                typeTraits, cmpFactories, bloomFilterKeyFields, runtimeContextProvider
                        .getBloomFilterFalsePositiveRate(),
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, datasetLifecycleManager),
                isPrimary ? runtimeContextProvider.getLSMBTreeOperationTracker(datasetId())
                        : new BaseOperationTracker(datasetId(), datasetLifecycleManager.getDatasetInfo(datasetId())),
                runtimeContextProvider.getLSMIOScheduler(),
                LSMBTreeIOOperationCallbackFactory.INSTANCE.createIOOperationCallback(), isPrimary, filterTypeTraits,
                filterCmpFactories, btreeFields, filterFields, true);
        return lsmBTree;
    }
}
