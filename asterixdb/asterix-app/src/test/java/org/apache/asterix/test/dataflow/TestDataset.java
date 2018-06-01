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
package org.apache.asterix.test.dataflow;

import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.transaction.management.resource.DatasetLocalResourceFactory;
import org.apache.asterix.transaction.management.runtime.CommitRuntime;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.common.IResourceFactory;

public class TestDataset extends Dataset {

    private static final long serialVersionUID = 1L;

    public TestDataset(String dataverseName, String datasetName, String recordTypeDataverseName, String recordTypeName,
            String nodeGroupName, String compactionPolicy, Map<String, String> compactionPolicyProperties,
            IDatasetDetails datasetDetails, Map<String, String> hints, DatasetType datasetType, int datasetId,
            int pendingOp) {
        super(dataverseName, datasetName, recordTypeDataverseName, recordTypeName, nodeGroupName, compactionPolicy,
                compactionPolicyProperties, datasetDetails, hints, datasetType, datasetId, pendingOp);
    }

    @Override
    public IPushRuntimeFactory getCommitRuntimeFactory(MetadataProvider metadataProvider,
            int[] primaryKeyFieldPermutation, boolean isSink) throws AlgebricksException {
        return new IPushRuntimeFactory() {
            @Override
            public IPushRuntime[] createPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
                return new IPushRuntime[] { new CommitRuntime(ctx, new TxnId(ctx.getJobletContext().getJobId().getId()),
                        getDatasetId(), primaryKeyFieldPermutation, true,
                        ctx.getTaskAttemptId().getTaskId().getPartition(), true) };
            }
        };
    }

    @Override
    public IResourceFactory getResourceFactory(MetadataProvider mdProvider, Index index, ARecordType recordType,
            ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties)
            throws AlgebricksException {
        ITypeTraits[] filterTypeTraits = DatasetUtil.computeFilterTypeTraits(this, recordType);
        IBinaryComparatorFactory[] filterCmpFactories = DatasetUtil.computeFilterBinaryComparatorFactories(this,
                recordType, mdProvider.getStorageComponentProvider().getComparatorFactoryProvider());
        IResourceFactory resourceFactory =
                TestLsmBTreeResourceFactoryProvider.INSTANCE.getResourceFactory(mdProvider, this, index, recordType,
                        metaType, mergePolicyFactory, mergePolicyProperties, filterTypeTraits, filterCmpFactories);
        return new DatasetLocalResourceFactory(getDatasetId(), resourceFactory);
    }

    @Override
    public ILSMIOOperationCallbackFactory getIoOperationCallbackFactory(Index index) throws AlgebricksException {
        return new TestLsmIoOpCallbackFactory(getComponentIdGeneratorFactory(), getDatasetInfoProvider());
    }
}
