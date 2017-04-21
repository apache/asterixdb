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

package org.apache.asterix.metadata.utils;

import java.util.Map;

import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexCompactOperatorDescriptor;

public abstract class SecondaryTreeIndexOperationsHelper extends SecondaryIndexOperationsHelper {

    protected SecondaryTreeIndexOperationsHelper(Dataset dataset, Index index, PhysicalOptimizationConfig physOptConf,
            MetadataProvider metadataProvider, ARecordType recType, ARecordType metaType, ARecordType enforcedType,
            ARecordType enforcedMetaType) {
        super(dataset, index, physOptConf, metadataProvider, recType, metaType, enforcedType, enforcedMetaType);
    }

    @Override
    public JobSpecification buildDropJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset, index.getIndexName());
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        ARecordType recordType =
                (ARecordType) metadataProvider.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        ARecordType metaType = DatasetUtil.getMetaType(metadataProvider, dataset);
        IIndexDataflowHelperFactory dataflowHelperFactory = dataset.getIndexDataflowHelperFactory(metadataProvider,
                index, recordType, metaType, compactionInfo.first, compactionInfo.second);
        // The index drop operation should be persistent regardless of temp datasets or permanent dataset.
        IndexDropOperatorDescriptor btreeDrop =
                new IndexDropOperatorDescriptor(spec, storageComponentProvider.getStorageManager(),
                        storageComponentProvider.getIndexLifecycleManagerProvider(), splitsAndConstraint.first,
                        dataflowHelperFactory, storageComponentProvider.getMetadataPageManagerFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, btreeDrop,
                splitsAndConstraint.second);
        spec.addRoot(btreeDrop);
        return spec;
    }

    @Override
    public JobSpecification buildCompactJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        IIndexDataflowHelperFactory indexDataflowHelperFactory = dataset.getIndexDataflowHelperFactory(metadataProvider,
                index, itemType, metaType, mergePolicyFactory, mergePolicyFactoryProperties);
        LSMTreeIndexCompactOperatorDescriptor compactOp = new LSMTreeIndexCompactOperatorDescriptor(spec,
                metadataProvider.getStorageComponentProvider().getStorageManager(),
                metadataProvider.getStorageComponentProvider().getIndexLifecycleManagerProvider(),
                secondaryFileSplitProvider, secondaryTypeTraits, secondaryComparatorFactories,
                secondaryBloomFilterKeyFields, indexDataflowHelperFactory,
                dataset.getModificationCallbackFactory(metadataProvider.getStorageComponentProvider(), index, null,
                        IndexOperation.FULL_MERGE, null),
                metadataProvider.getStorageComponentProvider().getMetadataPageManagerFactory());

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                secondaryPartitionConstraint);
        spec.addRoot(compactOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }
}
