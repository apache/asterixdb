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
package org.apache.asterix.file;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.AsterixStorageProperties;
import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalFile;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import org.apache.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import org.apache.asterix.translator.CompiledStatements.CompiledCreateIndexStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledIndexCompactStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledIndexDropStatement;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

public class IndexOperations {

    private static final PhysicalOptimizationConfig physicalOptimizationConfig = OptimizationConfUtil
            .getPhysicalOptimizationConfig();

    public static JobSpecification buildSecondaryIndexCreationJobSpec(CompiledCreateIndexStatement createIndexStmt,
            ARecordType recType, ARecordType enforcedType, AqlMetadataProvider metadataProvider)
            throws AsterixException, AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper = SecondaryIndexOperationsHelper
                .createIndexOperationsHelper(createIndexStmt.getIndexType(), createIndexStmt.getDataverseName(),
                        createIndexStmt.getDatasetName(), createIndexStmt.getIndexName(),
                        createIndexStmt.getKeyFields(), createIndexStmt.getKeyFieldTypes(),
                        createIndexStmt.isEnforced(), createIndexStmt.getGramLength(), metadataProvider,
                        physicalOptimizationConfig, recType, enforcedType);
        return secondaryIndexHelper.buildCreationJobSpec();
    }

    public static JobSpecification buildSecondaryIndexLoadingJobSpec(CompiledCreateIndexStatement createIndexStmt,
            ARecordType recType, ARecordType enforcedType, AqlMetadataProvider metadataProvider)
            throws AsterixException, AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper = SecondaryIndexOperationsHelper
                .createIndexOperationsHelper(createIndexStmt.getIndexType(), createIndexStmt.getDataverseName(),
                        createIndexStmt.getDatasetName(), createIndexStmt.getIndexName(),
                        createIndexStmt.getKeyFields(), createIndexStmt.getKeyFieldTypes(),
                        createIndexStmt.isEnforced(), createIndexStmt.getGramLength(), metadataProvider,
                        physicalOptimizationConfig, recType, enforcedType);
        return secondaryIndexHelper.buildLoadingJobSpec();
    }

    public static JobSpecification buildSecondaryIndexLoadingJobSpec(CompiledCreateIndexStatement createIndexStmt,
            ARecordType recType, ARecordType enforcedType, AqlMetadataProvider metadataProvider,
            List<ExternalFile> files) throws AsterixException, AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper = SecondaryIndexOperationsHelper
                .createIndexOperationsHelper(createIndexStmt.getIndexType(), createIndexStmt.getDataverseName(),
                        createIndexStmt.getDatasetName(), createIndexStmt.getIndexName(),
                        createIndexStmt.getKeyFields(), createIndexStmt.getKeyFieldTypes(),
                        createIndexStmt.isEnforced(), createIndexStmt.getGramLength(), metadataProvider,
                        physicalOptimizationConfig, recType, enforcedType);
        secondaryIndexHelper.setExternalFiles(files);
        return secondaryIndexHelper.buildLoadingJobSpec();
    }

    public static JobSpecification buildDropSecondaryIndexJobSpec(CompiledIndexDropStatement indexDropStmt,
            AqlMetadataProvider metadataProvider, Dataset dataset) throws AlgebricksException, MetadataException {
        String dataverseName = indexDropStmt.getDataverseName() == null ? metadataProvider.getDefaultDataverseName()
                : indexDropStmt.getDataverseName();
        String datasetName = indexDropStmt.getDatasetName();
        String indexName = indexDropStmt.getIndexName();
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        boolean temp = dataset.getDatasetDetails().isTemp();

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadataProvider
                .splitProviderAndPartitionConstraintsForDataset(dataverseName, datasetName, indexName, temp);
        AsterixStorageProperties storageProperties = AsterixAppContextInfo.getInstance().getStorageProperties();
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(dataset,
                metadataProvider.getMetadataTxnContext());

        // The index drop operation should be persistent regardless of temp datasets or permanent dataset.
        IndexDropOperatorDescriptor btreeDrop = new IndexDropOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                splitsAndConstraint.first, new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(
                        dataset.getDatasetId()), compactionInfo.first, compactionInfo.second,
                        new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate(), false, null, null, null, null, true));
        AlgebricksPartitionConstraintHelper
                .setPartitionConstraintInJobSpec(spec, btreeDrop, splitsAndConstraint.second);
        spec.addRoot(btreeDrop);

        return spec;
    }

    public static JobSpecification buildSecondaryIndexCompactJobSpec(CompiledIndexCompactStatement indexCompactStmt,
            ARecordType recType, ARecordType enforcedType, AqlMetadataProvider metadataProvider, Dataset dataset)
            throws AsterixException, AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper = SecondaryIndexOperationsHelper
                .createIndexOperationsHelper(indexCompactStmt.getIndexType(), indexCompactStmt.getDataverseName(),
                        indexCompactStmt.getDatasetName(), indexCompactStmt.getIndexName(),
                        indexCompactStmt.getKeyFields(), indexCompactStmt.getKeyTypes(), indexCompactStmt.isEnforced(),
                        indexCompactStmt.getGramLength(), metadataProvider, physicalOptimizationConfig, recType,
                        enforcedType);
        return secondaryIndexHelper.buildCompactJobSpec();
    }
}
