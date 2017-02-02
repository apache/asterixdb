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

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

public class IndexUtil {

    //TODO: replace this null with an empty array. currently, this breaks many tests
    private static final int[] empty = null;
    private static final PhysicalOptimizationConfig physicalOptimizationConfig =
            OptimizationConfUtil.getPhysicalOptimizationConfig();

    private IndexUtil() {
    }

    public static int[] getFilterFields(Dataset dataset, Index index, ITypeTraits[] filterTypeTraits)
            throws AlgebricksException {
        if (index.isPrimaryIndex()) {
            return DatasetUtil.createFilterFields(dataset);
        }
        return secondaryFilterFields(dataset, index, filterTypeTraits);
    }

    public static int[] getBtreeFieldsIfFiltered(Dataset dataset, Index index) throws AlgebricksException {
        if (index.isPrimaryIndex()) {
            return DatasetUtil.createBTreeFieldsWhenThereisAFilter(dataset);
        }
        int numPrimaryKeys = DatasetUtil.getPartitioningKeys(dataset).size();
        int numSecondaryKeys = index.getKeyFieldNames().size();
        int[] btreeFields = new int[numSecondaryKeys + numPrimaryKeys];
        for (int k = 0; k < btreeFields.length; k++) {
            btreeFields[k] = k;
        }
        return btreeFields;
    }

    private static int[] secondaryFilterFields(Dataset dataset, Index index, ITypeTraits[] filterTypeTraits)
            throws CompilationException {
        if (filterTypeTraits == null) {
            return empty;
        }
        int numPrimaryKeys = DatasetUtil.getPartitioningKeys(dataset).size();
        int numSecondaryKeys = index.getKeyFieldNames().size();
        switch (index.getIndexType()) {
            case BTREE:
                return new int[] { numPrimaryKeys + numSecondaryKeys };
            case RTREE:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case SINGLE_PARTITION_WORD_INVIX:
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE,
                        index.getIndexType().toString());
        }
        return empty;
    }

    public static JobSpecification dropJob(Index index, MetadataProvider metadataProvider, Dataset dataset)
            throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification();
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        boolean temp = dataset.getDatasetDetails().isTemp();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(index.getDataverseName(), index.getDatasetName(),
                        index.getIndexName(), temp);
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        ARecordType recordType =
                (ARecordType) metadataProvider.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        ARecordType metaType = DatasetUtil.getMetaType(metadataProvider, dataset);
        IIndexDataflowHelperFactory dataflowHelperFactory = dataset.getIndexDataflowHelperFactory(metadataProvider,
                index, recordType, metaType, compactionInfo.first, compactionInfo.second);
        IndexDropOperatorDescriptor btreeDrop =
                new IndexDropOperatorDescriptor(spec, storageComponentProvider.getStorageManager(),
                        storageComponentProvider.getIndexLifecycleManagerProvider(), splitsAndConstraint.first,
                        dataflowHelperFactory, storageComponentProvider.getMetadataPageManagerFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, btreeDrop,
                splitsAndConstraint.second);
        spec.addRoot(btreeDrop);
        return spec;
    }

    public static JobSpecification buildSecondaryIndexCreationJobSpec(Dataset dataset, Index index,
            ARecordType recType, ARecordType metaType, ARecordType enforcedType, ARecordType enforcedMetaType,
            MetadataProvider metadataProvider) throws AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper =
                SecondaryIndexOperationsHelper.createIndexOperationsHelper(dataset, index, metadataProvider,
                        physicalOptimizationConfig, recType, metaType, enforcedType, enforcedMetaType);
        return secondaryIndexHelper.buildCreationJobSpec();
    }

    public static JobSpecification buildSecondaryIndexLoadingJobSpec(Dataset dataset, Index index, ARecordType recType,
            ARecordType metaType, ARecordType enforcedType, ARecordType enforcedMetaType,
            MetadataProvider metadataProvider) throws AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper =
                SecondaryIndexOperationsHelper.createIndexOperationsHelper(dataset, index, metadataProvider,
                        physicalOptimizationConfig, recType, metaType, enforcedType, enforcedMetaType);
        return secondaryIndexHelper.buildLoadingJobSpec();
    }

    public static JobSpecification buildSecondaryIndexLoadingJobSpec(Dataset dataset, Index index, ARecordType recType,
            ARecordType metaType, ARecordType enforcedType, ARecordType enforcedMetaType,
            MetadataProvider metadataProvider, List<ExternalFile> files) throws AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper =
                SecondaryIndexOperationsHelper.createIndexOperationsHelper(dataset, index, metadataProvider,
                        physicalOptimizationConfig, recType, metaType, enforcedType, enforcedMetaType);
        secondaryIndexHelper.setExternalFiles(files);
        return secondaryIndexHelper.buildLoadingJobSpec();
    }

    public static JobSpecification buildDropSecondaryIndexJobSpec(Index index, MetadataProvider metadataProvider,
            Dataset dataset) throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification();
        boolean temp = dataset.getDatasetDetails().isTemp();
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(index.getDataverseName(), index.getDatasetName(),
                        index.getIndexName(), temp);
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

    public static JobSpecification buildSecondaryIndexCompactJobSpec(Dataset dataset, Index index, ARecordType recType,
            ARecordType metaType, ARecordType enforcedType, ARecordType enforcedMetaType,
            MetadataProvider metadataProvider) throws AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper =
                SecondaryIndexOperationsHelper.createIndexOperationsHelper(dataset, index, metadataProvider,
                        physicalOptimizationConfig, recType, metaType, enforcedType, enforcedMetaType);
        return secondaryIndexHelper.buildCompactJobSpec();
    }
}
