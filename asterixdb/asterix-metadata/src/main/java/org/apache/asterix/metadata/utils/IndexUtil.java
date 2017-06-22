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

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.transaction.management.service.transaction.JobIdFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobSpecification;

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

    public static Index getPrimaryIndex(Dataset dataset) {
        InternalDatasetDetails id = (InternalDatasetDetails) dataset.getDatasetDetails();
        return new Index(dataset.getDataverseName(), dataset.getDatasetName(), dataset.getDatasetName(),
                DatasetConfig.IndexType.BTREE, id.getPartitioningKey(), id.getKeySourceIndicator(),
                id.getPrimaryKeyType(), false, false, true, dataset.getPendingOp());
    }

    public static int[] getBtreeFieldsIfFiltered(Dataset dataset, Index index) throws AlgebricksException {
        if (index.isPrimaryIndex()) {
            return DatasetUtil.createBTreeFieldsWhenThereisAFilter(dataset);
        }
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
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
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
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

    public static JobSpecification buildDropIndexJobSpec(Index index, MetadataProvider metadataProvider,
            Dataset dataset) throws AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper = SecondaryIndexOperationsHelper
                .createIndexOperationsHelper(dataset, index, metadataProvider, physicalOptimizationConfig);
        return secondaryIndexHelper.buildDropJobSpec(false);
    }

    public static JobSpecification buildDropIndexJobSpec(Index index, MetadataProvider metadataProvider,
            Dataset dataset, boolean failSilently) throws AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper = SecondaryIndexOperationsHelper
                .createIndexOperationsHelper(dataset, index, metadataProvider, physicalOptimizationConfig);
        return secondaryIndexHelper.buildDropJobSpec(failSilently);
    }

    public static JobSpecification buildSecondaryIndexCreationJobSpec(Dataset dataset, Index index,
            MetadataProvider metadataProvider) throws AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper = SecondaryIndexOperationsHelper
                .createIndexOperationsHelper(dataset, index, metadataProvider, physicalOptimizationConfig);
        return secondaryIndexHelper.buildCreationJobSpec();
    }

    public static JobSpecification buildSecondaryIndexLoadingJobSpec(Dataset dataset, Index index,
            MetadataProvider metadataProvider) throws AlgebricksException {
        return buildSecondaryIndexLoadingJobSpec(dataset, index, metadataProvider, null);
    }

    public static JobSpecification buildSecondaryIndexLoadingJobSpec(Dataset dataset, Index index,
            MetadataProvider metadataProvider, List<ExternalFile> files) throws AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper;
        if (dataset.isCorrelated()) {
            secondaryIndexHelper = SecondaryCorrelatedTreeIndexOperationsHelper.createIndexOperationsHelper(dataset,
                    index, metadataProvider, physicalOptimizationConfig);
        } else {
            secondaryIndexHelper = SecondaryTreeIndexOperationsHelper.createIndexOperationsHelper(dataset, index,
                    metadataProvider, physicalOptimizationConfig);
        }
        if (files != null) {
            secondaryIndexHelper.setExternalFiles(files);
        }
        return secondaryIndexHelper.buildLoadingJobSpec();
    }

    public static JobSpecification buildSecondaryIndexCompactJobSpec(Dataset dataset, Index index,
            MetadataProvider metadataProvider) throws AlgebricksException {
        SecondaryIndexOperationsHelper secondaryIndexHelper = SecondaryIndexOperationsHelper
                .createIndexOperationsHelper(dataset, index, metadataProvider, physicalOptimizationConfig);
        return secondaryIndexHelper.buildCompactJobSpec();
    }

    /**
     * Binds a job event listener to the job specification.
     *
     * @param spec,
     *            the job specification.
     * @param metadataProvider,
     *            the metadata provider.
     * @return the AsterixDB job id for transaction management.
     */
    public static JobId bindJobEventListener(JobSpecification spec, MetadataProvider metadataProvider) {
        JobId jobId = JobIdFactory.generateJobId();
        metadataProvider.setJobId(jobId);
        boolean isWriteTransaction = metadataProvider.isWriteTransaction();
        IJobletEventListenerFactory jobEventListenerFactory = new JobEventListenerFactory(jobId, isWriteTransaction);
        spec.setJobletEventListenerFactory(jobEventListenerFactory);
        return jobId;
    }

}
