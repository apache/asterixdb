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

import static org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor.DropOption;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.util.OptionalBoolean;

public class IndexUtil {

    //TODO: replace this null with an empty array. currently, this breaks many tests
    private static final int[] empty = null;

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
        return Index.createPrimaryIndex(dataset.getDataverseName(), dataset.getDatasetName(), id.getPartitioningKey(),
                id.getKeySourceIndicator(), id.getPrimaryKeyType(), dataset.getPendingOp());
    }

    public static int[] getBtreeFieldsIfFiltered(Dataset dataset, Index index) throws AlgebricksException {
        if (index.isPrimaryIndex()) {
            return DatasetUtil.createBTreeFieldsWhenThereisAFilter(dataset);
        }
        int numSecondaryKeys;
        if (index.getIndexType() == DatasetConfig.IndexType.BTREE) {
            numSecondaryKeys = ((Index.ValueIndexDetails) index.getIndexDetails()).getKeyFieldNames().size();
        } else if (index.getIndexType() == DatasetConfig.IndexType.ARRAY) {
            numSecondaryKeys = ((Index.ArrayIndexDetails) index.getIndexDetails()).getElementList().stream()
                    .map(e -> e.getProjectList().size()).reduce(0, Integer::sum);
        } else if (index.getIndexType() == DatasetConfig.IndexType.SAMPLE) {
            return null;
        } else {
            throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE, index.getIndexType().toString());
        }
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
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
        int numSecondaryKeys;
        switch (index.getIndexType()) {
            case ARRAY:
                numSecondaryKeys = ((Index.ArrayIndexDetails) index.getIndexDetails()).getElementList().stream()
                        .map(e -> e.getProjectList().size()).reduce(0, Integer::sum);
                return new int[] { numPrimaryKeys + numSecondaryKeys };
            case BTREE:
                numSecondaryKeys = ((Index.ValueIndexDetails) index.getIndexDetails()).getKeyFieldNames().size();
                return new int[] { numPrimaryKeys + numSecondaryKeys };
            case RTREE:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case SINGLE_PARTITION_WORD_INVIX:
                break;
            case SAMPLE:
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE,
                        index.getIndexType().toString());
        }
        return empty;
    }

    public static JobSpecification buildDropIndexJobSpec(Index index, MetadataProvider metadataProvider,
            Dataset dataset, SourceLocation sourceLoc) throws AlgebricksException {
        ISecondaryIndexOperationsHelper secondaryIndexHelper =
                SecondaryIndexOperationsHelper.createIndexOperationsHelper(dataset, index, metadataProvider, sourceLoc);
        return secondaryIndexHelper.buildDropJobSpec(EnumSet.noneOf(DropOption.class));
    }

    public static JobSpecification buildDropIndexJobSpec(Index index, MetadataProvider metadataProvider,
            Dataset dataset, Set<DropOption> options, SourceLocation sourceLoc) throws AlgebricksException {
        ISecondaryIndexOperationsHelper secondaryIndexHelper =
                SecondaryIndexOperationsHelper.createIndexOperationsHelper(dataset, index, metadataProvider, sourceLoc);
        return secondaryIndexHelper.buildDropJobSpec(options);
    }

    public static JobSpecification buildSecondaryIndexCreationJobSpec(Dataset dataset, Index index,
            MetadataProvider metadataProvider, SourceLocation sourceLoc) throws AlgebricksException {
        ISecondaryIndexOperationsHelper secondaryIndexHelper =
                SecondaryIndexOperationsHelper.createIndexOperationsHelper(dataset, index, metadataProvider, sourceLoc);
        return secondaryIndexHelper.buildCreationJobSpec();
    }

    public static JobSpecification buildSecondaryIndexLoadingJobSpec(Dataset dataset, Index index,
            MetadataProvider metadataProvider, SourceLocation sourceLoc) throws AlgebricksException {
        return buildSecondaryIndexLoadingJobSpec(dataset, index, metadataProvider, null, sourceLoc);
    }

    public static JobSpecification buildSecondaryIndexLoadingJobSpec(Dataset dataset, Index index,
            MetadataProvider metadataProvider, List<ExternalFile> files, SourceLocation sourceLoc)
            throws AlgebricksException {
        ISecondaryIndexOperationsHelper secondaryIndexHelper;
        if (dataset.isCorrelated() && supportsCorrelated(index.getIndexType())) { //TODO:REVISIT
            secondaryIndexHelper = SecondaryCorrelatedTreeIndexOperationsHelper.createIndexOperationsHelper(dataset,
                    index, metadataProvider, sourceLoc);
        } else {
            secondaryIndexHelper = SecondaryTreeIndexOperationsHelper.createIndexOperationsHelper(dataset, index,
                    metadataProvider, sourceLoc);
        }
        if (files != null) {
            ((SecondaryIndexOperationsHelper) secondaryIndexHelper).setExternalFiles(files);
        }
        return secondaryIndexHelper.buildLoadingJobSpec();
    }

    private static boolean supportsCorrelated(DatasetConfig.IndexType indexType) {
        return indexType != DatasetConfig.IndexType.SAMPLE;
    }

    public static JobSpecification buildSecondaryIndexCompactJobSpec(Dataset dataset, Index index,
            MetadataProvider metadataProvider, SourceLocation sourceLoc) throws AlgebricksException {
        ISecondaryIndexOperationsHelper secondaryIndexHelper =
                SecondaryIndexOperationsHelper.createIndexOperationsHelper(dataset, index, metadataProvider, sourceLoc);
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
    public static void bindJobEventListener(JobSpecification spec, MetadataProvider metadataProvider)
            throws AlgebricksException {
        TxnId txnId = metadataProvider.getTxnIdFactory().create();
        metadataProvider.setTxnId(txnId);
        boolean isWriteTransaction = metadataProvider.isWriteTransaction();
        IJobletEventListenerFactory jobEventListenerFactory = new JobEventListenerFactory(txnId, isWriteTransaction);
        spec.setJobletEventListenerFactory(jobEventListenerFactory);
    }

    public static boolean castDefaultNull(Index index) {
        return Index.IndexCategory.of(index.getIndexType()) == Index.IndexCategory.VALUE
                && ((Index.ValueIndexDetails) index.getIndexDetails()).getCastDefaultNull().getOrElse(false);
    }

    public static Pair<FunctionIdentifier, IAObject> getTypeConstructorDefaultNull(Index index, IAType type,
            SourceLocation srcLoc) throws CompilationException {
        Triple<String, String, String> temporalFormats = getTemporalFormats(index);
        String format = temporalFormats != null ? TypeUtil.getTemporalFormat(type, temporalFormats) : null;
        boolean withFormat = format != null;
        FunctionIdentifier typeConstructorFun = TypeUtil.getTypeConstructorDefaultNull(type, withFormat);
        if (typeConstructorFun == null) {
            throw new CompilationException(ErrorCode.COMPILATION_TYPE_UNSUPPORTED, srcLoc, "index", type.getTypeName());
        }
        return new Pair<>(typeConstructorFun, withFormat ? new AString(format) : null);
    }

    private static Triple<String, String, String> getTemporalFormats(Index index) {
        if (Index.IndexCategory.of(index.getIndexType()) != Index.IndexCategory.VALUE) {
            return null;
        }
        Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
        String datetimeFormat = indexDetails.getCastDatetimeFormat();
        String dateFormat = indexDetails.getCastDateFormat();
        String timeFormat = indexDetails.getCastTimeFormat();
        if (datetimeFormat != null || dateFormat != null || timeFormat != null) {
            return new Triple<>(datetimeFormat, dateFormat, timeFormat);
        } else {
            return null;
        }
    }

    public static boolean includesUnknowns(Index index) {
        return !index.isPrimaryKeyIndex() && secondaryIndexIncludesUnknowns(index);
    }

    private static boolean secondaryIndexIncludesUnknowns(Index index) {
        if (Index.IndexCategory.of(index.getIndexType()) != Index.IndexCategory.VALUE) {
            // other types of indexes do not include unknowns
            return false;
        }
        OptionalBoolean excludeUnknownKey = ((Index.ValueIndexDetails) index.getIndexDetails()).getExcludeUnknownKey();
        if (index.getIndexType() == DatasetConfig.IndexType.BTREE) {
            // by default, Btree includes unknowns
            return excludeUnknownKey.isEmpty() || !excludeUnknownKey.get();
        } else {
            // by default, others exclude unknowns
            return !excludeUnknownKey.isEmpty() && !excludeUnknownKey.get();
        }
    }

    public static boolean excludesUnknowns(Index index) {
        return !includesUnknowns(index);
    }

    public static Pair<String, String> getSampleIndexNames(String datasetName) {
        return new Pair<>(MetadataConstants.SAMPLE_INDEX_1_PREFIX + datasetName,
                MetadataConstants.SAMPLE_INDEX_2_PREFIX + datasetName);
    }
}
