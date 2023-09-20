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

import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE;
import static org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor.DropOption;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.IColumnRangeFilterEvaluatorFactory;
import org.apache.asterix.column.operation.lsm.secondary.create.PrimaryScanColumnTupleProjectorFactory;
import org.apache.asterix.column.operation.lsm.secondary.upsert.UpsertPreviousColumnTupleProjectorFactory;
import org.apache.asterix.column.operation.query.QueryColumnTupleProjectorFactory;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.input.filter.NoOpExternalFilterEvaluatorFactory;
import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.asterix.metadata.dataset.DatasetFormatInfo;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.utils.filter.ColumnFilterBuilder;
import org.apache.asterix.metadata.utils.filter.ColumnRangeFilterBuilder;
import org.apache.asterix.metadata.utils.filter.ExternalFilterBuilder;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.runtime.projection.ColumnDatasetProjectionFiltrationInfo;
import org.apache.asterix.runtime.projection.ExternalDatasetProjectionFiltrationInfo;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.DefaultProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.impls.DefaultTupleProjectorFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpTupleProjectorFactory;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;
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
        return Index.createPrimaryIndex(dataset.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName(),
                id.getPartitioningKey(), id.getKeySourceIndicator(), id.getPrimaryKeyType(), dataset.getPendingOp());
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
     * @param spec,             the job specification.
     * @param metadataProvider, the metadata provider.
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

    public static ITupleProjectorFactory createTupleProjectorFactory(JobGenContext context,
            IVariableTypeEnvironment typeEnv, DatasetFormatInfo datasetFormatInfo,
            IProjectionFiltrationInfo projectionFiltrationInfo, ARecordType datasetType, ARecordType metaItemType,
            int numberOfPrimaryKeys) throws AlgebricksException {
        if (datasetFormatInfo.getFormat() == DatasetConfig.DatasetFormat.ROW) {
            return DefaultTupleProjectorFactory.INSTANCE;
        }

        if (projectionFiltrationInfo == DefaultProjectionFiltrationInfo.INSTANCE) {
            // pushdown is disabled
            ARecordType metaType = metaItemType == null ? null : ALL_FIELDS_TYPE;
            return new QueryColumnTupleProjectorFactory(datasetType, metaItemType, numberOfPrimaryKeys, ALL_FIELDS_TYPE,
                    Collections.emptyMap(), metaType, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                    NoOpColumnFilterEvaluatorFactory.INSTANCE);
        }
        ColumnDatasetProjectionFiltrationInfo columnInfo =
                (ColumnDatasetProjectionFiltrationInfo) projectionFiltrationInfo;

        ARecordType recordRequestedType = columnInfo.getProjectedType();
        ARecordType metaRequestedType = columnInfo.getMetaProjectedType();
        Map<String, FunctionCallInformation> callInfo = columnInfo.getFunctionCallInfoMap();

        ColumnRangeFilterBuilder columnRangeFilterBuilder = new ColumnRangeFilterBuilder(columnInfo);
        IColumnRangeFilterEvaluatorFactory rangeFilterEvaluatorFactory = columnRangeFilterBuilder.build();

        ColumnFilterBuilder columnFilterBuilder = new ColumnFilterBuilder(columnInfo, context, typeEnv);
        IColumnIterableFilterEvaluatorFactory columnFilterEvaluatorFactory = columnFilterBuilder.build();

        return new QueryColumnTupleProjectorFactory(datasetType, metaItemType, numberOfPrimaryKeys, recordRequestedType,
                callInfo, metaRequestedType, rangeFilterEvaluatorFactory, columnFilterEvaluatorFactory);
    }

    public static ITupleProjectorFactory createUpsertTupleProjectorFactory(DatasetFormatInfo datasetFormatInfo,
            ARecordType datasetRequestedType, ARecordType datasetType, ARecordType metaItemType,
            int numberOfPrimaryKeys) {
        if (datasetFormatInfo.getFormat() == DatasetConfig.DatasetFormat.ROW) {
            return NoOpTupleProjectorFactory.INSTANCE;
        }

        return new UpsertPreviousColumnTupleProjectorFactory(datasetType, metaItemType, numberOfPrimaryKeys,
                datasetRequestedType);
    }

    public static ITupleProjectorFactory createPrimaryIndexScanTupleProjectorFactory(
            DatasetFormatInfo datasetFormatInfo, ARecordType datasetRequestedType, ARecordType datasetType,
            ARecordType metaItemType, int numberOfPrimaryKeys) {
        if (datasetFormatInfo.getFormat() == DatasetConfig.DatasetFormat.ROW) {
            return DefaultTupleProjectorFactory.INSTANCE;
        }

        return new PrimaryScanColumnTupleProjectorFactory(datasetType, metaItemType, numberOfPrimaryKeys,
                datasetRequestedType);
    }

    public static IExternalFilterEvaluatorFactory createExternalFilterEvaluatorFactory(JobGenContext context,
            IVariableTypeEnvironment typeEnv, IProjectionFiltrationInfo projectionFiltrationInfo,
            Map<String, String> properties) throws AlgebricksException {
        if (projectionFiltrationInfo == DefaultProjectionFiltrationInfo.INSTANCE) {
            return NoOpExternalFilterEvaluatorFactory.INSTANCE;
        }

        ExternalDataPrefix prefix = new ExternalDataPrefix(properties);
        ExternalDatasetProjectionFiltrationInfo pfi =
                (ExternalDatasetProjectionFiltrationInfo) projectionFiltrationInfo;
        ExternalFilterBuilder build = new ExternalFilterBuilder(pfi, context, typeEnv, prefix);

        return build.build();
    }

}
