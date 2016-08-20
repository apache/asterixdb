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
package org.apache.asterix.metadata.declared;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.AsterixStorageProperties;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.ITransactionSubsystemProvider;
import org.apache.asterix.common.context.TransactionSubsystemProvider;
import org.apache.asterix.common.dataflow.AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor;
import org.apache.asterix.common.dataflow.AsterixLSMTreeInsertDeleteOperatorDescriptor;
import org.apache.asterix.common.dataflow.IAsterixApplicationContextInfo;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeWithBuddyIOOperationCallbackFactory;
import org.apache.asterix.common.ioopcallbacks.LSMInvertedIndexIOOperationCallbackFactory;
import org.apache.asterix.common.ioopcallbacks.LSMRTreeIOOperationCallbackFactory;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.transactions.IRecoveryManager.ResourceType;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.dataflow.data.nontagged.valueproviders.AqlPrimitiveValueProviderFactory;
import org.apache.asterix.external.adapter.factory.LookupAdapterFactory;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.api.IDataSourceAdapter;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.external.operators.ExternalBTreeSearchOperatorDescriptor;
import org.apache.asterix.external.operators.ExternalDataScanOperatorDescriptor;
import org.apache.asterix.external.operators.ExternalLookupOperatorDescriptor;
import org.apache.asterix.external.operators.ExternalRTreeSearchOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.provider.AdapterFactoryProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlLinearizeComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlTypeTraitProvider;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.dataset.hints.DatasetHints.DatasetCardinalityHint;
import org.apache.asterix.metadata.declared.AqlDataSource.AqlDataSourceType;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.metadata.utils.ExternalDatasetsRegistry;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.metadata.utils.SplitsAndConstraintsUtil;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.runtime.base.AsterixTupleFilterFactory;
import org.apache.asterix.runtime.formats.FormatUtils;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.runtime.operators.AsterixLSMInvertedIndexUpsertOperatorDescriptor;
import org.apache.asterix.runtime.operators.AsterixLSMTreeUpsertOperatorDescriptor;
import org.apache.asterix.transaction.management.opcallbacks.LockThenSearchOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexInstantSearchOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexModificationOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerProvider;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexModificationOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexSearchOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.TempDatasetPrimaryIndexModificationOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.TempDatasetSecondaryIndexModificationOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.UpsertOperationCallbackFactory;
import org.apache.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSink;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.DefaultNodeGroupDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.core.jobgen.impl.OperatorSchemaImpl;
import org.apache.hyracks.algebricks.data.IAWriterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.SinkWriterRuntimeFactory;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IResultSerializerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.PartitionedLSMInvertedIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.ExternalRTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.LSMRTreeWithAntiMatterTuplesDataflowHelperFactory;
import org.apache.hyracks.storage.am.rtree.dataflow.RTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;

public class AqlMetadataProvider implements IMetadataProvider<AqlSourceId, String> {

    private MetadataTransactionContext mdTxnCtx;
    private boolean isWriteTransaction;
    private final Map<String, String[]> stores;
    private Map<String, String> config;
    private IAWriterFactory writerFactory;
    private FileSplit outputFile;
    private boolean asyncResults;
    private ResultSetId resultSetId;
    private IResultSerializerFactoryProvider resultSerializerFactoryProvider;

    private final Dataverse defaultDataverse;
    private JobId jobId;
    private Map<String, Integer> locks;
    private boolean isTemporaryDatasetWriteJob = true;

    private final AsterixStorageProperties storageProperties;
    private final ILibraryManager libraryManager;

    public String getPropertyValue(String propertyName) {
        return config.get(propertyName);
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public Map<String, String[]> getAllStores() {
        return stores;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public AqlMetadataProvider(Dataverse defaultDataverse) {
        this.defaultDataverse = defaultDataverse;
        this.stores = AsterixAppContextInfo.getInstance().getMetadataProperties().getStores();
        this.storageProperties = AsterixAppContextInfo.getInstance().getStorageProperties();
        this.libraryManager = AsterixAppContextInfo.getInstance().getLibraryManager();
    }

    public ILibraryManager getLibraryManager() {
        return libraryManager;
    }

    public void setJobId(JobId jobId) {
        this.jobId = jobId;
    }

    public Dataverse getDefaultDataverse() {
        return defaultDataverse;
    }

    public String getDefaultDataverseName() {
        return defaultDataverse == null ? null : defaultDataverse.getDataverseName();
    }

    public void setWriteTransaction(boolean writeTransaction) {
        this.isWriteTransaction = writeTransaction;
    }

    public void setWriterFactory(IAWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    public void setMetadataTxnContext(MetadataTransactionContext mdTxnCtx) {
        this.mdTxnCtx = mdTxnCtx;
    }

    public MetadataTransactionContext getMetadataTxnContext() {
        return mdTxnCtx;
    }

    public IAWriterFactory getWriterFactory() {
        return this.writerFactory;
    }

    public FileSplit getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(FileSplit outputFile) {
        this.outputFile = outputFile;
    }

    public boolean getResultAsyncMode() {
        return asyncResults;
    }

    public void setResultAsyncMode(boolean asyncResults) {
        this.asyncResults = asyncResults;
    }

    public ResultSetId getResultSetId() {
        return resultSetId;
    }

    public void setResultSetId(ResultSetId resultSetId) {
        this.resultSetId = resultSetId;
    }

    public void setResultSerializerFactoryProvider(IResultSerializerFactoryProvider rafp) {
        this.resultSerializerFactoryProvider = rafp;
    }

    public IResultSerializerFactoryProvider getResultSerializerFactoryProvider() {
        return resultSerializerFactoryProvider;
    }

    /**
     * Retrieve the Output RecordType, as defined by "set output-record-type".
     */
    public ARecordType findOutputRecordType() throws AlgebricksException {
        String outputRecordType = getPropertyValue("output-record-type");
        if (outputRecordType == null) {
            return null;
        }
        String dataverse = getDefaultDataverseName();
        if (dataverse == null) {
            throw new AlgebricksException("Cannot declare output-record-type with no dataverse!");
        }
        IAType type = findType(dataverse, outputRecordType);
        if (!(type instanceof ARecordType)) {
            throw new AlgebricksException("Type " + outputRecordType + " is not a record type!");
        }
        return (ARecordType) type;
    }

    @Override
    public AqlDataSource findDataSource(AqlSourceId id) throws AlgebricksException {
        AqlSourceId aqlId = id;
        try {
            return lookupSourceInMetadata(aqlId);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public boolean isWriteTransaction() {
        // The transaction writes persistent datasets.
        return isWriteTransaction;
    }

    public boolean isTemporaryDatasetWriteJob() {
        // The transaction only writes temporary datasets.
        return isTemporaryDatasetWriteJob;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(
            IDataSource<AqlSourceId> dataSource, List<LogicalVariable> scanVariables,
            List<LogicalVariable> projectVariables, boolean projectPushed, List<LogicalVariable> minFilterVars,
            List<LogicalVariable> maxFilterVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            JobGenContext context, JobSpecification jobSpec, Object implConfig) throws AlgebricksException {
        try {
            return ((AqlDataSource) dataSource).buildDatasourceScanRuntime(this, dataSource, scanVariables,
                    projectVariables, projectPushed, minFilterVars, maxFilterVars, opSchema, typeEnv, context, jobSpec,
                    implConfig);
        } catch (AsterixException e) {
            throw new AlgebricksException(e);
        }
    }

    public static AlgebricksAbsolutePartitionConstraint determineLocationConstraint(FeedDataSource feedDataSource)
            throws AsterixException {
        return new AlgebricksAbsolutePartitionConstraint(feedDataSource.getLocations());
    }

    protected Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildLoadableDatasetScan(
            JobSpecification jobSpec, IAdapterFactory adapterFactory, RecordDescriptor rDesc)
            throws AlgebricksException {
        ExternalDataScanOperatorDescriptor dataScanner = new ExternalDataScanOperatorDescriptor(jobSpec, rDesc,
                adapterFactory);
        AlgebricksPartitionConstraint constraint;
        try {
            constraint = adapterFactory.getPartitionConstraint();
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
        return new Pair<>(dataScanner, constraint);
    }

    public IDataFormat getDataFormat(String dataverseName) throws AsterixException {
        Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
        IDataFormat format;
        try {
            format = (IDataFormat) Class.forName(dataverse.getDataFormat()).newInstance();
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return format;
    }

    protected IAdapterFactory getConfiguredAdapterFactory(Dataset dataset, String adapterName,
            Map<String, String> configuration, ARecordType itemType, boolean isPKAutoGenerated,
            List<List<String>> primaryKeys, ARecordType metaType) throws AlgebricksException {
        try {
            configuration.put(ExternalDataConstants.KEY_DATAVERSE, dataset.getDataverseName());
            IAdapterFactory adapterFactory = AdapterFactoryProvider.getAdapterFactory(libraryManager, adapterName,
                    configuration, itemType, metaType);

            // check to see if dataset is indexed
            Index filesIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(),
                    dataset.getDatasetName().concat(IndexingConstants.EXTERNAL_FILE_INDEX_NAME_SUFFIX));

            if (filesIndex != null && filesIndex.getPendingOp() == 0) {
                // get files
                List<ExternalFile> files = MetadataManager.INSTANCE.getDatasetExternalFiles(mdTxnCtx, dataset);
                Iterator<ExternalFile> iterator = files.iterator();
                while (iterator.hasNext()) {
                    if (iterator.next().getPendingOp() != ExternalFilePendingOp.PENDING_NO_OP) {
                        iterator.remove();
                    }
                }
                // TODO Check this call, result of merge from master!
                // ((IGenericAdapterFactory) adapterFactory).setFiles(files);
            }

            return adapterFactory;
        } catch (Exception e) {
            throw new AlgebricksException("Unable to create adapter", e);
        }
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDatasetDataScannerRuntime(
            JobSpecification jobSpec, IAType itemType, IAdapterFactory adapterFactory, IDataFormat format)
            throws AlgebricksException {
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Can only scan datasets of records.");
        }

        ISerializerDeserializer payloadSerde = format.getSerdeProvider().getSerializerDeserializer(itemType);
        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] { payloadSerde });

        ExternalDataScanOperatorDescriptor dataScanner = new ExternalDataScanOperatorDescriptor(jobSpec, scannerDesc,
                adapterFactory);

        AlgebricksPartitionConstraint constraint;
        try {
            constraint = adapterFactory.getPartitionConstraint();
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }

        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(dataScanner, constraint);
    }

    public Triple<IOperatorDescriptor, AlgebricksPartitionConstraint, IAdapterFactory> buildFeedIntakeRuntime(
            JobSpecification jobSpec, Feed primaryFeed, FeedPolicyAccessor policyAccessor) throws Exception {
        Triple<IAdapterFactory, RecordDescriptor, IDataSourceAdapter.AdapterType> factoryOutput = null;
        factoryOutput = FeedMetadataUtil.getPrimaryFeedFactoryAndOutput(primaryFeed, policyAccessor, mdTxnCtx,
                libraryManager);
        ARecordType recordType = FeedMetadataUtil.getOutputType(primaryFeed, primaryFeed.getAdapterConfiguration(),
                ExternalDataConstants.KEY_TYPE_NAME);
        IAdapterFactory adapterFactory = factoryOutput.first;
        FeedIntakeOperatorDescriptor feedIngestor = null;
        switch (factoryOutput.third) {
            case INTERNAL:
                feedIngestor = new FeedIntakeOperatorDescriptor(jobSpec, primaryFeed, adapterFactory, recordType,
                        policyAccessor, factoryOutput.second);
                break;
            case EXTERNAL:
                String libraryName = primaryFeed.getAdapterName().trim()
                        .split(FeedConstants.NamingConstants.LIBRARY_NAME_SEPARATOR)[0];
                feedIngestor = new FeedIntakeOperatorDescriptor(jobSpec, primaryFeed, libraryName,
                        adapterFactory.getClass().getName(), recordType, policyAccessor, factoryOutput.second);
                break;
        }

        AlgebricksPartitionConstraint partitionConstraint = adapterFactory.getPartitionConstraint();
        return new Triple<IOperatorDescriptor, AlgebricksPartitionConstraint, IAdapterFactory>(feedIngestor,
                partitionConstraint, adapterFactory);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildBtreeRuntime(JobSpecification jobSpec,
            List<LogicalVariable> outputVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            JobGenContext context, boolean retainInput, boolean retainMissing, Dataset dataset, String indexName,
            int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            Object implConfig, int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes) throws AlgebricksException {

        boolean isSecondary = true;
        int numSecondaryKeys = 0;
        try {
            boolean temp = dataset.getDatasetDetails().isTemp();
            Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), dataset.getDatasetName());
            if (primaryIndex != null && (dataset.getDatasetType() != DatasetType.EXTERNAL)) {
                isSecondary = !indexName.equals(primaryIndex.getIndexName());
            }
            int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
            RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
            int[] bloomFilterKeyFields;
            ITypeTraits[] typeTraits;
            IBinaryComparatorFactory[] comparatorFactories;

            ARecordType itemType = (ARecordType) this.findType(dataset.getItemTypeDataverseName(),
                    dataset.getItemTypeName());
            ARecordType metaType = null;
            List<Integer> primaryKeyIndicators = null;
            if (dataset.hasMetaPart()) {
                metaType = (ARecordType) findType(dataset.getMetaItemTypeDataverseName(),
                        dataset.getMetaItemTypeName());
                primaryKeyIndicators = ((InternalDatasetDetails) dataset.getDatasetDetails()).getKeySourceIndicator();
            }

            ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, itemType);
            IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.
                    computeFilterBinaryComparatorFactories(dataset,
                    itemType, context.getBinaryComparatorFactoryProvider());
            int[] filterFields = null;
            int[] btreeFields = null;

            if (isSecondary) {
                Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                        dataset.getDatasetName(), indexName);
                numSecondaryKeys = secondaryIndex.getKeyFieldNames().size();
                bloomFilterKeyFields = new int[numSecondaryKeys];
                for (int i = 0; i < numSecondaryKeys; i++) {
                    bloomFilterKeyFields[i] = i;
                }
                Pair<IBinaryComparatorFactory[], ITypeTraits[]> comparatorFactoriesAndTypeTraits =
                        getComparatorFactoriesAndTypeTraitsOfSecondaryBTreeIndex(
                        secondaryIndex.getIndexType(), secondaryIndex.getKeyFieldNames(),
                        secondaryIndex.getKeyFieldTypes(), DatasetUtils.getPartitioningKeys(dataset), itemType,
                        dataset.getDatasetType(), dataset.hasMetaPart(), primaryKeyIndicators,
                        secondaryIndex.getKeyFieldSourceIndicators(), metaType);
                comparatorFactories = comparatorFactoriesAndTypeTraits.first;
                typeTraits = comparatorFactoriesAndTypeTraits.second;
                if (filterTypeTraits != null) {
                    filterFields = new int[1];
                    filterFields[0] = numSecondaryKeys + numPrimaryKeys;
                    btreeFields = new int[numSecondaryKeys + numPrimaryKeys];
                    for (int k = 0; k < btreeFields.length; k++) {
                        btreeFields[k] = k;
                    }
                }

            } else {
                bloomFilterKeyFields = new int[numPrimaryKeys];
                for (int i = 0; i < numPrimaryKeys; i++) {
                    bloomFilterKeyFields[i] = i;
                }
                // get meta item type
                ARecordType metaItemType = DatasetUtils.getMetaType(this, dataset);
                typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType, metaItemType);
                comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset, itemType,
                        metaItemType, context.getBinaryComparatorFactoryProvider());
                filterFields = DatasetUtils.createFilterFields(dataset);
                btreeFields = DatasetUtils.createBTreeFieldsWhenThereisAFilter(dataset);
            }

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc;
            try {
                spPc = splitProviderAndPartitionConstraintsForDataset(dataset.getDataverseName(),
                        dataset.getDatasetName(), indexName, temp);
            } catch (Exception e) {
                throw new AlgebricksException(e);
            }

            ISearchOperationCallbackFactory searchCallbackFactory = null;
            if (isSecondary) {
                searchCallbackFactory = temp ? NoOpOperationCallbackFactory.INSTANCE
                        : new SecondaryIndexSearchOperationCallbackFactory();
            } else {
                JobId jobId = ((JobEventListenerFactory) jobSpec.getJobletEventListenerFactory()).getJobId();
                int datasetId = dataset.getDatasetId();
                int[] primaryKeyFields = new int[numPrimaryKeys];
                for (int i = 0; i < numPrimaryKeys; i++) {
                    primaryKeyFields[i] = i;
                }

                ITransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();

                /**
                 * Due to the read-committed isolation level,
                 * we may acquire very short duration lock(i.e., instant lock) for readers.
                 */
                searchCallbackFactory = temp ? NoOpOperationCallbackFactory.INSTANCE
                        : new PrimaryIndexInstantSearchOperationCallbackFactory(jobId, datasetId, primaryKeyFields,
                                txnSubsystemProvider, ResourceType.LSM_BTREE);
            }
            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils
                    .getMergePolicyFactory(dataset, mdTxnCtx);
            AsterixRuntimeComponentsProvider rtcProvider = AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER;
            BTreeSearchOperatorDescriptor btreeSearchOp;
            if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                btreeSearchOp = new BTreeSearchOperatorDescriptor(jobSpec, outputRecDesc,
                        appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                        spPc.first, typeTraits, comparatorFactories, bloomFilterKeyFields, lowKeyFields, highKeyFields,
                        lowKeyInclusive, highKeyInclusive,
                        new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                                compactionInfo.first, compactionInfo.second,
                                isSecondary ? new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId())
                                        : new PrimaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                                rtcProvider, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                                storageProperties.getBloomFilterFalsePositiveRate(), !isSecondary, filterTypeTraits,
                                filterCmpFactories, btreeFields, filterFields, !temp),
                        retainInput, retainMissing, context.getMissingWriterFactory(), searchCallbackFactory,
                        minFilterFieldIndexes, maxFilterFieldIndexes);
            } else {
                // External dataset <- use the btree with buddy btree->
                // Be Careful of Key Start Index ?
                int[] buddyBreeFields = new int[] { numSecondaryKeys };
                ExternalBTreeWithBuddyDataflowHelperFactory indexDataflowHelperFactory =
                        new ExternalBTreeWithBuddyDataflowHelperFactory(
                        compactionInfo.first, compactionInfo.second,
                        new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                        LSMBTreeWithBuddyIOOperationCallbackFactory.INSTANCE,
                        getStorageProperties().getBloomFilterFalsePositiveRate(), buddyBreeFields,
                        ExternalDatasetsRegistry.INSTANCE.getAndLockDatasetVersion(dataset, this), !temp);
                btreeSearchOp = new ExternalBTreeSearchOperatorDescriptor(jobSpec, outputRecDesc, rtcProvider,
                        rtcProvider, spPc.first, typeTraits, comparatorFactories, bloomFilterKeyFields, lowKeyFields,
                        highKeyFields, lowKeyInclusive, highKeyInclusive, indexDataflowHelperFactory, retainInput,
                        retainMissing, context.getMissingWriterFactory(), searchCallbackFactory);
            }

            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeSearchOp, spPc.second);

        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    private Pair<IBinaryComparatorFactory[], ITypeTraits[]> getComparatorFactoriesAndTypeTraitsOfSecondaryBTreeIndex(
            IndexType indexType, List<List<String>> sidxKeyFieldNames, List<IAType> sidxKeyFieldTypes,
            List<List<String>> pidxKeyFieldNames, ARecordType recType, DatasetType dsType, boolean hasMeta,
            List<Integer> primaryIndexKeyIndicators, List<Integer> secondaryIndexIndicators, ARecordType metaType)
            throws AlgebricksException {

        IBinaryComparatorFactory[] comparatorFactories;
        ITypeTraits[] typeTraits;
        int sidxKeyFieldCount = sidxKeyFieldNames.size();
        int pidxKeyFieldCount = pidxKeyFieldNames.size();
        typeTraits = new ITypeTraits[sidxKeyFieldCount + pidxKeyFieldCount];
        comparatorFactories = new IBinaryComparatorFactory[sidxKeyFieldCount + pidxKeyFieldCount];

        int i = 0;
        for (; i < sidxKeyFieldCount; ++i) {
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(sidxKeyFieldTypes.get(i),
                    sidxKeyFieldNames.get(i),
                    (hasMeta && secondaryIndexIndicators.get(i).intValue() == 1) ? metaType : recType);
            IAType keyType = keyPairType.first;
            comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType,
                    true);
            typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
        }

        for (int j = 0; j < pidxKeyFieldCount; ++j, ++i) {
            IAType keyType = null;
            try {
                switch (dsType) {
                    case INTERNAL:
                        keyType = (hasMeta && primaryIndexKeyIndicators.get(j).intValue() == 1)
                                ? metaType.getSubFieldType(pidxKeyFieldNames.get(j))
                                : recType.getSubFieldType(pidxKeyFieldNames.get(j));
                        break;
                    case EXTERNAL:
                        keyType = IndexingConstants.getFieldType(j);
                        break;
                    default:
                        throw new AlgebricksException("Unknown Dataset Type");
                }
            } catch (AsterixException e) {
                throw new AlgebricksException(e);
            }
            comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType,
                    true);
            typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
        }

        return new Pair<IBinaryComparatorFactory[], ITypeTraits[]>(comparatorFactories, typeTraits);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildRtreeRuntime(JobSpecification jobSpec,
            List<LogicalVariable> outputVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            JobGenContext context, boolean retainInput, boolean retainMissing, Dataset dataset, String indexName,
            int[] keyFields, int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes) throws AlgebricksException {

        try {
            ARecordType recType = (ARecordType) findType(dataset.getItemTypeDataverseName(),
                    dataset.getItemTypeName());
            int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();

            boolean temp = dataset.getDatasetDetails().isTemp();
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
            if (secondaryIndex == null) {
                throw new AlgebricksException(
                        "Code generation error: no index " + indexName + " for dataset " + dataset.getDatasetName());
            }
            List<List<String>> secondaryKeyFields = secondaryIndex.getKeyFieldNames();
            List<IAType> secondaryKeyTypes = secondaryIndex.getKeyFieldTypes();
            int numSecondaryKeys = secondaryKeyFields.size();
            if (numSecondaryKeys != 1) {
                throw new AlgebricksException("Cannot use " + numSecondaryKeys
                        + " fields as a key for the R-tree index. "
                        + "There can be only one field as a key for the R-tree index.");
            }
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(0),
                    secondaryKeyFields.get(0), recType);
            IAType keyType = keyTypePair.first;
            if (keyType == null) {
                throw new AlgebricksException("Could not find field " + secondaryKeyFields.get(0) + " in the schema.");
            }
            int numDimensions = NonTaggedFormatUtil.getNumDimensions(keyType.getTypeTag());
            boolean isPointMBR = keyType.getTypeTag() == ATypeTag.POINT || keyType.getTypeTag() == ATypeTag.POINT3D;
            int numNestedSecondaryKeyFields = numDimensions * 2;
            IPrimitiveValueProviderFactory[] valueProviderFactories =
                    new IPrimitiveValueProviderFactory[numNestedSecondaryKeyFields];
            for (int i = 0; i < numNestedSecondaryKeyFields; i++) {
                valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
            }

            RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
            // IS NOT THE VARIABLE BELOW ALWAYS = 0 ??
            int keysStartIndex = outputRecDesc.getFieldCount() - numNestedSecondaryKeyFields - numPrimaryKeys;
            if (retainInput) {
                keysStartIndex -= numNestedSecondaryKeyFields;
            }
            IBinaryComparatorFactory[] comparatorFactories = JobGenHelper.variablesToAscBinaryComparatorFactories(
                    outputVars, keysStartIndex, numNestedSecondaryKeyFields, typeEnv, context);
            ITypeTraits[] typeTraits = JobGenHelper.variablesToTypeTraits(outputVars, keysStartIndex,
                    numNestedSecondaryKeyFields + numPrimaryKeys, typeEnv, context);
            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc =
                    splitProviderAndPartitionConstraintsForDataset(
                    dataset.getDataverseName(), dataset.getDatasetName(), indexName, temp);
            ARecordType metaType = null;
            if (dataset.hasMetaPart()) {
                metaType = (ARecordType) findType(dataset.getMetaItemTypeDataverseName(),
                        dataset.getMetaItemTypeName());
            }

            IBinaryComparatorFactory[] primaryComparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(
                    dataset, recType, metaType, context.getBinaryComparatorFactoryProvider());
            int[] btreeFields = new int[primaryComparatorFactories.length];
            for (int i = 0; i < btreeFields.length; i++) {
                btreeFields[i] = i + numNestedSecondaryKeyFields;
            }

            ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, recType);
            IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.
                    computeFilterBinaryComparatorFactories(dataset,
                    recType, context.getBinaryComparatorFactoryProvider());
            int[] filterFields = null;
            int[] rtreeFields = null;
            if (filterTypeTraits != null) {
                filterFields = new int[1];
                filterFields[0] = numNestedSecondaryKeyFields + numPrimaryKeys;
                rtreeFields = new int[numNestedSecondaryKeyFields + numPrimaryKeys];
                for (int i = 0; i < rtreeFields.length; i++) {
                    rtreeFields[i] = i;
                }
            }

            IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());
            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils
                    .getMergePolicyFactory(dataset, mdTxnCtx);
            ISearchOperationCallbackFactory searchCallbackFactory = temp ? NoOpOperationCallbackFactory.INSTANCE
                    : new SecondaryIndexSearchOperationCallbackFactory();

            RTreeSearchOperatorDescriptor rtreeSearchOp;
            if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                IBinaryComparatorFactory[] deletedKeyBTreeCompFactories = getMergedComparatorFactories(
                        comparatorFactories, primaryComparatorFactories);
                IIndexDataflowHelperFactory idff = new LSMRTreeWithAntiMatterTuplesDataflowHelperFactory(
                        valueProviderFactories, RTreePolicyType.RTREE, deletedKeyBTreeCompFactories,
                        new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                        compactionInfo.second, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMRTreeIOOperationCallbackFactory.INSTANCE,
                        AqlMetadataProvider.proposeLinearizer(nestedKeyType.getTypeTag(), comparatorFactories.length),
                        rtreeFields, filterTypeTraits, filterCmpFactories, filterFields, !temp, isPointMBR);
                rtreeSearchOp = new RTreeSearchOperatorDescriptor(jobSpec, outputRecDesc,
                        appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                        spPc.first, typeTraits, comparatorFactories, keyFields, idff, retainInput, retainMissing,
                        context.getMissingWriterFactory(), searchCallbackFactory, minFilterFieldIndexes,
                        maxFilterFieldIndexes);
            } else {
                // External Dataset
                ExternalRTreeDataflowHelperFactory indexDataflowHelperFactory = new ExternalRTreeDataflowHelperFactory(
                        valueProviderFactories, RTreePolicyType.RTREE,
                        IndexingConstants.getBuddyBtreeComparatorFactories(), compactionInfo.first,
                        compactionInfo.second, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMRTreeIOOperationCallbackFactory.INSTANCE,
                        proposeLinearizer(nestedKeyType.getTypeTag(), comparatorFactories.length),
                        getStorageProperties().getBloomFilterFalsePositiveRate(),
                        new int[] { numNestedSecondaryKeyFields },
                        ExternalDatasetsRegistry.INSTANCE.getAndLockDatasetVersion(dataset, this), !temp, isPointMBR);
                // Create the operator
                rtreeSearchOp = new ExternalRTreeSearchOperatorDescriptor(jobSpec, outputRecDesc,
                        appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                        spPc.first, typeTraits, comparatorFactories, keyFields, indexDataflowHelperFactory, retainInput,
                        retainMissing, context.getMissingWriterFactory(), searchCallbackFactory);
            }

            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(rtreeSearchOp, spPc.second);

        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    private IBinaryComparatorFactory[] getMergedComparatorFactories(IBinaryComparatorFactory[] comparatorFactories,
            IBinaryComparatorFactory[] primaryComparatorFactories) {
        IBinaryComparatorFactory[] btreeCompFactories = null;
        int btreeCompFactoriesCount = comparatorFactories.length + primaryComparatorFactories.length;
        btreeCompFactories = new IBinaryComparatorFactory[btreeCompFactoriesCount];
        int i = 0;
        for (; i < comparatorFactories.length; i++) {
            btreeCompFactories[i] = comparatorFactories[i];
        }
        for (int j = 0; i < btreeCompFactoriesCount; i++, j++) {
            btreeCompFactories[i] = primaryComparatorFactories[j];
        }
        return btreeCompFactories;
    }

    @Override
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc) {
        FileSplitDataSink fsds = (FileSplitDataSink) sink;
        FileSplitSinkId fssi = fsds.getId();
        FileSplit fs = fssi.getFileSplit();
        File outFile = fs.getLocalFile().getFile();
        String nodeId = fs.getNodeName();

        SinkWriterRuntimeFactory runtime = new SinkWriterRuntimeFactory(printColumns, printerFactories, outFile,
                getWriterFactory(), inputDesc);
        AlgebricksPartitionConstraint apc = new AlgebricksAbsolutePartitionConstraint(new String[] { nodeId });
        return new Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint>(runtime, apc);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getResultHandleRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc, boolean ordered,
            JobSpecification spec) throws AlgebricksException {
        ResultSetDataSink rsds = (ResultSetDataSink) sink;
        ResultSetSinkId rssId = rsds.getId();
        ResultSetId rsId = rssId.getResultSetId();

        ResultWriterOperatorDescriptor resultWriter = null;
        try {
            IResultSerializerFactory resultSerializedAppenderFactory = resultSerializerFactoryProvider
                    .getAqlResultSerializerFactoryProvider(printColumns, printerFactories, getWriterFactory());
            resultWriter = new ResultWriterOperatorDescriptor(spec, rsId, ordered, getResultAsyncMode(),
                    resultSerializedAppenderFactory);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }

        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(resultWriter, null);
    }

    @Override
    public IDataSourceIndex<String, AqlSourceId> findDataSourceIndex(String indexId, AqlSourceId dataSourceId)
            throws AlgebricksException {
        AqlDataSource ads = findDataSource(dataSourceId);
        Dataset dataset = ((DatasetDataSource) ads).getDataset();

        try {
            String indexName = indexId;
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
            if (secondaryIndex != null) {
                return new AqlIndex(secondaryIndex, dataset.getDataverseName(), dataset.getDatasetName(), this);
            } else {
                Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                        dataset.getDatasetName(), dataset.getDatasetName());
                if (primaryIndex.getIndexName().equals(indexId)) {
                    return new AqlIndex(primaryIndex, dataset.getDataverseName(), dataset.getDatasetName(), this);
                } else {
                    return null;
                }
            }
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    public AqlDataSource lookupSourceInMetadata(AqlSourceId aqlId) throws AlgebricksException, MetadataException {
        Dataset dataset = findDataset(aqlId.getDataverseName(), aqlId.getDatasourceName());
        if (dataset == null) {
            throw new AlgebricksException("Datasource with id " + aqlId + " was not found.");
        }
        IAType itemType = findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        IAType metaItemType = findType(dataset.getMetaItemTypeDataverseName(), dataset.getMetaItemTypeName());
        INodeDomain domain = findNodeDomain(dataset.getNodeGroupName());
        AqlDataSourceType datasourceType = dataset.getDatasetType().equals(DatasetType.EXTERNAL)
                ? AqlDataSourceType.EXTERNAL_DATASET : AqlDataSourceType.INTERNAL_DATASET;
        return new DatasetDataSource(aqlId, dataset, itemType, metaItemType, datasourceType,
                dataset.getDatasetDetails(), domain);
    }

    @Override
    public boolean scannerOperatorIsLeaf(IDataSource<AqlSourceId> dataSource) {
        boolean result = false;
        switch (((AqlDataSource) dataSource).getDatasourceType()) {
            case INTERNAL_DATASET:
            case EXTERNAL_DATASET:
                result = ((DatasetDataSource) dataSource).getDataset().getDatasetType() == DatasetType.EXTERNAL;
                break;
            case FEED:
                result = true;
                break;
            case LOADABLE:
                result = true;
                break;
            default:
                break;
        }
        return result;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payload, List<LogicalVariable> additionalNonKeyFields, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        String dataverseName = dataSource.getId().getDataverseName();
        String datasetName = dataSource.getId().getDatasourceName();

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }

        int numKeys = keys.size();
        int numFilterFields = DatasetUtils.getFilterField(dataset) == null ? 0 : 1;

        // move key fields to front
        int[] fieldPermutation = new int[numKeys + 1 + numFilterFields];
        int[] bloomFilterKeyFields = new int[numKeys];
        // System.arraycopy(keys, 0, fieldPermutation, 0, numKeys);
        int i = 0;
        for (LogicalVariable varKey : keys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            bloomFilterKeyFields[i] = i;
            i++;
        }
        fieldPermutation[numKeys] = propagatedSchema.findVariable(payload);
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalNonKeyFields.get(0));
            fieldPermutation[numKeys + 1] = idx;
        }

        try {
            boolean temp = dataset.getDatasetDetails().isTemp();
            isTemporaryDatasetWriteJob = isTemporaryDatasetWriteJob && temp;

            Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), dataset.getDatasetName());
            String indexName = primaryIndex.getIndexName();

            ARecordType metaType = null;
            if (dataset.hasMetaPart()) {
                metaType = (ARecordType) findType(dataset.getMetaItemTypeDataverseName(),
                        dataset.getMetaItemTypeName());
            }

            String itemTypeName = dataset.getItemTypeName();
            ARecordType itemType = (ARecordType) MetadataManager.INSTANCE
                    .getDatatype(mdTxnCtx, dataset.getItemTypeDataverseName(), itemTypeName).getDatatype();
            ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType, null);
            IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                    itemType, metaType, context.getBinaryComparatorFactoryProvider());

            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    splitProviderAndPartitionConstraintsForDataset(
                    dataSource.getId().getDataverseName(), datasetName, indexName, temp);
            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();

            long numElementsHint = getCardinalityPerPartitionHint(dataset);

            ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, itemType);
            IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.
                    computeFilterBinaryComparatorFactories(dataset,
                    itemType, context.getBinaryComparatorFactoryProvider());
            int[] filterFields = DatasetUtils.createFilterFields(dataset);
            int[] btreeFields = DatasetUtils.createBTreeFieldsWhenThereisAFilter(dataset);

            // TODO
            // figure out the right behavior of the bulkload and then give the
            // right callback
            // (ex. what's the expected behavior when there is an error during
            // bulkload?)
            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils
                    .getMergePolicyFactory(dataset, mdTxnCtx);
            TreeIndexBulkLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec, null,
                    appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                    splitsAndConstraint.first, typeTraits, comparatorFactories, bloomFilterKeyFields, fieldPermutation,
                    GlobalConfig.DEFAULT_TREE_FILL_FACTOR, false, numElementsHint, true,
                    new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                            compactionInfo.first, compactionInfo.second,
                            new PrimaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE,
                            storageProperties.getBloomFilterFalsePositiveRate(), true, filterTypeTraits,
                            filterCmpFactories, btreeFields, filterFields, !temp));
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeBulkLoad,
                    splitsAndConstraint.second);
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertOrDeleteRuntime(IndexOperation indexOp,
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payload, List<LogicalVariable> additionalNonKeyFields,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec, boolean bulkload,
            List<LogicalVariable> additionalNonFilteringFields) throws AlgebricksException {

        String datasetName = dataSource.getId().getDatasourceName();
        Dataset dataset = findDataset(dataSource.getId().getDataverseName(), datasetName);
        if (dataset == null) {
            throw new AlgebricksException(
                    "Unknown dataset " + datasetName + " in dataverse " + dataSource.getId().getDataverseName());
        }
        boolean temp = dataset.getDatasetDetails().isTemp();
        isTemporaryDatasetWriteJob = isTemporaryDatasetWriteJob && temp;

        int numKeys = keys.size();
        int numFilterFields = DatasetUtils.getFilterField(dataset) == null ? 0 : 1;
        // Move key fields to front.
        int[] fieldPermutation = new int[numKeys + 1 + numFilterFields
                + (additionalNonFilteringFields == null ? 0 : additionalNonFilteringFields.size())];
        int[] bloomFilterKeyFields = new int[numKeys];
        int i = 0;
        for (LogicalVariable varKey : keys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            bloomFilterKeyFields[i] = i;
            i++;
        }
        fieldPermutation[i++] = propagatedSchema.findVariable(payload);
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalNonKeyFields.get(0));
            fieldPermutation[i++] = idx;
        }
        if (additionalNonFilteringFields != null) {
            for (LogicalVariable variable : additionalNonFilteringFields) {
                int idx = propagatedSchema.findVariable(variable);
                fieldPermutation[i++] = idx;
            }
        }

        try {
            Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), dataset.getDatasetName());
            String indexName = primaryIndex.getIndexName();
            ARecordType itemType = (ARecordType) MetadataManager.INSTANCE
                    .getDatatype(mdTxnCtx, dataset.getItemTypeDataverseName(), dataset.getItemTypeName()).getDatatype();
            ARecordType metaItemType = DatasetUtils.getMetaType(this, dataset);
            ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType, metaItemType);

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                    itemType, metaItemType, context.getBinaryComparatorFactoryProvider());
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    splitProviderAndPartitionConstraintsForDataset(
                    dataSource.getId().getDataverseName(), datasetName, indexName, temp);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            int[] primaryKeyFields = new int[numKeys];
            for (i = 0; i < numKeys; i++) {
                primaryKeyFields[i] = i;
            }

            ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, itemType);
            IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.computeFilterBinaryComparatorFactories(dataset,
                    itemType, context.getBinaryComparatorFactoryProvider());
            int[] filterFields = DatasetUtils.createFilterFields(dataset);
            int[] btreeFields = DatasetUtils.createBTreeFieldsWhenThereisAFilter(dataset);

            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            IModificationOperationCallbackFactory modificationCallbackFactory = temp
                    ? new TempDatasetPrimaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            primaryKeyFields, txnSubsystemProvider, indexOp, ResourceType.LSM_BTREE)
                    : new PrimaryIndexModificationOperationCallbackFactory(jobId, datasetId, primaryKeyFields,
                            txnSubsystemProvider, indexOp, ResourceType.LSM_BTREE, dataset.hasMetaPart());

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils
                    .getMergePolicyFactory(dataset, mdTxnCtx);
            IIndexDataflowHelperFactory idfh = new LSMBTreeDataflowHelperFactory(
                    new AsterixVirtualBufferCacheProvider(datasetId), compactionInfo.first, compactionInfo.second,
                    new PrimaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), true, filterTypeTraits, filterCmpFactories,
                    btreeFields, filterFields, !temp);
            IOperatorDescriptor op;
            if (bulkload) {
                long numElementsHint = getCardinalityPerPartitionHint(dataset);
                op = new TreeIndexBulkLoadOperatorDescriptor(spec, recordDesc, appContext.getStorageManagerInterface(),
                        appContext.getIndexLifecycleManagerProvider(), splitsAndConstraint.first, typeTraits,
                        comparatorFactories, bloomFilterKeyFields, fieldPermutation,
                        GlobalConfig.DEFAULT_TREE_FILL_FACTOR, true, numElementsHint, true, idfh);
            } else {
                op = new AsterixLSMTreeInsertDeleteOperatorDescriptor(spec, recordDesc,
                        appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                        splitsAndConstraint.first, typeTraits, comparatorFactories, bloomFilterKeyFields,
                        fieldPermutation, indexOp, idfh, null, true, indexName, null, modificationCallbackFactory,
                        NoOpOperationCallbackFactory.INSTANCE);
            }
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(op, splitsAndConstraint.second);

        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payload, List<LogicalVariable> additionalNonKeyFields,
            List<LogicalVariable> additionalNonFilteringFields, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec, boolean bulkload) throws AlgebricksException {
        return getInsertOrDeleteRuntime(IndexOperation.INSERT, dataSource, propagatedSchema, typeEnv, keys, payload,
                additionalNonKeyFields, recordDesc, context, spec, bulkload, additionalNonFilteringFields);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payload, List<LogicalVariable> additionalNonKeyFields,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec) throws AlgebricksException {
        return getInsertOrDeleteRuntime(IndexOperation.DELETE, dataSource, propagatedSchema, typeEnv, keys, payload,
                additionalNonKeyFields, recordDesc, context, spec, false, null);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertOrDeleteRuntime(
            IndexOperation indexOp, IDataSourceIndex<String, AqlSourceId> dataSourceIndex,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalNonKeyFields, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec, boolean bulkload) throws AlgebricksException {
        String indexName = dataSourceIndex.getId();
        String dataverseName = dataSourceIndex.getDataSource().getId().getDataverseName();
        String datasetName = dataSourceIndex.getDataSource().getId().getDatasourceName();

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        Index secondaryIndex;
        try {
            secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
        AsterixTupleFilterFactory filterFactory = createTupleFilterFactory(inputSchemas, typeEnv, filterExpr, context);
        switch (secondaryIndex.getIndexType()) {
            case BTREE: {
                return getBTreeDmlRuntime(dataverseName, datasetName, indexName, propagatedSchema, primaryKeys,
                        secondaryKeys, additionalNonKeyFields, filterFactory, recordDesc, context, spec, indexOp,
                        bulkload);
            }
            case RTREE: {
                return getRTreeDmlRuntime(dataverseName, datasetName, indexName, propagatedSchema, primaryKeys,
                        secondaryKeys, additionalNonKeyFields, filterFactory, recordDesc, context, spec, indexOp,
                        bulkload);
            }
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX: {
                return getInvertedIndexDmlRuntime(dataverseName, datasetName, indexName, propagatedSchema, typeEnv,
                        primaryKeys, secondaryKeys, additionalNonKeyFields, filterFactory, recordDesc, context, spec,
                        indexOp, secondaryIndex.getIndexType(), bulkload);
            }
            default: {
                throw new AlgebricksException(
                        "Insert and delete not implemented for index type: " + secondaryIndex.getIndexType());
            }
        }
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<String, AqlSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec,
            boolean bulkload) throws AlgebricksException {
        return getIndexInsertOrDeleteRuntime(IndexOperation.INSERT, dataSourceIndex, propagatedSchema, inputSchemas,
                typeEnv, primaryKeys, secondaryKeys, additionalNonKeyFields, filterExpr, recordDesc, context, spec,
                bulkload);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getTokenizerRuntime(
            IDataSourceIndex<String, AqlSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec, boolean bulkload) throws AlgebricksException {

        String indexName = dataSourceIndex.getId();
        String dataverseName = dataSourceIndex.getDataSource().getId().getDataverseName();
        String datasetName = dataSourceIndex.getDataSource().getId().getDatasourceName();

        IOperatorSchema inputSchema = new OperatorSchemaImpl();
        if (inputSchemas.length > 0) {
            inputSchema = inputSchemas[0];
        } else {
            throw new AlgebricksException("TokenizeOperator can not operate without any input variable.");
        }

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        Index secondaryIndex;
        try {
            secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
        AsterixTupleFilterFactory filterFactory = createTupleFilterFactory(inputSchemas, typeEnv, filterExpr, context);
        // TokenizeOperator only supports a keyword or n-gram index.
        switch (secondaryIndex.getIndexType()) {
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX: {
                return getBinaryTokenizerRuntime(dataverseName, datasetName, indexName, inputSchema, propagatedSchema,
                        typeEnv, primaryKeys, secondaryKeys, filterFactory, recordDesc, context, spec,
                        IndexOperation.INSERT, secondaryIndex.getIndexType(), bulkload);
            }
            default: {
                throw new AlgebricksException("Currently, we do not support TokenizeOperator for the index type: "
                        + secondaryIndex.getIndexType());
            }
        }

    }

    // Get a Tokenizer for the bulk-loading data into a n-gram or keyword index.
    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getBinaryTokenizerRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema inputSchema, IOperatorSchema propagatedSchema,
            IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            AsterixTupleFilterFactory filterFactory, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp, IndexType indexType, boolean bulkload)
            throws AlgebricksException {

        // Sanity checks.
        if (primaryKeys.size() > 1) {
            throw new AlgebricksException("Cannot tokenize composite primary key.");
        }
        if (secondaryKeys.size() > 1) {
            throw new AlgebricksException("Cannot tokenize composite secondary key fields.");
        }

        boolean isPartitioned;
        if (indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
            isPartitioned = true;
        } else {
            isPartitioned = false;
        }

        // Number of Keys that needs to be propagated
        int numKeys = inputSchema.getSize();

        // Get the rest of Logical Variables that are not (PK or SK) and each
        // variable's positions.
        // These variables will be propagated through TokenizeOperator.
        List<LogicalVariable> otherKeys = new ArrayList<LogicalVariable>();
        if (inputSchema.getSize() > 0) {
            for (int k = 0; k < inputSchema.getSize(); k++) {
                boolean found = false;
                for (LogicalVariable varKey : primaryKeys) {
                    if (varKey.equals(inputSchema.getVariable(k))) {
                        found = true;
                        break;
                    } else {
                        found = false;
                    }
                }
                if (!found) {
                    for (LogicalVariable varKey : secondaryKeys) {
                        if (varKey.equals(inputSchema.getVariable(k))) {
                            found = true;
                            break;
                        } else {
                            found = false;
                        }
                    }
                }
                if (!found) {
                    otherKeys.add(inputSchema.getVariable(k));
                }
            }
        }

        // For tokenization, sorting and loading.
        // One token (+ optional partitioning field) + primary keys + secondary
        // keys + other variables
        // secondary keys and other variables will be just passed to the
        // IndexInsertDelete Operator.
        int numTokenKeyPairFields = (!isPartitioned) ? 1 + numKeys : 2 + numKeys;

        // generate field permutations for the input
        int[] fieldPermutation = new int[numKeys];

        int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
        int i = 0;
        int j = 0;
        for (LogicalVariable varKey : primaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            modificationCallbackPrimaryKeyFields[j] = i;
            i++;
            j++;
        }
        for (LogicalVariable varKey : otherKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            i++;
        }
        for (LogicalVariable varKey : secondaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            i++;
        }

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType;
        try {
            itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getItemTypeDataverseName(), itemTypeName)
                    .getDatatype();

            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be tokenized.");
            }

            ARecordType recType = (ARecordType) itemType;

            // Index parameters.
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);

            List<List<String>> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();
            List<IAType> secondaryKeyTypeEntries = secondaryIndex.getKeyFieldTypes();

            int numTokenFields = (!isPartitioned) ? secondaryKeys.size() : secondaryKeys.size() + 1;
            ITypeTraits[] tokenTypeTraits = new ITypeTraits[numTokenFields];
            ITypeTraits[] invListsTypeTraits = new ITypeTraits[primaryKeys.size()];

            // Find the key type of the secondary key. If it's a derived type,
            // return the derived type.
            // e.g. UNORDERED LIST -> return UNORDERED LIST type
            IAType secondaryKeyType = null;
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryKeyTypeEntries.get(0),
                    secondaryKeyExprs.get(0), recType);
            secondaryKeyType = keyPairType.first;
            List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            i = 0;
            for (List<String> partitioningKey : partitioningKeys) {
                IAType keyType = recType.getSubFieldType(partitioningKey);
                invListsTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }

            tokenTypeTraits[0] = NonTaggedFormatUtil.getTokenTypeTrait(secondaryKeyType);
            if (isPartitioned) {
                // The partitioning field is hardcoded to be a short *without*
                // an Asterix type tag.
                tokenTypeTraits[1] = ShortPointable.TYPE_TRAITS;
            }

            IBinaryTokenizerFactory tokenizerFactory = NonTaggedFormatUtil.getBinaryTokenizerFactory(
                    secondaryKeyType.getTypeTag(), indexType, secondaryIndex.getGramLength());

            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    splitProviderAndPartitionConstraintsForDataset(
                    dataverseName, datasetName, indexName, dataset.getDatasetDetails().isTemp());

            // Generate Output Record format
            ISerializerDeserializer<?>[] tokenKeyPairFields = new ISerializerDeserializer[numTokenKeyPairFields];
            ITypeTraits[] tokenKeyPairTypeTraits = new ITypeTraits[numTokenKeyPairFields];
            ISerializerDeserializerProvider serdeProvider = FormatUtils.getDefaultFormat().getSerdeProvider();

            // The order of the output record: propagated variables (including
            // PK and SK), token, and number of token.
            // #1. propagate all input variables
            for (int k = 0; k < recordDesc.getFieldCount(); k++) {
                tokenKeyPairFields[k] = recordDesc.getFields()[k];
                tokenKeyPairTypeTraits[k] = recordDesc.getTypeTraits()[k];
            }
            int tokenOffset = recordDesc.getFieldCount();

            // #2. Specify the token type
            tokenKeyPairFields[tokenOffset] = serdeProvider.getSerializerDeserializer(secondaryKeyType);
            tokenKeyPairTypeTraits[tokenOffset] = tokenTypeTraits[0];
            tokenOffset++;

            // #3. Specify the length-partitioning key: number of token
            if (isPartitioned) {
                tokenKeyPairFields[tokenOffset] = ShortSerializerDeserializer.INSTANCE;
                tokenKeyPairTypeTraits[tokenOffset] = tokenTypeTraits[1];
            }

            RecordDescriptor tokenKeyPairRecDesc = new RecordDescriptor(tokenKeyPairFields, tokenKeyPairTypeTraits);
            IOperatorDescriptor tokenizerOp;

            // Keys to be tokenized : SK
            int docField = fieldPermutation[fieldPermutation.length - 1];

            // Keys to be propagated
            int[] keyFields = new int[numKeys];
            for (int k = 0; k < keyFields.length; k++) {
                keyFields[k] = k;
            }

            tokenizerOp = new BinaryTokenizerOperatorDescriptor(spec, tokenKeyPairRecDesc, tokenizerFactory, docField,
                    keyFields, isPartitioned, true);
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(tokenizerOp,
                    splitsAndConstraint.second);

        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<String, AqlSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec)
            throws AlgebricksException {
        return getIndexInsertOrDeleteRuntime(IndexOperation.DELETE, dataSourceIndex, propagatedSchema, inputSchemas,
                typeEnv, primaryKeys, secondaryKeys, additionalNonKeyFields, filterExpr, recordDesc, context, spec,
                false);
    }

    private AsterixTupleFilterFactory createTupleFilterFactory(IOperatorSchema[] inputSchemas,
            IVariableTypeEnvironment typeEnv, ILogicalExpression filterExpr, JobGenContext context)
            throws AlgebricksException {
        // No filtering condition.
        if (filterExpr == null) {
            return null;
        }
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        IScalarEvaluatorFactory filterEvalFactory = expressionRuntimeProvider.createEvaluatorFactory(filterExpr,
                typeEnv, inputSchemas, context);
        return new AsterixTupleFilterFactory(filterEvalFactory, context.getBinaryBooleanInspectorFactory());
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getBTreeDmlRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalNonKeyFields, AsterixTupleFilterFactory filterFactory,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec, IndexOperation indexOp,
            boolean bulkload) throws AlgebricksException {

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }
        boolean temp = dataset.getDatasetDetails().isTemp();
        isTemporaryDatasetWriteJob = isTemporaryDatasetWriteJob && temp;

        int numKeys = primaryKeys.size() + secondaryKeys.size();
        int numFilterFields = DatasetUtils.getFilterField(dataset) == null ? 0 : 1;

        // generate field permutations
        int[] fieldPermutation = new int[numKeys + numFilterFields];
        int[] bloomFilterKeyFields = new int[secondaryKeys.size()];
        int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
        int i = 0;
        int j = 0;
        for (LogicalVariable varKey : secondaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            bloomFilterKeyFields[i] = i;
            i++;
        }
        for (LogicalVariable varKey : primaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            modificationCallbackPrimaryKeyFields[j] = i;
            i++;
            j++;
        }
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalNonKeyFields.get(0));
            fieldPermutation[numKeys] = idx;
        }

        String itemTypeName = dataset.getItemTypeName();
        IAType itemType;
        try {
            itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getItemTypeDataverseName(), itemTypeName)
                    .getDatatype();

            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be indexed.");
            }

            ARecordType recType = (ARecordType) itemType;

            // Index parameters.
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);

            ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, recType);
            IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.
                    computeFilterBinaryComparatorFactories(dataset,
                    recType, context.getBinaryComparatorFactoryProvider());
            int[] filterFields = null;
            int[] btreeFields = null;
            if (filterTypeTraits != null) {
                filterFields = new int[1];
                filterFields[0] = numKeys;
                btreeFields = new int[numKeys];
                for (int k = 0; k < btreeFields.length; k++) {
                    btreeFields[k] = k;
                }
            }

            List<List<String>> secondaryKeyNames = secondaryIndex.getKeyFieldNames();
            List<IAType> secondaryKeyTypes = secondaryIndex.getKeyFieldTypes();
            ITypeTraits[] typeTraits = new ITypeTraits[numKeys];
            IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numKeys];
            for (i = 0; i < secondaryKeys.size(); ++i) {
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(i),
                        secondaryKeyNames.get(i), recType);
                IAType keyType = keyPairType.first;
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType,
                        true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            }
            List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            for (List<String> partitioningKey : partitioningKeys) {
                IAType keyType = recType.getSubFieldType(partitioningKey);
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType,
                        true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    splitProviderAndPartitionConstraintsForDataset(
                    dataverseName, datasetName, indexName, temp);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            IModificationOperationCallbackFactory modificationCallbackFactory = temp
                    ? new TempDatasetSecondaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            modificationCallbackPrimaryKeyFields, txnSubsystemProvider, indexOp, ResourceType.LSM_BTREE)
                    : new SecondaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            modificationCallbackPrimaryKeyFields, txnSubsystemProvider, indexOp, ResourceType.LSM_BTREE,
                            dataset.hasMetaPart());

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils
                    .getMergePolicyFactory(dataset, mdTxnCtx);
            IIndexDataflowHelperFactory idfh = new LSMBTreeDataflowHelperFactory(
                    new AsterixVirtualBufferCacheProvider(datasetId), compactionInfo.first, compactionInfo.second,
                    new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), false, filterTypeTraits, filterCmpFactories,
                    btreeFields, filterFields, !temp);
            IOperatorDescriptor op;
            if (bulkload) {
                long numElementsHint = getCardinalityPerPartitionHint(dataset);
                op = new TreeIndexBulkLoadOperatorDescriptor(spec, recordDesc, appContext.getStorageManagerInterface(),
                        appContext.getIndexLifecycleManagerProvider(), splitsAndConstraint.first, typeTraits,
                        comparatorFactories, bloomFilterKeyFields, fieldPermutation,
                        GlobalConfig.DEFAULT_TREE_FILL_FACTOR, false, numElementsHint, false, idfh);
            } else {
                op = new AsterixLSMTreeInsertDeleteOperatorDescriptor(spec, recordDesc,
                        appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                        splitsAndConstraint.first, typeTraits, comparatorFactories, bloomFilterKeyFields,
                        fieldPermutation, indexOp,
                        new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(datasetId),
                                compactionInfo.first, compactionInfo.second,
                                new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                                LSMBTreeIOOperationCallbackFactory.INSTANCE,
                                storageProperties.getBloomFilterFalsePositiveRate(), false, filterTypeTraits,
                                filterCmpFactories, btreeFields, filterFields, !temp),
                        filterFactory, false, indexName, null, modificationCallbackFactory,
                        NoOpOperationCallbackFactory.INSTANCE);
            }
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(op, splitsAndConstraint.second);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInvertedIndexDmlRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalNonKeyFields, AsterixTupleFilterFactory filterFactory,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec, IndexOperation indexOp,
            IndexType indexType, boolean bulkload) throws AlgebricksException {

        // Check the index is length-partitioned or not.
        boolean isPartitioned;
        if (indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
            isPartitioned = true;
        } else {
            isPartitioned = false;
        }

        // Sanity checks.
        if (primaryKeys.size() > 1) {
            throw new AlgebricksException("Cannot create inverted index on dataset with composite primary key.");
        }
        // The size of secondaryKeys can be two if it receives input from its
        // TokenizeOperator- [token, number of token]
        if (secondaryKeys.size() > 1 && !isPartitioned) {
            throw new AlgebricksException("Cannot create composite inverted index on multiple fields.");
        } else if (secondaryKeys.size() > 2 && isPartitioned) {
            throw new AlgebricksException("Cannot create composite inverted index on multiple fields.");
        }

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }
        boolean temp = dataset.getDatasetDetails().isTemp();
        isTemporaryDatasetWriteJob = isTemporaryDatasetWriteJob && temp;

        // For tokenization, sorting and loading.
        // One token (+ optional partitioning field) + primary keys: [token,
        // number of token, PK]
        int numKeys = primaryKeys.size() + secondaryKeys.size();
        int numTokenKeyPairFields = (!isPartitioned) ? 1 + primaryKeys.size() : 2 + primaryKeys.size();
        int numFilterFields = DatasetUtils.getFilterField(dataset) == null ? 0 : 1;

        // generate field permutations
        int[] fieldPermutation = new int[numKeys + numFilterFields];
        int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
        int i = 0;
        int j = 0;

        // If the index is partitioned: [token, number of token]
        // Otherwise: [token]
        for (LogicalVariable varKey : secondaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            i++;
        }
        for (LogicalVariable varKey : primaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            modificationCallbackPrimaryKeyFields[j] = i;
            i++;
            j++;
        }
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalNonKeyFields.get(0));
            fieldPermutation[numKeys] = idx;
        }

        String itemTypeName = dataset.getItemTypeName();
        IAType itemType;
        try {
            itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getItemTypeDataverseName(), itemTypeName)
                    .getDatatype();

            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be indexed.");
            }

            ARecordType recType = (ARecordType) itemType;

            // Index parameters.
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);

            List<List<String>> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();
            List<IAType> secondaryKeyTypes = secondaryIndex.getKeyFieldTypes();

            int numTokenFields = 0;

            // SecondaryKeys.size() can be two if it comes from the bulkload.
            // In this case, [token, number of token] are the secondaryKeys.
            if (!isPartitioned || (secondaryKeys.size() > 1)) {
                numTokenFields = secondaryKeys.size();
            } else if (isPartitioned && (secondaryKeys.size() == 1)) {
                numTokenFields = secondaryKeys.size() + 1;
            }

            ARecordType metaItemType = DatasetUtils.getMetaType(this, dataset);
            ITypeTraits[] tokenTypeTraits = new ITypeTraits[numTokenFields];
            ITypeTraits[] invListsTypeTraits = new ITypeTraits[primaryKeys.size()];
            IBinaryComparatorFactory[] tokenComparatorFactories = new IBinaryComparatorFactory[numTokenFields];
            IBinaryComparatorFactory[] invListComparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(
                    dataset, recType, metaItemType, context.getBinaryComparatorFactoryProvider());

            IAType secondaryKeyType = null;

            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(0),
                    secondaryKeyExprs.get(0), recType);
            secondaryKeyType = keyPairType.first;

            List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);

            i = 0;
            for (List<String> partitioningKey : partitioningKeys) {
                IAType keyType = recType.getSubFieldType(partitioningKey);
                invListsTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }

            tokenComparatorFactories[0] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
            tokenTypeTraits[0] = NonTaggedFormatUtil.getTokenTypeTrait(secondaryKeyType);
            if (isPartitioned) {
                // The partitioning field is hardcoded to be a short *without*
                // an Asterix type tag.
                tokenComparatorFactories[1] = PointableBinaryComparatorFactory.of(ShortPointable.FACTORY);
                tokenTypeTraits[1] = ShortPointable.TYPE_TRAITS;
            }
            IBinaryTokenizerFactory tokenizerFactory = NonTaggedFormatUtil.getBinaryTokenizerFactory(
                    secondaryKeyType.getTypeTag(), indexType, secondaryIndex.getGramLength());

            ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, recType);
            IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.computeFilterBinaryComparatorFactories(dataset,
                    recType, context.getBinaryComparatorFactoryProvider());

            int[] filterFields = null;
            int[] invertedIndexFields = null;
            int[] filterFieldsForNonBulkLoadOps = null;
            int[] invertedIndexFieldsForNonBulkLoadOps = null;
            if (filterTypeTraits != null) {
                filterFields = new int[1];
                filterFields[0] = numTokenFields + primaryKeys.size();
                invertedIndexFields = new int[numTokenFields + primaryKeys.size()];
                for (int k = 0; k < invertedIndexFields.length; k++) {
                    invertedIndexFields[k] = k;
                }

                filterFieldsForNonBulkLoadOps = new int[numFilterFields];
                filterFieldsForNonBulkLoadOps[0] = numTokenKeyPairFields;
                invertedIndexFieldsForNonBulkLoadOps = new int[numTokenKeyPairFields];
                for (int k = 0; k < invertedIndexFieldsForNonBulkLoadOps.length; k++) {
                    invertedIndexFieldsForNonBulkLoadOps[k] = k;
                }
            }

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    splitProviderAndPartitionConstraintsForDataset(
                    dataverseName, datasetName, indexName, temp);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            IModificationOperationCallbackFactory modificationCallbackFactory = temp
                    ? new TempDatasetSecondaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            modificationCallbackPrimaryKeyFields, txnSubsystemProvider, indexOp,
                            ResourceType.LSM_INVERTED_INDEX)
                    : new SecondaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            modificationCallbackPrimaryKeyFields, txnSubsystemProvider, indexOp,
                            ResourceType.LSM_INVERTED_INDEX, dataset.hasMetaPart());

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils
                    .getMergePolicyFactory(dataset, mdTxnCtx);
            IIndexDataflowHelperFactory indexDataFlowFactory;
            if (!isPartitioned) {
                indexDataFlowFactory = new LSMInvertedIndexDataflowHelperFactory(
                        new AsterixVirtualBufferCacheProvider(datasetId), compactionInfo.first, compactionInfo.second,
                        new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                        LSMInvertedIndexIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate(), invertedIndexFields, filterTypeTraits,
                        filterCmpFactories, filterFields, filterFieldsForNonBulkLoadOps,
                        invertedIndexFieldsForNonBulkLoadOps, !temp);
            } else {
                indexDataFlowFactory = new PartitionedLSMInvertedIndexDataflowHelperFactory(
                        new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                        compactionInfo.second, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                        LSMInvertedIndexIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate(), invertedIndexFields, filterTypeTraits,
                        filterCmpFactories, filterFields, filterFieldsForNonBulkLoadOps,
                        invertedIndexFieldsForNonBulkLoadOps, !temp);
            }
            IOperatorDescriptor op;
            if (bulkload) {
                long numElementsHint = getCardinalityPerPartitionHint(dataset);
                op = new LSMInvertedIndexBulkLoadOperatorDescriptor(spec, recordDesc, fieldPermutation, false,
                        numElementsHint, false, appContext.getStorageManagerInterface(), splitsAndConstraint.first,
                        appContext.getIndexLifecycleManagerProvider(), tokenTypeTraits, tokenComparatorFactories,
                        invListsTypeTraits, invListComparatorFactories, tokenizerFactory, indexDataFlowFactory);
            } else {
                op = new AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor(spec, recordDesc,
                        appContext.getStorageManagerInterface(), splitsAndConstraint.first,
                        appContext.getIndexLifecycleManagerProvider(), tokenTypeTraits, tokenComparatorFactories,
                        invListsTypeTraits, invListComparatorFactories, tokenizerFactory, fieldPermutation, indexOp,
                        indexDataFlowFactory, filterFactory, modificationCallbackFactory, indexName);
            }
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(op, splitsAndConstraint.second);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getRTreeDmlRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalNonKeyFields, AsterixTupleFilterFactory filterFactory,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec, IndexOperation indexOp,
            boolean bulkload) throws AlgebricksException {
        try {
            Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);

            boolean temp = dataset.getDatasetDetails().isTemp();
            isTemporaryDatasetWriteJob = isTemporaryDatasetWriteJob && temp;

            String itemTypeName = dataset.getItemTypeName();
            IAType itemType = MetadataManager.INSTANCE
                    .getDatatype(mdTxnCtx, dataset.getItemTypeDataverseName(), itemTypeName).getDatatype();
            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be indexed.");
            }
            ARecordType recType = (ARecordType) itemType;
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
            List<List<String>> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();
            List<IAType> secondaryKeyTypes = secondaryIndex.getKeyFieldTypes();
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(0),
                    secondaryKeyExprs.get(0), recType);
            IAType spatialType = keyPairType.first;
            boolean isPointMBR = spatialType.getTypeTag() == ATypeTag.POINT
                    || spatialType.getTypeTag() == ATypeTag.POINT3D;
            int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
            int numSecondaryKeys = dimension * 2;
            int numPrimaryKeys = primaryKeys.size();
            int numKeys = numSecondaryKeys + numPrimaryKeys;
            ITypeTraits[] typeTraits = new ITypeTraits[numKeys];
            IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys];

            int numFilterFields = DatasetUtils.getFilterField(dataset) == null ? 0 : 1;
            int[] fieldPermutation = new int[numKeys + numFilterFields];
            int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
            int i = 0;
            int j = 0;

            for (LogicalVariable varKey : secondaryKeys) {
                int idx = propagatedSchema.findVariable(varKey);
                fieldPermutation[i] = idx;
                i++;
            }
            for (LogicalVariable varKey : primaryKeys) {
                int idx = propagatedSchema.findVariable(varKey);
                fieldPermutation[i] = idx;
                modificationCallbackPrimaryKeyFields[j] = i;
                i++;
                j++;
            }

            if (numFilterFields > 0) {
                int idx = propagatedSchema.findVariable(additionalNonKeyFields.get(0));
                fieldPermutation[numKeys] = idx;
            }
            IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(spatialType.getTypeTag());
            IPrimitiveValueProviderFactory[] valueProviderFactories =
                    new IPrimitiveValueProviderFactory[numSecondaryKeys];
            for (i = 0; i < numSecondaryKeys; i++) {
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE
                        .getBinaryComparatorFactory(nestedKeyType, true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(nestedKeyType);
                valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
            }
            List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            for (List<String> partitioningKey : partitioningKeys) {
                IAType keyType = recType.getSubFieldType(partitioningKey);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }
            ARecordType metaItemType = DatasetUtils.getMetaType(this, dataset);
            IBinaryComparatorFactory[] primaryComparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(
                    dataset, recType, metaItemType, context.getBinaryComparatorFactoryProvider());
            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    splitProviderAndPartitionConstraintsForDataset(
                    dataverseName, datasetName, indexName, temp);
            int[] btreeFields = new int[primaryComparatorFactories.length];
            for (int k = 0; k < btreeFields.length; k++) {
                btreeFields[k] = k + numSecondaryKeys;
            }

            ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, recType);
            IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.
                    computeFilterBinaryComparatorFactories(dataset,
                    recType, context.getBinaryComparatorFactoryProvider());
            int[] filterFields = null;
            int[] rtreeFields = null;
            if (filterTypeTraits != null) {
                filterFields = new int[1];
                filterFields[0] = numSecondaryKeys + numPrimaryKeys;
                rtreeFields = new int[numSecondaryKeys + numPrimaryKeys];
                for (int k = 0; k < rtreeFields.length; k++) {
                    rtreeFields[k] = k;
                }
            }

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            IModificationOperationCallbackFactory modificationCallbackFactory = temp
                    ? new TempDatasetSecondaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            modificationCallbackPrimaryKeyFields, txnSubsystemProvider,
                            indexOp, ResourceType.LSM_RTREE)
                    : new SecondaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            modificationCallbackPrimaryKeyFields, txnSubsystemProvider,
                            indexOp, ResourceType.LSM_RTREE,
                            dataset.hasMetaPart());

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils
                    .getMergePolicyFactory(dataset, mdTxnCtx);

            IBinaryComparatorFactory[] deletedKeyBTreeCompFactories = getMergedComparatorFactories(comparatorFactories,
                    primaryComparatorFactories);
            IIndexDataflowHelperFactory idff = new LSMRTreeWithAntiMatterTuplesDataflowHelperFactory(
                    valueProviderFactories, RTreePolicyType.RTREE, deletedKeyBTreeCompFactories,
                    new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                    compactionInfo.second, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMRTreeIOOperationCallbackFactory.INSTANCE,
                    AqlMetadataProvider.proposeLinearizer(nestedKeyType.getTypeTag(), comparatorFactories.length),
                    rtreeFields, filterTypeTraits, filterCmpFactories, filterFields, !temp, isPointMBR);
            IOperatorDescriptor op;
            if (bulkload) {
                long numElementsHint = getCardinalityPerPartitionHint(dataset);
                op = new TreeIndexBulkLoadOperatorDescriptor(spec, recordDesc, appContext.getStorageManagerInterface(),
                        appContext.getIndexLifecycleManagerProvider(), splitsAndConstraint.first, typeTraits,
                        primaryComparatorFactories, btreeFields, fieldPermutation,
                        GlobalConfig.DEFAULT_TREE_FILL_FACTOR, false, numElementsHint, false, idff);
            } else {
                op = new AsterixLSMTreeInsertDeleteOperatorDescriptor(spec, recordDesc,
                        appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                        splitsAndConstraint.first, typeTraits, comparatorFactories, null, fieldPermutation, indexOp,
                        idff, filterFactory, false, indexName, null, modificationCallbackFactory,
                        NoOpOperationCallbackFactory.INSTANCE);
            }
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(op, splitsAndConstraint.second);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public JobId getJobId() {
        return jobId;
    }

    public static ILinearizeComparatorFactory proposeLinearizer(ATypeTag keyType, int numKeyFields)
            throws AlgebricksException {
        return AqlLinearizeComparatorFactoryProvider.INSTANCE.getLinearizeComparatorFactory(keyType, true,
                numKeyFields / 2);
    }

    /**
     * Calculate an estimate size of the bloom filter. Note that this is an
     * estimation which assumes that the data is going to be uniformly
     * distributed across all partitions.
     *
     * @param dataset
     * @return Number of elements that will be used to create a bloom filter per
     *         dataset per partition
     * @throws MetadataException
     * @throws AlgebricksException
     */
    public long getCardinalityPerPartitionHint(Dataset dataset) throws MetadataException, AlgebricksException {
        String numElementsHintString = dataset.getHints().get(DatasetCardinalityHint.NAME);
        long numElementsHint;
        if (numElementsHintString == null) {
            numElementsHint = DatasetCardinalityHint.DEFAULT;
        } else {
            numElementsHint = Long.parseLong(numElementsHintString);
        }
        int numPartitions = 0;
        List<String> nodeGroup = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, dataset.getNodeGroupName())
                .getNodeNames();
        for (String nd : nodeGroup) {
            numPartitions += AsterixClusterProperties.INSTANCE.getNodePartitionsCount(nd);
        }
        return numElementsHint /= numPartitions;
    }

    @Override
    public IFunctionInfo lookupFunction(FunctionIdentifier fid) {
        return AsterixBuiltinFunctions.lookupFunction(fid);
    }

    public Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraintsForDataset(
            String dataverseName, String datasetName, String targetIdxName, boolean temp) throws AlgebricksException {
        FileSplit[] splits = splitsForDataset(mdTxnCtx, dataverseName, datasetName, targetIdxName, temp);
        return StoragePathUtil.splitProviderAndPartitionConstraints(splits);
    }

    public Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraintsForDataverse(
            String dataverse) {
        return SplitsAndConstraintsUtil.splitProviderAndPartitionConstraintsForDataverse(dataverse);
    }

    public FileSplit[] splitsForDataset(MetadataTransactionContext mdTxnCtx, String dataverseName, String datasetName,
            String targetIdxName, boolean temp) throws AlgebricksException {
        return SplitsAndConstraintsUtil.splitsForDataset(mdTxnCtx, dataverseName, datasetName, targetIdxName, temp);
    }

    public DatasourceAdapter getAdapter(MetadataTransactionContext mdTxnCtx, String dataverseName, String adapterName)
            throws MetadataException {
        DatasourceAdapter adapter = null;
        // search in default namespace (built-in adapter)
        adapter = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME, adapterName);

        // search in dataverse (user-defined adapter)
        if (adapter == null) {
            adapter = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, dataverseName, adapterName);
        }
        return adapter;
    }

    public Dataset findDataset(String dataverse, String dataset) throws AlgebricksException {
        String dv = dataverse == null ? (defaultDataverse == null ? null : defaultDataverse.getDataverseName())
                : dataverse;
        if (dv == null) {
            return null;
        }
        try {
            return MetadataManager.INSTANCE.getDataset(mdTxnCtx, dv, dataset);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public INodeDomain findNodeDomain(String nodeGroupName) throws AlgebricksException {
        NodeGroup nodeGroup = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodeGroupName);
        List<String> partitions = new ArrayList<>();
        for (String location : nodeGroup.getNodeNames()) {
            int numPartitions = AsterixClusterProperties.INSTANCE.getNodePartitionsCount(location);
            for (int i = 0; i < numPartitions; i++) {
                partitions.add(location);
            }
        }
        return new DefaultNodeGroupDomain(partitions);
    }

    public IAType findType(String dataverse, String typeName) throws AlgebricksException {
        if (dataverse == null || typeName == null) {
            return null;
        }
        Datatype type;
        try {
            type = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverse, typeName);
        } catch (MetadataException e) {
            throw new AlgebricksException(
                    "Metadata exception while looking up type '" + typeName + "' in dataverse '" + dataverse + "'", e);
        }
        if (type == null) {
            throw new AlgebricksException("Type name '" + typeName + "' unknown in dataverse '" + dataverse + "'");
        }
        return type.getDatatype();
    }

    public Feed findFeed(String dataverse, String feedName) throws AlgebricksException {
        try {
            return MetadataManager.INSTANCE.getFeed(mdTxnCtx, dataverse, feedName);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public FeedPolicyEntity findFeedPolicy(String dataverse, String policyName) throws AlgebricksException {
        try {
            return MetadataManager.INSTANCE.getFeedPolicy(mdTxnCtx, dataverse, policyName);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public List<Index> getDatasetIndexes(String dataverseName, String datasetName) throws AlgebricksException {
        try {
            return MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public AlgebricksAbsolutePartitionConstraint getClusterLocations() {
        return AsterixClusterProperties.INSTANCE.getClusterLocations();
    }

    public IDataFormat getFormat() {
        return FormatUtils.getDefaultFormat();
    }

    public Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraintsForFilesIndex(
            String dataverseName, String datasetName, String targetIdxName, boolean create) throws AlgebricksException {
        return SplitsAndConstraintsUtil.splitProviderAndPartitionConstraintsForFilesIndex(mdTxnCtx, dataverseName,
                datasetName, targetIdxName, create);
    }

    public AsterixStorageProperties getStorageProperties() {
        return storageProperties;
    }

    public Map<String, Integer> getLocks() {
        return locks;
    }

    public void setLocks(Map<String, Integer> locks) {
        this.locks = locks;
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDataLookupRuntime(
            JobSpecification jobSpec, Dataset dataset, Index secondaryIndex, int[] ridIndexes, boolean retainInput,
            IVariableTypeEnvironment typeEnv, List<LogicalVariable> outputVars, IOperatorSchema opSchema,
            JobGenContext context, AqlMetadataProvider metadataProvider, boolean retainMissing)
            throws AlgebricksException {
        try {
            // Get data type
            IAType itemType = null;
            itemType = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                    dataset.getDataverseName(), dataset.getItemTypeName()).getDatatype();

            // Create the adapter factory <- right now there is only one. if there are more in the future, we can create
            // a map->
            ExternalDatasetDetails datasetDetails = (ExternalDatasetDetails) dataset.getDatasetDetails();
            LookupAdapterFactory<?> adapterFactory = AdapterFactoryProvider.getLookupAdapterFactory(libraryManager,
                    datasetDetails.getProperties(), (ARecordType) itemType, ridIndexes, retainInput, retainMissing,
                    context.getMissingWriterFactory());

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo;
            try {
                compactionInfo = DatasetUtils.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
            } catch (MetadataException e) {
                throw new AlgebricksException(" Unabel to create merge policy factory for external dataset", e);
            }

            boolean temp = datasetDetails.isTemp();
            // Create the file index data flow helper
            ExternalBTreeDataflowHelperFactory indexDataflowHelperFactory = new ExternalBTreeDataflowHelperFactory(
                    compactionInfo.first, compactionInfo.second,
                    new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                    metadataProvider.getStorageProperties().getBloomFilterFalsePositiveRate(),
                    ExternalDatasetsRegistry.INSTANCE.getAndLockDatasetVersion(dataset, metadataProvider), !temp);

            // Create the out record descriptor, appContext and fileSplitProvider for the files index
            RecordDescriptor outRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc;
            spPc = metadataProvider.splitProviderAndPartitionConstraintsForFilesIndex(dataset.getDataverseName(),
                    dataset.getDatasetName(),
                    dataset.getDatasetName().concat(IndexingConstants.EXTERNAL_FILE_INDEX_NAME_SUFFIX), false);
            ISearchOperationCallbackFactory searchOpCallbackFactory = temp ? NoOpOperationCallbackFactory.INSTANCE
                    : new SecondaryIndexSearchOperationCallbackFactory();
            // Create the operator
            ExternalLookupOperatorDescriptor op = new ExternalLookupOperatorDescriptor(jobSpec, adapterFactory,
                    outRecDesc, indexDataflowHelperFactory, retainInput, appContext.getIndexLifecycleManagerProvider(),
                    appContext.getStorageManagerInterface(), spPc.first, dataset.getDatasetId(),
                    metadataProvider.getStorageProperties().getBloomFilterFalsePositiveRate(), searchOpCallbackFactory,
                    retainMissing, context.getMissingWriterFactory());

            // Return value
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(op, spPc.second);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getUpsertRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, LogicalVariable payload, List<LogicalVariable> filterKeys,
            List<LogicalVariable> additionalNonFilterFields, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        String datasetName = dataSource.getId().getDatasourceName();
        Dataset dataset = findDataset(dataSource.getId().getDataverseName(), datasetName);
        if (dataset == null) {
            throw new AlgebricksException(
                    "Unknown dataset " + datasetName + " in dataverse " + dataSource.getId().getDataverseName());
        }
        boolean temp = dataset.getDatasetDetails().isTemp();
        isTemporaryDatasetWriteJob = isTemporaryDatasetWriteJob && temp;

        int numKeys = primaryKeys.size();
        int numFilterFields = DatasetUtils.getFilterField(dataset) == null ? 0 : 1;
        int numOfAdditionalFields = additionalNonFilterFields == null ? 0 : additionalNonFilterFields.size();
        // Move key fields to front. {keys, record, filters}
        int[] fieldPermutation = new int[numKeys + 1 + numFilterFields + numOfAdditionalFields];
        int[] bloomFilterKeyFields = new int[numKeys];
        int i = 0;
        // set the keys' permutations
        for (LogicalVariable varKey : primaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            bloomFilterKeyFields[i] = i;
            i++;
        }
        // set the record permutation
        fieldPermutation[i++] = propagatedSchema.findVariable(payload);
        // set the filters' permutations.
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(filterKeys.get(0));
            fieldPermutation[i++] = idx;
        }

        if (additionalNonFilterFields != null) {
            for (LogicalVariable var : additionalNonFilterFields) {
                int idx = propagatedSchema.findVariable(var);
                fieldPermutation[i++] = idx;
            }
        }

        try {
            Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), dataset.getDatasetName());
            String indexName = primaryIndex.getIndexName();

            String itemTypeName = dataset.getItemTypeName();
            String itemTypeDataverseName = dataset.getItemTypeDataverseName();
            ARecordType itemType = (ARecordType) MetadataManager.INSTANCE
                    .getDatatype(mdTxnCtx, itemTypeDataverseName, itemTypeName).getDatatype();
            ARecordType metaItemType = DatasetUtils.getMetaType(this, dataset);
            ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType, metaItemType);
            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                    itemType, metaItemType, context.getBinaryComparatorFactoryProvider());
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    splitProviderAndPartitionConstraintsForDataset(
                    dataSource.getId().getDataverseName(), datasetName, indexName, temp);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            int[] primaryKeyFields = new int[numKeys];
            for (i = 0; i < numKeys; i++) {
                primaryKeyFields[i] = i;
            }

            ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, itemType);
            IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.
                    computeFilterBinaryComparatorFactories(dataset,
                    itemType, context.getBinaryComparatorFactoryProvider());
            int[] filterFields = DatasetUtils.createFilterFields(dataset);
            int[] btreeFields = DatasetUtils.createBTreeFieldsWhenThereisAFilter(dataset);

            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            IModificationOperationCallbackFactory modificationCallbackFactory = temp
                    ? new TempDatasetPrimaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            primaryKeyFields, txnSubsystemProvider, IndexOperation.UPSERT, ResourceType.LSM_BTREE)
                    : new UpsertOperationCallbackFactory(jobId, datasetId, primaryKeyFields, txnSubsystemProvider,
                            IndexOperation.UPSERT, ResourceType.LSM_BTREE, dataset.hasMetaPart());

            LockThenSearchOperationCallbackFactory searchCallbackFactory = new LockThenSearchOperationCallbackFactory(
                    jobId, datasetId, primaryKeyFields, txnSubsystemProvider, ResourceType.LSM_BTREE);

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils
                    .getMergePolicyFactory(dataset, mdTxnCtx);
            IIndexDataflowHelperFactory idfh = new LSMBTreeDataflowHelperFactory(
                    new AsterixVirtualBufferCacheProvider(datasetId), compactionInfo.first, compactionInfo.second,
                    new PrimaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), true, filterTypeTraits, filterCmpFactories,
                    btreeFields, filterFields, !temp);
            AsterixLSMTreeUpsertOperatorDescriptor op;

            ITypeTraits[] outputTypeTraits = new ITypeTraits[recordDesc.getFieldCount()
                    + (dataset.hasMetaPart() ? 2 : 1) + numFilterFields];
            ISerializerDeserializer[] outputSerDes = new ISerializerDeserializer[recordDesc.getFieldCount()
                    + (dataset.hasMetaPart() ? 2 : 1) + numFilterFields];
            for (int j = 0; j < recordDesc.getFieldCount(); j++) {
                outputTypeTraits[j] = recordDesc.getTypeTraits()[j];
                outputSerDes[j] = recordDesc.getFields()[j];
            }
            outputSerDes[outputSerDes.length - (dataset.hasMetaPart() ? 2 : 1) - numFilterFields] = FormatUtils
                    .getDefaultFormat().getSerdeProvider().getSerializerDeserializer(itemType);
            outputTypeTraits[outputTypeTraits.length - (dataset.hasMetaPart() ? 2 : 1) - numFilterFields] = FormatUtils
                    .getDefaultFormat().getTypeTraitProvider().getTypeTrait(itemType);

            if (dataset.hasMetaPart()) {
                outputSerDes[outputSerDes.length - 1 - numFilterFields] = FormatUtils.getDefaultFormat()
                        .getSerdeProvider().getSerializerDeserializer(metaItemType);
                outputTypeTraits[outputTypeTraits.length - 1 - numFilterFields] = FormatUtils.getDefaultFormat()
                        .getTypeTraitProvider().getTypeTrait(metaItemType);
            }

            int fieldIdx = -1;
            if (numFilterFields > 0) {
                String filterField = DatasetUtils.getFilterField(dataset).get(0);
                for (i = 0; i < itemType.getFieldNames().length; i++) {
                    if (itemType.getFieldNames()[i].equals(filterField)) {
                        break;
                    }
                }
                fieldIdx = i;
                outputTypeTraits[outputTypeTraits.length - 1] = FormatUtils.getDefaultFormat().getTypeTraitProvider()
                        .getTypeTrait(itemType.getFieldTypes()[fieldIdx]);
                outputSerDes[outputSerDes.length - 1] = FormatUtils.getDefaultFormat().getSerdeProvider()
                        .getSerializerDeserializer(itemType.getFieldTypes()[fieldIdx]);
            }

            RecordDescriptor outputRecordDesc = new RecordDescriptor(outputSerDes, outputTypeTraits);
            op = new AsterixLSMTreeUpsertOperatorDescriptor(spec, outputRecordDesc,
                    appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                    splitsAndConstraint.first, typeTraits, comparatorFactories, bloomFilterKeyFields, fieldPermutation,
                    idfh, null, true, indexName, context.getMissingWriterFactory(), modificationCallbackFactory,
                    searchCallbackFactory, null);
            op.setType(itemType);
            op.setFilterIndex(fieldIdx);
            return new Pair<>(op, splitsAndConstraint.second);

        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    // TODO refactor this method
    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexUpsertRuntime(
            IDataSourceIndex<String, AqlSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalFilteringKeys,
            ILogicalExpression filterExpr, List<LogicalVariable> prevSecondaryKeys,
            LogicalVariable prevAdditionalFilteringKey, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        String indexName = dataSourceIndex.getId();
        String dataverseName = dataSourceIndex.getDataSource().getId().getDataverseName();
        String datasetName = dataSourceIndex.getDataSource().getId().getDatasourceName();

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        Index secondaryIndex;
        try {
            secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
        AsterixTupleFilterFactory filterFactory = createTupleFilterFactory(inputSchemas, typeEnv, filterExpr, context);
        ArrayList<LogicalVariable> prevAdditionalFilteringKeys = null;
        if (prevAdditionalFilteringKey != null) {
            prevAdditionalFilteringKeys = new ArrayList<>();
            prevAdditionalFilteringKeys.add(prevAdditionalFilteringKey);
        }
        switch (secondaryIndex.getIndexType()) {
            case BTREE: {
                return getBTreeUpsertRuntime(dataverseName, datasetName, indexName, propagatedSchema, typeEnv,
                        primaryKeys, secondaryKeys, additionalFilteringKeys, filterFactory, recordDesc, context, spec,
                        prevSecondaryKeys, prevAdditionalFilteringKeys);
            }
            case RTREE: {
                return getRTreeUpsertRuntime(dataverseName, datasetName, indexName, propagatedSchema, typeEnv,
                        primaryKeys, secondaryKeys, additionalFilteringKeys, filterFactory, recordDesc, context, spec,
                        prevSecondaryKeys, prevAdditionalFilteringKeys);
            }
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX: {
                return getInvertedIndexUpsertRuntime(dataverseName, datasetName, indexName, propagatedSchema,
                        primaryKeys, secondaryKeys, additionalFilteringKeys, filterFactory, recordDesc, context, spec,
                        secondaryIndex.getIndexType(), prevSecondaryKeys, prevAdditionalFilteringKeys);
            }
            default: {
                throw new AlgebricksException(
                        "upsert is not implemented for index type: " + secondaryIndex.getIndexType());
            }
        }
    }

    //TODO: refactor this method
    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInvertedIndexUpsertRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalFilteringKeys, AsterixTupleFilterFactory filterFactory,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec, IndexType indexType,
            List<LogicalVariable> prevSecondaryKeys, List<LogicalVariable> prevAdditionalFilteringKeys)
            throws AlgebricksException {
        // Check the index is length-partitioned or not.
        boolean isPartitioned;
        if (indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
            isPartitioned = true;
        } else {
            isPartitioned = false;
        }

        // Sanity checks.
        if (primaryKeys.size() > 1) {
            throw new AlgebricksException("Cannot create inverted index on dataset with composite primary key.");
        }
        // The size of secondaryKeys can be two if it receives input from its
        // TokenizeOperator- [token, number of token]
        if (secondaryKeys.size() > 1 && !isPartitioned) {
            throw new AlgebricksException("Cannot create composite inverted index on multiple fields.");
        } else if (secondaryKeys.size() > 2 && isPartitioned) {
            throw new AlgebricksException("Cannot create composite inverted index on multiple fields.");
        }

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }
        boolean temp = dataset.getDatasetDetails().isTemp();
        isTemporaryDatasetWriteJob = isTemporaryDatasetWriteJob && temp;

        // For tokenization, sorting and loading.
        // One token (+ optional partitioning field) + primary keys: [token,
        // number of token, PK]
        int numKeys = primaryKeys.size() + secondaryKeys.size();
        int numTokenKeyPairFields = (!isPartitioned) ? 1 + primaryKeys.size() : 2 + primaryKeys.size();
        int numFilterFields = DatasetUtils.getFilterField(dataset) == null ? 0 : 1;

        // generate field permutations
        int[] fieldPermutation = new int[numKeys + numFilterFields];
        int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
        int i = 0;
        int j = 0;

        // If the index is partitioned: [token, number of token]
        // Otherwise: [token]
        for (LogicalVariable varKey : secondaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            i++;
        }
        for (LogicalVariable varKey : primaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            modificationCallbackPrimaryKeyFields[j] = i;
            i++;
            j++;
        }
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalFilteringKeys.get(0));
            fieldPermutation[numKeys] = idx;
        }

        // Find permutations for prev value
        int[] prevFieldPermutation = new int[numKeys + numFilterFields];
        i = 0;

        // If the index is partitioned: [token, number of token]
        // Otherwise: [token]
        for (LogicalVariable varKey : prevSecondaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            prevFieldPermutation[i] = idx;
            i++;
        }

        for (int k = 0; k < primaryKeys.size(); k++) {
            prevFieldPermutation[k + i] = fieldPermutation[k + i];
            i++;
        }

        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(prevAdditionalFilteringKeys.get(0));
            prevFieldPermutation[numKeys] = idx;
        }

        String itemTypeName = dataset.getItemTypeName();
        IAType itemType;
        try {
            itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getDataverseName(), itemTypeName)
                    .getDatatype();

            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be indexed.");
            }

            ARecordType recType = (ARecordType) itemType;

            // Index parameters.
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);

            List<List<String>> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();
            List<IAType> secondaryKeyTypes = secondaryIndex.getKeyFieldTypes();

            int numTokenFields = 0;

            // SecondaryKeys.size() can be two if it comes from the bulkload.
            // In this case, [token, number of token] are the secondaryKeys.
            if (!isPartitioned || (secondaryKeys.size() > 1)) {
                numTokenFields = secondaryKeys.size();
            } else if (isPartitioned && (secondaryKeys.size() == 1)) {
                numTokenFields = secondaryKeys.size() + 1;
            }

            ARecordType metaItemType = DatasetUtils.getMetaType(this, dataset);
            ITypeTraits[] tokenTypeTraits = new ITypeTraits[numTokenFields];
            ITypeTraits[] invListsTypeTraits = new ITypeTraits[primaryKeys.size()];
            IBinaryComparatorFactory[] tokenComparatorFactories = new IBinaryComparatorFactory[numTokenFields];
            IBinaryComparatorFactory[] invListComparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(
                    dataset, recType, metaItemType, context.getBinaryComparatorFactoryProvider());

            IAType secondaryKeyType = null;

            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(0),
                    secondaryKeyExprs.get(0), recType);
            secondaryKeyType = keyPairType.first;

            List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);

            i = 0;
            for (List<String> partitioningKey : partitioningKeys) {
                IAType keyType = recType.getSubFieldType(partitioningKey);
                invListsTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }

            tokenComparatorFactories[0] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
            tokenTypeTraits[0] = NonTaggedFormatUtil.getTokenTypeTrait(secondaryKeyType);
            if (isPartitioned) {
                // The partitioning field is hardcoded to be a short *without*
                // an Asterix type tag.
                tokenComparatorFactories[1] = PointableBinaryComparatorFactory.of(ShortPointable.FACTORY);
                tokenTypeTraits[1] = ShortPointable.TYPE_TRAITS;
            }
            IBinaryTokenizerFactory tokenizerFactory = NonTaggedFormatUtil.getBinaryTokenizerFactory(
                    secondaryKeyType.getTypeTag(), indexType, secondaryIndex.getGramLength());

            ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, recType);
            IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.
                    computeFilterBinaryComparatorFactories(dataset,
                    recType, context.getBinaryComparatorFactoryProvider());

            int[] filterFields = null;
            int[] invertedIndexFields = null;
            int[] filterFieldsForNonBulkLoadOps = null;
            int[] invertedIndexFieldsForNonBulkLoadOps = null;
            if (filterTypeTraits != null) {
                filterFields = new int[1];
                filterFields[0] = numTokenFields + primaryKeys.size();
                invertedIndexFields = new int[numTokenFields + primaryKeys.size()];
                for (int k = 0; k < invertedIndexFields.length; k++) {
                    invertedIndexFields[k] = k;
                }

                filterFieldsForNonBulkLoadOps = new int[numFilterFields];
                filterFieldsForNonBulkLoadOps[0] = numTokenKeyPairFields;
                invertedIndexFieldsForNonBulkLoadOps = new int[numTokenKeyPairFields];
                for (int k = 0; k < invertedIndexFieldsForNonBulkLoadOps.length; k++) {
                    invertedIndexFieldsForNonBulkLoadOps[k] = k;
                }
            }

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    splitProviderAndPartitionConstraintsForDataset(
                    dataverseName, datasetName, indexName, temp);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            IModificationOperationCallbackFactory modificationCallbackFactory = temp
                    ? new TempDatasetSecondaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            modificationCallbackPrimaryKeyFields, txnSubsystemProvider, IndexOperation.UPSERT,
                            ResourceType.LSM_INVERTED_INDEX)
                    : new SecondaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            modificationCallbackPrimaryKeyFields, txnSubsystemProvider, IndexOperation.UPSERT,
                            ResourceType.LSM_INVERTED_INDEX, dataset.hasMetaPart());

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils
                    .getMergePolicyFactory(dataset, mdTxnCtx);
            IIndexDataflowHelperFactory indexDataFlowFactory;
            if (!isPartitioned) {
                indexDataFlowFactory = new LSMInvertedIndexDataflowHelperFactory(
                        new AsterixVirtualBufferCacheProvider(datasetId), compactionInfo.first, compactionInfo.second,
                        new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                        LSMInvertedIndexIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate(), invertedIndexFields, filterTypeTraits,
                        filterCmpFactories, filterFields, filterFieldsForNonBulkLoadOps,
                        invertedIndexFieldsForNonBulkLoadOps, !temp);
            } else {
                indexDataFlowFactory = new PartitionedLSMInvertedIndexDataflowHelperFactory(
                        new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                        compactionInfo.second, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                        LSMInvertedIndexIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate(), invertedIndexFields, filterTypeTraits,
                        filterCmpFactories, filterFields, filterFieldsForNonBulkLoadOps,
                        invertedIndexFieldsForNonBulkLoadOps, !temp);
            }
            IOperatorDescriptor op = new AsterixLSMInvertedIndexUpsertOperatorDescriptor(spec, recordDesc,
                    appContext.getStorageManagerInterface(), splitsAndConstraint.first,
                    appContext.getIndexLifecycleManagerProvider(), tokenTypeTraits, tokenComparatorFactories,
                    invListsTypeTraits, invListComparatorFactories, tokenizerFactory, fieldPermutation,
                    indexDataFlowFactory, filterFactory, modificationCallbackFactory, indexName, prevFieldPermutation);

            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(op, splitsAndConstraint.second);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    //TODO: refactor this method
    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getRTreeUpsertRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalFilteringKeys, AsterixTupleFilterFactory filterFactory,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec,
            List<LogicalVariable> prevSecondaryKeys, List<LogicalVariable> prevAdditionalFilteringKeys)
            throws AlgebricksException {
        try {
            Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);

            boolean temp = dataset.getDatasetDetails().isTemp();
            isTemporaryDatasetWriteJob = isTemporaryDatasetWriteJob && temp;

            String itemTypeName = dataset.getItemTypeName();
            IAType itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, itemTypeName).getDatatype();
            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be indexed.");
            }
            ARecordType recType = (ARecordType) itemType;
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);

            List<List<String>> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();
            List<IAType> secondaryKeyTypes = secondaryIndex.getKeyFieldTypes();
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(0),
                    secondaryKeyExprs.get(0), recType);
            IAType spatialType = keyPairType.first;

            boolean isPointMBR = spatialType.getTypeTag() == ATypeTag.POINT
                    || spatialType.getTypeTag() == ATypeTag.POINT3D;
            int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
            int numSecondaryKeys = dimension * 2;
            int numPrimaryKeys = primaryKeys.size();
            int numKeys = numSecondaryKeys + numPrimaryKeys;
            ITypeTraits[] typeTraits = new ITypeTraits[numKeys];
            IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys];

            int numFilterFields = DatasetUtils.getFilterField(dataset) == null ? 0 : 1;
            int[] fieldPermutation = new int[numKeys + numFilterFields];
            int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
            int i = 0;
            int j = 0;

            // Get field permutation for new value
            for (LogicalVariable varKey : secondaryKeys) {
                int idx = propagatedSchema.findVariable(varKey);
                fieldPermutation[i] = idx;
                i++;
            }
            for (LogicalVariable varKey : primaryKeys) {
                int idx = propagatedSchema.findVariable(varKey);
                fieldPermutation[i] = idx;
                modificationCallbackPrimaryKeyFields[j] = i;
                i++;
                j++;
            }

            if (numFilterFields > 0) {
                int idx = propagatedSchema.findVariable(additionalFilteringKeys.get(0));
                fieldPermutation[numKeys] = idx;
            }

            // Get field permutation for previous value
            int[] prevFieldPermutation = new int[numKeys + numFilterFields];
            i = 0;

            // Get field permutation for new value
            for (LogicalVariable varKey : prevSecondaryKeys) {
                int idx = propagatedSchema.findVariable(varKey);
                prevFieldPermutation[i] = idx;
                i++;
            }
            for (int k = 0; k < numPrimaryKeys; k++) {
                prevFieldPermutation[k + i] = fieldPermutation[k + i];
                i++;
            }

            if (numFilterFields > 0) {
                int idx = propagatedSchema.findVariable(prevAdditionalFilteringKeys.get(0));
                prevFieldPermutation[numKeys] = idx;
            }

            IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(spatialType.getTypeTag());
            IPrimitiveValueProviderFactory[] valueProviderFactories =
                    new IPrimitiveValueProviderFactory[numSecondaryKeys];
            for (i = 0; i < numSecondaryKeys; i++) {
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE
                        .getBinaryComparatorFactory(nestedKeyType, true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(nestedKeyType);
                valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
            }
            List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            for (List<String> partitioningKey : partitioningKeys) {
                IAType keyType = recType.getSubFieldType(partitioningKey);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }

            ARecordType metaItemType = DatasetUtils.getMetaType(this, dataset);
            IBinaryComparatorFactory[] primaryComparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(
                    dataset, recType, metaItemType, context.getBinaryComparatorFactoryProvider());
            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    splitProviderAndPartitionConstraintsForDataset(
                    dataverseName, datasetName, indexName, temp);
            int[] btreeFields = new int[primaryComparatorFactories.length];
            for (int k = 0; k < btreeFields.length; k++) {
                btreeFields[k] = k + numSecondaryKeys;
            }

            ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, recType);
            IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.
                    computeFilterBinaryComparatorFactories(dataset,
                    recType, context.getBinaryComparatorFactoryProvider());
            int[] filterFields = null;
            int[] rtreeFields = null;
            if (filterTypeTraits != null) {
                filterFields = new int[1];
                filterFields[0] = numSecondaryKeys + numPrimaryKeys;
                rtreeFields = new int[numSecondaryKeys + numPrimaryKeys];
                for (int k = 0; k < rtreeFields.length; k++) {
                    rtreeFields[k] = k;
                }
            }

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            IModificationOperationCallbackFactory modificationCallbackFactory = temp
                    ? new TempDatasetSecondaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            modificationCallbackPrimaryKeyFields, txnSubsystemProvider, IndexOperation.UPSERT,
                            ResourceType.LSM_RTREE)
                    : new SecondaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            modificationCallbackPrimaryKeyFields, txnSubsystemProvider, IndexOperation.UPSERT,
                            ResourceType.LSM_RTREE, dataset.hasMetaPart());

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils
                    .getMergePolicyFactory(dataset, mdTxnCtx);
            IBinaryComparatorFactory[] deletedKeyBTreeCompFactories = getMergedComparatorFactories(comparatorFactories,
                    primaryComparatorFactories);
            IIndexDataflowHelperFactory idff = new LSMRTreeWithAntiMatterTuplesDataflowHelperFactory(
                    valueProviderFactories, RTreePolicyType.RTREE, deletedKeyBTreeCompFactories,
                    new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                    compactionInfo.second, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMRTreeIOOperationCallbackFactory.INSTANCE,
                    AqlMetadataProvider.proposeLinearizer(nestedKeyType.getTypeTag(), comparatorFactories.length),
                    rtreeFields, filterTypeTraits, filterCmpFactories, filterFields, !temp, isPointMBR);
            AsterixLSMTreeUpsertOperatorDescriptor op = new AsterixLSMTreeUpsertOperatorDescriptor(spec, recordDesc,
                    appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                    splitsAndConstraint.first, typeTraits, comparatorFactories, null, fieldPermutation, idff,
                    filterFactory, false, indexName, null, modificationCallbackFactory,
                    NoOpOperationCallbackFactory.INSTANCE, prevFieldPermutation);
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(op, splitsAndConstraint.second);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    //TODO: refactor this method
    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getBTreeUpsertRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalFilteringKeys, AsterixTupleFilterFactory filterFactory,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec,
            List<LogicalVariable> prevSecondaryKeys, List<LogicalVariable> prevAdditionalFilteringKeys)
            throws AlgebricksException {
        // we start with the btree
        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }
        boolean temp = dataset.getDatasetDetails().isTemp();
        isTemporaryDatasetWriteJob = isTemporaryDatasetWriteJob && temp;

        int numKeys = primaryKeys.size() + secondaryKeys.size();
        int numFilterFields = DatasetUtils.getFilterField(dataset) == null ? 0 : 1;

        // generate field permutations
        int[] fieldPermutation = new int[numKeys + numFilterFields];
        int[] bloomFilterKeyFields = new int[secondaryKeys.size()];
        int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
        int i = 0;
        int j = 0;
        for (LogicalVariable varKey : secondaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            bloomFilterKeyFields[i] = i;
            i++;
        }
        for (LogicalVariable varKey : primaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            modificationCallbackPrimaryKeyFields[j] = i;
            i++;
            j++;
        }
        // Filter can only be one field!
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalFilteringKeys.get(0));
            fieldPermutation[numKeys] = idx;
        }

        // generate field permutations for prev record
        int[] prevFieldPermutation = new int[numKeys + numFilterFields];
        int k = 0;
        for (LogicalVariable varKey : prevSecondaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            prevFieldPermutation[k] = idx;
            k++;
        }
        for (LogicalVariable varKey : primaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            prevFieldPermutation[k] = idx;
            k++;
        }
        // Filter can only be one field!
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(prevAdditionalFilteringKeys.get(0));
            prevFieldPermutation[numKeys] = idx;
        }

        String itemTypeName = dataset.getItemTypeName();
        IAType itemType;
        try {
            itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getDataverseName(), itemTypeName)
                    .getDatatype();

            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be indexed.");
            }

            ARecordType recType = (ARecordType) itemType;

            // Index parameters.
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);

            ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, recType);
            IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.
                    computeFilterBinaryComparatorFactories(dataset,
                    recType, context.getBinaryComparatorFactoryProvider());
            int[] filterFields = null;
            int[] btreeFields = null;
            if (filterTypeTraits != null) {
                filterFields = new int[1];
                filterFields[0] = numKeys;
                btreeFields = new int[numKeys];
                for (int l = 0; l < btreeFields.length; l++) {
                    btreeFields[l] = l;
                }
            }

            List<List<String>> secondaryKeyNames = secondaryIndex.getKeyFieldNames();
            List<IAType> secondaryKeyTypes = secondaryIndex.getKeyFieldTypes();
            ITypeTraits[] typeTraits = new ITypeTraits[numKeys];
            IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numKeys];
            for (i = 0; i < secondaryKeys.size(); ++i) {
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(i),
                        secondaryKeyNames.get(i), recType);
                IAType keyType = keyPairType.first;
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.
                        getBinaryComparatorFactory(keyType, true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            }
            List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            for (List<String> partitioningKey : partitioningKeys) {
                IAType keyType = recType.getSubFieldType(partitioningKey);
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.
                        getBinaryComparatorFactory(keyType, true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    splitProviderAndPartitionConstraintsForDataset(
                    dataverseName, datasetName, indexName, temp);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            IModificationOperationCallbackFactory modificationCallbackFactory = temp
                    ? new TempDatasetSecondaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            modificationCallbackPrimaryKeyFields, txnSubsystemProvider, IndexOperation.UPSERT,
                            ResourceType.LSM_BTREE)
                    : new SecondaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                            modificationCallbackPrimaryKeyFields, txnSubsystemProvider, IndexOperation.UPSERT,
                            ResourceType.LSM_BTREE, dataset.hasMetaPart());

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils
                    .getMergePolicyFactory(dataset, mdTxnCtx);
            IIndexDataflowHelperFactory idfh = new LSMBTreeDataflowHelperFactory(
                    new AsterixVirtualBufferCacheProvider(datasetId), compactionInfo.first, compactionInfo.second,
                    new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), false, filterTypeTraits, filterCmpFactories,
                    btreeFields, filterFields, !temp);
            AsterixLSMTreeUpsertOperatorDescriptor op = new AsterixLSMTreeUpsertOperatorDescriptor(spec, recordDesc,
                    appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                    splitsAndConstraint.first, typeTraits, comparatorFactories, bloomFilterKeyFields, fieldPermutation,
                    idfh, filterFactory, false, indexName, null, modificationCallbackFactory,
                    NoOpOperationCallbackFactory.INSTANCE, prevFieldPermutation);
            return new Pair<>(op, splitsAndConstraint.second);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }
}
