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

import static org.apache.asterix.common.api.IIdentifierMapper.Modifier.PLURAL;
import static org.apache.asterix.common.metadata.MetadataConstants.METADATA_OBJECT_NAME_INVALID_CHARS;
import static org.apache.asterix.common.utils.IdentifierUtil.dataset;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.INamespaceResolver;
import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.dataflow.LSMTreeInsertDeleteOperatorDescriptor;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IDataSourceAdapter;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.LockList;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.common.storage.ICompressionManager;
import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.dataflow.data.nontagged.MissingWriterFactory;
import org.apache.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import org.apache.asterix.external.adapter.factory.ExternalAdapterFactory;
import org.apache.asterix.external.api.ITypedAdapterFactory;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.operators.ExternalScanOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.provider.AdapterFactoryProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.formats.nontagged.BinaryIntegerInspector;
import org.apache.asterix.formats.nontagged.LinearizeComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.api.ICCExtensionManager;
import org.apache.asterix.metadata.dataset.hints.DatasetHints.DatasetCardinalityHint;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.FullTextConfigMetadataEntity;
import org.apache.asterix.metadata.entities.FullTextFilterMetadataEntity;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.Synonym;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;
import org.apache.asterix.metadata.provider.ExternalWriterProvider;
import org.apache.asterix.metadata.utils.DataPartitioningProvider;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.FullTextUtil;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionExtensionManager;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.runtime.base.AsterixTupleFilterFactory;
import org.apache.asterix.runtime.formats.FormatUtils;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor.BulkLoadUsage;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMSecondaryInsertDeleteWithNestedPlanOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMSecondaryUpsertOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMSecondaryUpsertWithNestedPlanOperatorDescriptor;
import org.apache.asterix.runtime.writer.ExternalWriterFactory;
import org.apache.asterix.runtime.writer.IExternalFilePrinterFactory;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactory;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
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
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.metadata.IWriteDataSink;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.data.IAWriterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.writer.SinkExternalWriterRuntimeFactory;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IResultSerializerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionerFactory;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeBatchPointSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfigEvaluatorFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.rtree.dataflow.RTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

public class MetadataProvider implements IMetadataProvider<DataSourceId, String> {

    private final ICcApplicationContext appCtx;
    private final IStorageComponentProvider storageComponentProvider;
    private final StorageProperties storageProperties;
    private final IFunctionManager functionManager;
    private final LockList locks;
    private final Map<String, Object> config;

    private Namespace defaultNamespace;
    private MetadataTransactionContext mdTxnCtx;
    private boolean isWriteTransaction;
    private FileSplit outputFile;
    private boolean asyncResults;
    private long maxResultReads;
    private ResultSetId resultSetId;
    private Counter resultSetIdCounter;
    private TxnId txnId;
    private boolean blockingOperatorDisabled = false;

    private final DataPartitioningProvider dataPartitioningProvider;
    private final INamespaceResolver namespaceResolver;
    private IDataFormat dataFormat = FormatUtils.getDefaultFormat();

    public static MetadataProvider createWithDefaultNamespace(ICcApplicationContext appCtx) {
        java.util.function.Function<ICcApplicationContext, IMetadataProvider<?, ?>> factory =
                ((ICCExtensionManager) appCtx.getExtensionManager()).getMetadataProviderFactory();
        MetadataProvider mp = factory != null ? (MetadataProvider) factory.apply(appCtx) : new MetadataProvider(appCtx);
        mp.setDefaultNamespace(MetadataConstants.DEFAULT_NAMESPACE);
        return mp;
    }

    public static MetadataProvider create(ICcApplicationContext appCtx, Namespace defaultNamespace) {
        java.util.function.Function<ICcApplicationContext, IMetadataProvider<?, ?>> factory =
                ((ICCExtensionManager) appCtx.getExtensionManager()).getMetadataProviderFactory();
        MetadataProvider mp = factory != null ? (MetadataProvider) factory.apply(appCtx) : new MetadataProvider(appCtx);
        mp.setDefaultNamespace(defaultNamespace);
        return mp;
    }

    protected MetadataProvider(ICcApplicationContext appCtx) {
        this.appCtx = appCtx;
        this.storageComponentProvider = appCtx.getStorageComponentProvider();
        namespaceResolver = appCtx.getNamespaceResolver();
        storageProperties = appCtx.getStorageProperties();
        functionManager = ((IFunctionExtensionManager) appCtx.getExtensionManager()).getFunctionManager();
        dataPartitioningProvider = (DataPartitioningProvider) appCtx.getDataPartitioningProvider();
        locks = new LockList();
        config = new HashMap<>();
        setDefaultNamespace(MetadataConstants.DEFAULT_NAMESPACE);
    }

    @SuppressWarnings("unchecked")
    public <T> T getProperty(String name) {
        return (T) config.get(name);
    }

    public void setProperty(String name, Object value) {
        config.put(name, value);
    }

    @SuppressWarnings("unchecked")
    public <T> T removeProperty(String name) {
        return (T) config.remove(name);
    }

    public boolean getBooleanProperty(String name, boolean defaultValue) {
        Object v = config.get(name);
        return v != null ? Boolean.parseBoolean(String.valueOf(v)) : defaultValue;
    }

    public void disableBlockingOperator() {
        blockingOperatorDisabled = true;
    }

    @Override
    public boolean isBlockingOperatorDisabled() {
        return blockingOperatorDisabled;
    }

    @Override
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setTxnId(TxnId txnId) {
        this.txnId = txnId;
    }

    public void setDefaultNamespace(Namespace namespace) {
        this.defaultNamespace = namespace == null ? MetadataConstants.DEFAULT_NAMESPACE : namespace;
    }

    public Namespace getDefaultNamespace() {
        return defaultNamespace;
    }

    public void setWriteTransaction(boolean writeTransaction) {
        this.isWriteTransaction = writeTransaction;
    }

    public void setMetadataTxnContext(MetadataTransactionContext mdTxnCtx) {
        this.mdTxnCtx = mdTxnCtx;
    }

    public MetadataTransactionContext getMetadataTxnContext() {
        return mdTxnCtx;
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

    public void setMaxResultReads(long maxResultReads) {
        this.maxResultReads = maxResultReads;
    }

    public long getMaxResultReads() {
        return maxResultReads;
    }

    public ResultSetId getResultSetId() {
        return resultSetId;
    }

    public void setResultSetId(ResultSetId resultSetId) {
        this.resultSetId = resultSetId;
    }

    public Counter getResultSetIdCounter() {
        return resultSetIdCounter;
    }

    public void setResultSetIdCounter(Counter resultSetIdCounter) {
        this.resultSetIdCounter = resultSetIdCounter;
    }

    public boolean isWriteTransaction() {
        // The transaction writes persistent datasets.
        return isWriteTransaction;
    }

    public IFunctionManager getFunctionManager() {
        return functionManager;
    }

    public IDataFormat getDataFormat() {
        return dataFormat;
    }

    public void setDataFormat(IDataFormat dataFormat) {
        this.dataFormat = dataFormat;
    }

    public INamespaceResolver getNamespaceResolver() {
        return namespaceResolver;
    }

    public boolean isUsingDatabase() {
        return namespaceResolver.isUsingDatabase();
    }

    public StorageProperties getStorageProperties() {
        return storageProperties;
    }

    /**
     * Retrieve the Output RecordType, as defined by "set output-record-type".
     */
    public ARecordType findOutputRecordType() throws AlgebricksException {
        String database = null;
        DataverseName dataverseName = null;
        if (defaultNamespace != null) {
            database = defaultNamespace.getDatabaseName();
            dataverseName = defaultNamespace.getDataverseName();
        }
        return MetadataManagerUtil.findOutputRecordType(mdTxnCtx, database, dataverseName,
                getProperty("output-record-type"));
    }

    public Dataset findDataset(String database, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        return findDataset(database, dataverseName, datasetName, false);
    }

    public Dataset findDataset(String database, DataverseName dataverseName, String datasetName, boolean includingViews)
            throws AlgebricksException {
        String dbName = database;
        DataverseName dvName = dataverseName;
        if (dbName == null && dvName == null) {
            if (defaultNamespace == null) {
                return null;
            }
            dbName = defaultNamespace.getDatabaseName();
            dvName = defaultNamespace.getDataverseName();
        } else if (dbName == null || dvName == null) {
            return null;
        }
        //TODO(DB): read lock on database
        appCtx.getMetadataLockManager().acquireDataverseReadLock(locks, dbName, dvName);
        appCtx.getMetadataLockManager().acquireDatasetReadLock(locks, dbName, dvName, datasetName);
        return MetadataManagerUtil.findDataset(mdTxnCtx, dbName, dvName, datasetName, includingViews);
    }

    public INodeDomain findNodeDomain(String nodeGroupName) throws AlgebricksException {
        return MetadataManagerUtil.findNodeDomain(appCtx.getClusterStateManager(), mdTxnCtx, nodeGroupName);
    }

    public List<String> findNodes(String nodeGroupName) throws AlgebricksException {
        return MetadataManagerUtil.findNodes(mdTxnCtx, nodeGroupName);
    }

    public Datatype findTypeEntity(String database, DataverseName dataverseName, String typeName)
            throws AlgebricksException {
        return MetadataManagerUtil.findTypeEntity(mdTxnCtx, database, dataverseName, typeName);
    }

    public IAType findTypeForDatasetWithoutType(IAType recordType, IAType metaRecordType, Dataset dataset)
            throws AlgebricksException {
        return MetadataManagerUtil.findTypeForDatasetWithoutType(recordType, metaRecordType, dataset);
    }

    public IAType findType(String database, DataverseName dataverseName, String typeName) throws AlgebricksException {
        return MetadataManagerUtil.findType(mdTxnCtx, database, dataverseName, typeName);
    }

    public IAType findType(Dataset dataset) throws AlgebricksException {
        return findType(dataset.getItemTypeDatabaseName(), dataset.getItemTypeDataverseName(),
                dataset.getItemTypeName());
    }

    public IAType findMetaType(Dataset dataset) throws AlgebricksException {
        return findType(dataset.getMetaItemTypeDatabaseName(), dataset.getMetaItemTypeDataverseName(),
                dataset.getMetaItemTypeName());
    }

    public Feed findFeed(String database, DataverseName dataverseName, String feedName) throws AlgebricksException {
        return MetadataManagerUtil.findFeed(mdTxnCtx, database, dataverseName, feedName);
    }

    public FeedConnection findFeedConnection(String database, DataverseName dataverseName, String feedName,
            String datasetName) throws AlgebricksException {
        return MetadataManagerUtil.findFeedConnection(mdTxnCtx, database, dataverseName, feedName, datasetName);
    }

    public FeedPolicyEntity findFeedPolicy(String database, DataverseName dataverseName, String policyName)
            throws AlgebricksException {
        return MetadataManagerUtil.findFeedPolicy(mdTxnCtx, database, dataverseName, policyName);
    }

    @Override
    public DataSource findDataSource(DataSourceId id) throws AlgebricksException {
        return MetadataManagerUtil.findDataSource(appCtx.getClusterStateManager(), mdTxnCtx, id);
    }

    public DataSource lookupSourceInMetadata(DataSourceId aqlId) throws AlgebricksException {
        return MetadataManagerUtil.lookupSourceInMetadata(appCtx.getClusterStateManager(), mdTxnCtx, aqlId);
    }

    @Override
    public IDataSourceIndex<String, DataSourceId> findDataSourceIndex(String indexId, DataSourceId dataSourceId)
            throws AlgebricksException {
        DataSource source = findDataSource(dataSourceId);
        Dataset dataset = ((DatasetDataSource) source).getDataset();
        // index could be a primary index or secondary index
        DataverseName dataverseName = dataset.getDataverseName();
        String database = dataset.getDatabaseName();
        String datasetName = dataset.getDatasetName();
        Index index = getIndex(database, dataverseName, datasetName, indexId);
        return index != null ? new DataSourceIndex(index, database, dataverseName, datasetName, this) : null;
    }

    public Index getIndex(String database, DataverseName dataverseName, String datasetName, String indexName)
            throws AlgebricksException {
        return MetadataManager.INSTANCE.getIndex(mdTxnCtx, database, dataverseName, datasetName, indexName);
    }

    public List<Index> getDatasetIndexes(String database, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        return MetadataManagerUtil.getDatasetIndexes(mdTxnCtx, database, dataverseName, datasetName);
    }

    public Index findSampleIndex(String database, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        Pair<String, String> sampleIndexNames = IndexUtil.getSampleIndexNames(datasetName);
        Index sampleIndex = getIndex(database, dataverseName, datasetName, sampleIndexNames.first);
        if (sampleIndex != null && sampleIndex.getPendingOp() == MetadataUtil.PENDING_NO_OP) {
            return sampleIndex;
        }
        sampleIndex = getIndex(database, dataverseName, datasetName, sampleIndexNames.second);
        return sampleIndex != null && sampleIndex.getPendingOp() == MetadataUtil.PENDING_NO_OP ? sampleIndex : null;
    }

    public Quadruple<DataverseName, String, Boolean, String> resolveDatasetNameUsingSynonyms(String databaseName,
            DataverseName dataverseName, String datasetName, boolean includingViews) throws AlgebricksException {
        String dbName = databaseName;
        DataverseName dvName = dataverseName;
        if (dbName == null && dvName == null) {
            if (defaultNamespace == null) {
                return null;
            }
            dbName = defaultNamespace.getDatabaseName();
            dvName = defaultNamespace.getDataverseName();
        } else if (dbName == null || dvName == null) {
            return null;
        }
        Synonym synonym = null;
        while (MetadataManagerUtil.findDataset(mdTxnCtx, dbName, dvName, datasetName, includingViews) == null) {
            synonym = findSynonym(dbName, dvName, datasetName);
            if (synonym == null) {
                return null;
            }
            dbName = synonym.getObjectDatabaseName();
            dvName = synonym.getObjectDataverseName();
            datasetName = synonym.getObjectName();
        }
        return new Quadruple<>(dvName, datasetName, synonym != null, dbName);
    }

    public Synonym findSynonym(String database, DataverseName dataverseName, String synonymName)
            throws AlgebricksException {
        return MetadataManagerUtil.findSynonym(mdTxnCtx, database, dataverseName, synonymName);
    }

    public FullTextConfigMetadataEntity findFullTextConfig(String database, DataverseName dataverseName,
            String ftConfigName) throws AlgebricksException {
        return MetadataManagerUtil.findFullTextConfigDescriptor(mdTxnCtx, database, dataverseName, ftConfigName);
    }

    public FullTextFilterMetadataEntity findFullTextFilter(String database, DataverseName dataverseName,
            String ftFilterName) throws AlgebricksException {
        return MetadataManagerUtil.findFullTextFilterDescriptor(mdTxnCtx, database, dataverseName, ftFilterName);
    }

    @Override
    public IFunctionInfo lookupFunction(FunctionIdentifier fid) {
        return BuiltinFunctions.getBuiltinFunctionInfo(fid);
    }

    public Function lookupUserDefinedFunction(FunctionSignature signature) throws AlgebricksException {
        //TODO(DB):
        if (signature.getDataverseName() == null) {
            return null;
        }
        return MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(
            IDataSource<DataSourceId> dataSource, List<LogicalVariable> scanVariables,
            List<LogicalVariable> projectVariables, boolean projectPushed, List<LogicalVariable> minFilterVars,
            List<LogicalVariable> maxFilterVars, ITupleFilterFactory tupleFilterFactory, long outputLimit,
            IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec,
            Object implConfig, IProjectionFiltrationInfo projectionFiltrationInfo) throws AlgebricksException {
        return ((DataSource) dataSource).buildDatasourceScanRuntime(this, dataSource, scanVariables, projectVariables,
                projectPushed, minFilterVars, maxFilterVars, tupleFilterFactory, outputLimit, opSchema, typeEnv,
                context, jobSpec, implConfig, projectionFiltrationInfo);
    }

    protected Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getLoadableDatasetScanRuntime(
            JobSpecification jobSpec, ITypedAdapterFactory adapterFactory, RecordDescriptor rDesc)
            throws AlgebricksException {
        ExternalScanOperatorDescriptor dataScanner = new ExternalScanOperatorDescriptor(jobSpec, rDesc, adapterFactory);
        try {
            return new Pair<>(dataScanner, adapterFactory.getPartitionConstraint());
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    public Dataverse findDataverse(String database, DataverseName dataverseName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getDataverse(mdTxnCtx, database, dataverseName);
    }

    public Triple<IOperatorDescriptor, AlgebricksPartitionConstraint, ITypedAdapterFactory> getFeedIntakeRuntime(
            JobSpecification jobSpec, Feed feed, FeedPolicyAccessor policyAccessor) throws Exception {
        Triple<ITypedAdapterFactory, RecordDescriptor, IDataSourceAdapter.AdapterType> factoryOutput;
        factoryOutput =
                FeedMetadataUtil.getFeedFactoryAndOutput(feed, policyAccessor, mdTxnCtx, getApplicationContext());
        ARecordType recordType =
                FeedMetadataUtil.getOutputType(feed, feed.getConfiguration().get(ExternalDataConstants.KEY_TYPE_NAME));
        ITypedAdapterFactory adapterFactory = factoryOutput.first;
        FeedIntakeOperatorDescriptor feedIngestor = null;
        switch (factoryOutput.third) {
            case INTERNAL:
                feedIngestor = new FeedIntakeOperatorDescriptor(jobSpec, feed, adapterFactory, recordType,
                        policyAccessor, factoryOutput.second);
                break;
            case EXTERNAL:
                ExternalAdapterFactory extAdapterFactory = (ExternalAdapterFactory) adapterFactory;
                feedIngestor = new FeedIntakeOperatorDescriptor(jobSpec, feed, extAdapterFactory.getLibraryDatabase(),
                        extAdapterFactory.getLibraryDataverse(), extAdapterFactory.getLibraryName(),
                        extAdapterFactory.getClassName(), recordType, policyAccessor, factoryOutput.second);
                break;
            default:
                break;
        }

        AlgebricksPartitionConstraint partitionConstraint = adapterFactory.getPartitionConstraint();
        return new Triple<>(feedIngestor, partitionConstraint, adapterFactory);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getBtreeSearchRuntime(JobSpecification jobSpec,
            IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv, JobGenContext context, boolean retainInput,
            boolean retainMissing, IMissingWriterFactory nonMatchWriterFactory, Dataset dataset, String indexName,
            int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            boolean propagateFilter, IMissingWriterFactory nonFilterWriterFactory, int[] minFilterFieldIndexes,
            int[] maxFilterFieldIndexes, ITupleFilterFactory tupleFilterFactory, long outputLimit,
            boolean isIndexOnlyPlan, boolean isPrimaryIndexPointSearch, ITupleProjectorFactory tupleProjectorFactory,
            boolean partitionInputTuples) throws AlgebricksException {
        boolean isSecondary = true;
        Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDatabaseName(),
                dataset.getDataverseName(), dataset.getDatasetName(), dataset.getDatasetName());
        if (primaryIndex != null && (dataset.getDatasetType() != DatasetType.EXTERNAL)) {
            isSecondary = !indexName.equals(primaryIndex.getIndexName());
        }
        Index theIndex = isSecondary ? MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDatabaseName(),
                dataset.getDataverseName(), dataset.getDatasetName(), indexName) : primaryIndex;

        int numSecondaryKeys;
        switch (theIndex.getIndexType()) {
            case ARRAY:
                numSecondaryKeys = ((Index.ArrayIndexDetails) theIndex.getIndexDetails()).getElementList().stream()
                        .map(e -> e.getProjectList().size()).reduce(0, Integer::sum);
                break;
            case BTREE:
                numSecondaryKeys = ((Index.ValueIndexDetails) theIndex.getIndexDetails()).getKeyFieldNames().size();
                break;
            case SAMPLE:
                if (isIndexOnlyPlan) {
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "");
                }
                numSecondaryKeys = 0;
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE,
                        theIndex.getIndexType().toString());
        }

        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
        PartitioningProperties datasetPartitioningProp = getPartitioningProperties(dataset, theIndex.getIndexName());
        int[] primaryKeyFields = new int[numPrimaryKeys];
        for (int i = 0; i < numPrimaryKeys; i++) {
            primaryKeyFields[i] = i;
        }

        int[] primaryKeyFieldsInSecondaryIndex = null;
        byte[] successValueForIndexOnlyPlan = null;
        byte[] failValueForIndexOnlyPlan = null;
        boolean proceedIndexOnlyPlan = isIndexOnlyPlan && isSecondary;
        if (proceedIndexOnlyPlan) {
            primaryKeyFieldsInSecondaryIndex = new int[numPrimaryKeys];
            for (int i = 0; i < numPrimaryKeys; i++) {
                primaryKeyFieldsInSecondaryIndex[i] = i + numSecondaryKeys;
            }
            // Defines the return value from a secondary index search if this is an index-only plan.
            failValueForIndexOnlyPlan = SerializerDeserializerUtil.computeByteArrayForIntValue(0);
            successValueForIndexOnlyPlan = SerializerDeserializerUtil.computeByteArrayForIntValue(1);
        }

        ISearchOperationCallbackFactory searchCallbackFactory =
                dataset.getSearchCallbackFactory(storageComponentProvider, theIndex, IndexOperation.SEARCH,
                        primaryKeyFields, primaryKeyFieldsInSecondaryIndex, proceedIndexOnlyPlan);
        IStorageManager storageManager = getStorageComponentProvider().getStorageManager();
        IIndexDataflowHelperFactory indexHelperFactory =
                new IndexDataflowHelperFactory(storageManager, datasetPartitioningProp.getSplitsProvider());
        BTreeSearchOperatorDescriptor btreeSearchOp;

        int[][] partitionsMap = datasetPartitioningProp.getComputeStorageMap();
        ITuplePartitionerFactory tuplePartitionerFactory = null;
        if (partitionInputTuples) {
            IBinaryHashFunctionFactory[] pkHashFunFactories = dataset.getPrimaryHashFunctionFactories(this);
            tuplePartitionerFactory = new FieldHashPartitionerFactory(lowKeyFields, pkHashFunFactories,
                    datasetPartitioningProp.getNumberOfPartitions());
        }

        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            btreeSearchOp = !isSecondary && isPrimaryIndexPointSearch
                    ? new LSMBTreeBatchPointSearchOperatorDescriptor(jobSpec, outputRecDesc, lowKeyFields,
                            highKeyFields, lowKeyInclusive, highKeyInclusive, indexHelperFactory, retainInput,
                            retainMissing, nonMatchWriterFactory, searchCallbackFactory, minFilterFieldIndexes,
                            maxFilterFieldIndexes, tupleFilterFactory, outputLimit, tupleProjectorFactory,
                            tuplePartitionerFactory, partitionsMap)
                    : new BTreeSearchOperatorDescriptor(jobSpec, outputRecDesc, lowKeyFields, highKeyFields,
                            lowKeyInclusive, highKeyInclusive, indexHelperFactory, retainInput, retainMissing,
                            nonMatchWriterFactory, searchCallbackFactory, minFilterFieldIndexes, maxFilterFieldIndexes,
                            propagateFilter, nonFilterWriterFactory, tupleFilterFactory, outputLimit,
                            proceedIndexOnlyPlan, failValueForIndexOnlyPlan, successValueForIndexOnlyPlan,
                            tupleProjectorFactory, tuplePartitionerFactory, partitionsMap);
        } else {
            btreeSearchOp = null;
        }
        return new Pair<>(btreeSearchOp, datasetPartitioningProp.getConstraints());
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getRtreeSearchRuntime(JobSpecification jobSpec,
            List<LogicalVariable> outputVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            JobGenContext context, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory nonMatchWriterFactory, Dataset dataset, String indexName, int[] keyFields,
            boolean propagateFilter, IMissingWriterFactory nonFilterWriterFactory, int[] minFilterFieldIndexes,
            int[] maxFilterFieldIndexes, boolean isIndexOnlyPlan) throws AlgebricksException {
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDatabaseName(),
                dataset.getDataverseName(), dataset.getDatasetName(), indexName);
        if (secondaryIndex == null) {
            throw new AlgebricksException("Code generation error: no index " + indexName + " for " + dataset() + " "
                    + dataset.getDatasetName());
        }
        Index.ValueIndexDetails secondaryIndexDetails = (Index.ValueIndexDetails) secondaryIndex.getIndexDetails();
        RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
        PartitioningProperties partitioningProperties =
                getPartitioningProperties(dataset, secondaryIndex.getIndexName());
        int[] primaryKeyFields = new int[numPrimaryKeys];
        for (int i = 0; i < numPrimaryKeys; i++) {
            primaryKeyFields[i] = i;
        }

        int[] primaryKeyFieldsInSecondaryIndex = null;
        byte[] successValueForIndexOnlyPlan = null;
        byte[] failValueForIndexOnlyPlan = null;
        if (isIndexOnlyPlan) {
            ARecordType recType = (ARecordType) findType(dataset.getItemTypeDatabaseName(),
                    dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
            List<List<String>> secondaryKeyFields = secondaryIndexDetails.getKeyFieldNames();
            List<IAType> secondaryKeyTypes = secondaryIndexDetails.getKeyFieldTypes();
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(secondaryIndex,
                    secondaryKeyTypes.get(0), secondaryKeyFields.get(0), recType);
            IAType keyType = keyTypePair.first;
            int numDimensions = NonTaggedFormatUtil.getNumDimensions(keyType.getTypeTag());
            int numNestedSecondaryKeyFields = numDimensions * 2;
            primaryKeyFieldsInSecondaryIndex = new int[numPrimaryKeys];
            for (int i = 0; i < numPrimaryKeys; i++) {
                primaryKeyFieldsInSecondaryIndex[i] = i + numNestedSecondaryKeyFields;
            }
            // Defines the return value from a secondary index search if this is an index-only plan.
            failValueForIndexOnlyPlan = SerializerDeserializerUtil.computeByteArrayForIntValue(0);
            successValueForIndexOnlyPlan = SerializerDeserializerUtil.computeByteArrayForIntValue(1);
        }

        ISearchOperationCallbackFactory searchCallbackFactory =
                dataset.getSearchCallbackFactory(storageComponentProvider, secondaryIndex, IndexOperation.SEARCH,
                        primaryKeyFields, primaryKeyFieldsInSecondaryIndex, isIndexOnlyPlan);
        RTreeSearchOperatorDescriptor rtreeSearchOp;
        IIndexDataflowHelperFactory indexDataflowHelperFactory = new IndexDataflowHelperFactory(
                storageComponentProvider.getStorageManager(), partitioningProperties.getSplitsProvider());
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            int[][] partitionsMap = partitioningProperties.getComputeStorageMap();
            rtreeSearchOp = new RTreeSearchOperatorDescriptor(jobSpec, outputRecDesc, keyFields, true, true,
                    indexDataflowHelperFactory, retainInput, retainMissing, nonMatchWriterFactory,
                    searchCallbackFactory, minFilterFieldIndexes, maxFilterFieldIndexes, propagateFilter,
                    nonFilterWriterFactory, isIndexOnlyPlan, failValueForIndexOnlyPlan, successValueForIndexOnlyPlan,
                    partitionsMap);
        } else {
            // Create the operator
            rtreeSearchOp = null;
        }

        return new Pair<>(rtreeSearchOp, partitioningProperties.getConstraints());
    }

    @Override
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(int sourceColumn,
            int[] partitionColumns, IBinaryComparatorFactory[] partitionComparatorFactories,
            IScalarEvaluatorFactory dynamicPathEvalFactory, ILogicalExpression staticPathExpr,
            SourceLocation pathSourceLocation, IWriteDataSink sink, RecordDescriptor inputDesc, Object sourceType)
            throws AlgebricksException {
        String staticPath = staticPathExpr != null ? ConstantExpressionUtil.getStringConstant(staticPathExpr) : null;
        IExternalFileWriterFactory fileWriterFactory =
                ExternalWriterProvider.createWriterFactory(appCtx, sink, staticPath, pathSourceLocation);
        fileWriterFactory.validate();
        String fileExtension = ExternalWriterProvider.getFileExtension(sink);
        int maxResult = ExternalWriterProvider.getMaxResult(sink);
        IExternalFilePrinterFactory printerFactory = ExternalWriterProvider.createPrinter(sink, sourceType);
        ExternalWriterFactory writerFactory = new ExternalWriterFactory(fileWriterFactory, printerFactory,
                fileExtension, maxResult, dynamicPathEvalFactory, staticPath, pathSourceLocation);
        SinkExternalWriterRuntimeFactory runtime = new SinkExternalWriterRuntimeFactory(sourceColumn, partitionColumns,
                partitionComparatorFactories, inputDesc, writerFactory);
        return new Pair<>(runtime, null);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getResultHandleRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, IAWriterFactory writerFactory,
            IResultSerializerFactoryProvider resultSerializerFactoryProvider, RecordDescriptor inputDesc,
            IResultMetadata metadata, JobSpecification spec) throws AlgebricksException {
        ResultSetDataSink rsds = (ResultSetDataSink) sink;
        ResultSetSinkId rssId = rsds.getId();
        ResultSetId rsId = rssId.getResultSetId();
        ResultWriterOperatorDescriptor resultWriter = null;
        try {
            IResultSerializerFactory resultSerializedAppenderFactory = resultSerializerFactoryProvider
                    .getResultSerializerFactoryProvider(printColumns, printerFactories, writerFactory);
            resultWriter = new ResultWriterOperatorDescriptor(spec, rsId, metadata, getResultAsyncMode(),
                    resultSerializedAppenderFactory, getMaxResultReads());
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        return new Pair<>(resultWriter, null);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(
            IDataSource<DataSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payload, List<LogicalVariable> additionalNonKeyFields,
            List<LogicalVariable> additionalNonFilteringFields, RecordDescriptor inputRecordDesc, JobGenContext context,
            JobSpecification spec, boolean bulkload) throws AlgebricksException {
        return getInsertOrDeleteRuntime(IndexOperation.INSERT, dataSource, propagatedSchema, keys, payload,
                additionalNonKeyFields, inputRecordDesc, context, spec, bulkload, additionalNonFilteringFields);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(
            IDataSource<DataSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payload, List<LogicalVariable> additionalNonKeyFields,
            List<LogicalVariable> additionalNonFilteringFields, RecordDescriptor inputRecordDesc, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        return getInsertOrDeleteRuntime(IndexOperation.DELETE, dataSource, propagatedSchema, keys, payload,
                additionalNonKeyFields, inputRecordDesc, context, spec, false, additionalNonFilteringFields);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getUpsertRuntime(
            IDataSource<DataSourceId> dataSource, IOperatorSchema inputSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, LogicalVariable payload, List<LogicalVariable> filterKeys,
            List<LogicalVariable> additionalNonFilterFields, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        DataverseName dataverseName = dataSource.getId().getDataverseName();
        String datasetName = dataSource.getId().getDatasourceName();
        String database = dataSource.getId().getDatabaseName();
        Dataset dataset = findDataset(database, dataverseName, datasetName);
        if (dataset == null) {
            throw new AsterixException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, datasetName,
                    MetadataUtil.dataverseName(database, dataverseName, isUsingDatabase()));
        }
        int numKeys = primaryKeys.size();
        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;
        int numOfAdditionalFields = additionalNonFilterFields == null ? 0 : additionalNonFilterFields.size();
        // Move key fields to front. [keys, record, filters]
        int[] fieldPermutation = new int[numKeys + 1 + numFilterFields + numOfAdditionalFields];
        int[] bloomFilterKeyFields = new int[numKeys];
        int i = 0;
        // set the keys' permutations
        for (LogicalVariable varKey : primaryKeys) {
            int idx = inputSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            bloomFilterKeyFields[i] = i;
            i++;
        }
        // set the record permutation
        fieldPermutation[i++] = inputSchema.findVariable(payload);

        // set the meta record permutation
        if (additionalNonFilterFields != null) {
            for (LogicalVariable var : additionalNonFilterFields) {
                int idx = inputSchema.findVariable(var);
                fieldPermutation[i++] = idx;
            }
        }

        // set the filters' permutations.
        if (numFilterFields > 0) {
            int idx = inputSchema.findVariable(filterKeys.get(0));
            fieldPermutation[i++] = idx;
        }

        return createPrimaryIndexUpsertOp(spec, this, dataset, recordDesc, fieldPermutation,
                context.getMissingWriterFactory());
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<String, DataSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec,
            boolean bulkload, List<List<AlgebricksPipeline>> secondaryKeysPipelines, IOperatorSchema pipelineTopSchema)
            throws AlgebricksException {
        return getIndexModificationRuntime(IndexOperation.INSERT, dataSourceIndex, propagatedSchema, inputSchemas,
                typeEnv, primaryKeys, secondaryKeys, additionalNonKeyFields, filterExpr, null, recordDesc, context,
                spec, bulkload, null, null, null, secondaryKeysPipelines, pipelineTopSchema);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<String, DataSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec,
            List<List<AlgebricksPipeline>> secondaryKeysPipelines, IOperatorSchema pipelineTopSchema)
            throws AlgebricksException {
        return getIndexModificationRuntime(IndexOperation.DELETE, dataSourceIndex, propagatedSchema, inputSchemas,
                typeEnv, primaryKeys, secondaryKeys, additionalNonKeyFields, filterExpr, null, recordDesc, context,
                spec, false, null, null, null, secondaryKeysPipelines, pipelineTopSchema);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexUpsertRuntime(
            IDataSourceIndex<String, DataSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalFilteringKeys,
            ILogicalExpression filterExpr, ILogicalExpression prevFilterExpr, LogicalVariable operationVar,
            List<LogicalVariable> prevSecondaryKeys, LogicalVariable prevAdditionalFilteringKey,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec,
            List<List<AlgebricksPipeline>> secondaryKeysPipelines) throws AlgebricksException {
        return getIndexModificationRuntime(IndexOperation.UPSERT, dataSourceIndex, propagatedSchema, inputSchemas,
                typeEnv, primaryKeys, secondaryKeys, additionalFilteringKeys, filterExpr, prevFilterExpr, recordDesc,
                context, spec, false, operationVar, prevSecondaryKeys, prevAdditionalFilteringKey,
                secondaryKeysPipelines, null);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getTokenizerRuntime(
            IDataSourceIndex<String, DataSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec, boolean bulkload) throws AlgebricksException {

        String indexName = dataSourceIndex.getId();
        DataverseName dataverseName = dataSourceIndex.getDataSource().getId().getDataverseName();
        String datasetName = dataSourceIndex.getDataSource().getId().getDatasourceName();
        String database = dataSourceIndex.getDataSource().getId().getDatabaseName();

        IOperatorSchema inputSchema;
        if (inputSchemas.length > 0) {
            inputSchema = inputSchemas[0];
        } else {
            throw new AlgebricksException("TokenizeOperator can not operate without any input variable.");
        }

        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, database, dataverseName, datasetName);
        Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDatabaseName(),
                dataset.getDataverseName(), dataset.getDatasetName(), indexName);
        // TokenizeOperator only supports a keyword or n-gram index.
        switch (secondaryIndex.getIndexType()) {
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
                return getBinaryTokenizerRuntime(database, dataverseName, datasetName, indexName, inputSchema,
                        propagatedSchema, primaryKeys, secondaryKeys, recordDesc, spec, secondaryIndex.getIndexType());
            default:
                throw new AlgebricksException("Currently, we do not support TokenizeOperator for the index type: "
                        + secondaryIndex.getIndexType());
        }
    }

    /**
     * Calculate an estimate size of the bloom filter. Note that this is an
     * estimation which assumes that the data is going to be uniformly distributed
     * across all partitions.
     *
     * @param dataset
     * @return Number of elements that will be used to create a bloom filter per
     * dataset per partition
     * @throws AlgebricksException
     */
    public long getCardinalityPerPartitionHint(Dataset dataset) throws AlgebricksException {
        String numElementsHintString = dataset.getHints().get(DatasetCardinalityHint.NAME);
        long numElementsHint;
        if (numElementsHintString == null) {
            numElementsHint = DatasetCardinalityHint.DEFAULT;
        } else {
            numElementsHint = Long.parseLong(numElementsHintString);
        }
        int numPartitions = getPartitioningProperties(dataset).getNumberOfPartitions();
        return numElementsHint / numPartitions;
    }

    protected ITypedAdapterFactory getConfiguredAdapterFactory(Dataset dataset, String adapterName,
            Map<String, String> configuration, ARecordType itemType, IWarningCollector warningCollector,
            IExternalFilterEvaluatorFactory filterEvaluatorFactory) throws AlgebricksException {
        try {
            configuration.put(ExternalDataConstants.KEY_DATABASE_DATAVERSE, dataset.getDatabaseName());
            configuration.put(ExternalDataConstants.KEY_DATASET_DATAVERSE,
                    dataset.getDataverseName().getCanonicalForm());
            return AdapterFactoryProvider.getAdapterFactory(getApplicationContext().getServiceContext(), adapterName,
                    configuration, itemType, null, warningCollector, filterEvaluatorFactory);
        } catch (Exception e) {
            throw new AlgebricksException("Unable to create adapter", e);
        }
    }

    public TxnId getTxnId() {
        return txnId;
    }

    public static ILinearizeComparatorFactory proposeLinearizer(ATypeTag keyType, int numKeyFields)
            throws AlgebricksException {
        return LinearizeComparatorFactoryProvider.INSTANCE.getLinearizeComparatorFactory(keyType, true,
                numKeyFields / 2);
    }

    public PartitioningProperties splitAndConstraints(String databaseName) {
        return dataPartitioningProvider.getPartitioningProperties(databaseName);
    }

    public PartitioningProperties splitAndConstraints(String databaseName, DataverseName dataverseName) {
        return dataPartitioningProvider.getPartitioningProperties(databaseName, dataverseName);
    }

    public FileSplit[] splitsForIndex(MetadataTransactionContext mdTxnCtx, Dataset dataset, String indexName)
            throws AlgebricksException {
        return dataPartitioningProvider.getPartitioningProperties(mdTxnCtx, dataset, indexName).getSplitsProvider()
                .getFileSplits();
    }

    public DatasourceAdapter getAdapter(MetadataTransactionContext mdTxnCtx, String database,
            DataverseName dataverseName, String adapterName) throws AlgebricksException {
        DatasourceAdapter adapter;
        // search in default namespace (built-in adapter)
        adapter = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.SYSTEM_DATABASE,
                MetadataConstants.METADATA_DATAVERSE_NAME, adapterName);

        // search in dataverse (user-defined adapter)
        if (adapter == null) {
            adapter = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, database, dataverseName, adapterName);
        }
        return adapter;
    }

    public AlgebricksAbsolutePartitionConstraint getClusterLocations() {
        return appCtx.getClusterStateManager().getClusterLocations();
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDataLookupRuntime(
            JobSpecification jobSpec, Dataset dataset, int[] ridIndexes, boolean retainInput,
            IVariableTypeEnvironment typeEnv, IOperatorSchema opSchema, JobGenContext context,
            MetadataProvider metadataProvider, boolean retainMissing) throws AlgebricksException {
        return null;
    }

    protected Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> createPrimaryIndexUpsertOp(JobSpecification spec,
            MetadataProvider metadataProvider, Dataset dataset, RecordDescriptor inputRecordDesc,
            int[] fieldPermutation, IMissingWriterFactory missingWriterFactory) throws AlgebricksException {
        // this can be used by extensions to pick up their own operators
        return DatasetUtil.createPrimaryIndexUpsertOp(spec, this, dataset, inputRecordDesc, fieldPermutation,
                missingWriterFactory);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getExternalDatasetScanRuntime(
            JobSpecification jobSpec, IAType itemType, ITypedAdapterFactory adapterFactory,
            ITupleFilterFactory tupleFilterFactory, long outputLimit) throws AlgebricksException {
        if (itemType.getTypeTag() != ATypeTag.OBJECT) {
            throw new AlgebricksException("Can only scan " + dataset(PLURAL) + "of records.");
        }

        ISerializerDeserializer<?> payloadSerde =
                getDataFormat().getSerdeProvider().getSerializerDeserializer(itemType);
        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] { payloadSerde });

        ExternalScanOperatorDescriptor dataScanner = new ExternalScanOperatorDescriptor(jobSpec, scannerDesc,
                adapterFactory, tupleFilterFactory, outputLimit);

        //TODO(partitioning) check
        AlgebricksPartitionConstraint constraint;
        try {
            constraint = adapterFactory.getPartitionConstraint();
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }

        return new Pair<>(dataScanner, constraint);
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertOrDeleteRuntime(IndexOperation indexOp,
            IDataSource<DataSourceId> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payload, List<LogicalVariable> additionalNonKeyFields, RecordDescriptor inputRecordDesc,
            JobGenContext context, JobSpecification spec, boolean bulkload,
            List<LogicalVariable> additionalNonFilteringFields) throws AlgebricksException {

        String datasetName = dataSource.getId().getDatasourceName();
        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, dataSource.getId().getDatabaseName(),
                dataSource.getId().getDataverseName(), datasetName);
        int numKeys = keys.size();
        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;
        // Move key fields to front.
        int[] fieldPermutation = new int[numKeys + 1 + numFilterFields
                + (additionalNonFilteringFields == null ? 0 : additionalNonFilteringFields.size())];
        int[] bloomFilterKeyFields = new int[numKeys];
        int[] pkFields = new int[numKeys];
        int i = 0;
        for (LogicalVariable varKey : keys) {
            int idx = propagatedSchema.findVariable(varKey);
            pkFields[i] = idx;
            fieldPermutation[i] = idx;
            bloomFilterKeyFields[i] = i;
            i++;
        }
        fieldPermutation[i++] = propagatedSchema.findVariable(payload);

        if (additionalNonFilteringFields != null) {
            for (LogicalVariable variable : additionalNonFilteringFields) {
                int idx = propagatedSchema.findVariable(variable);
                fieldPermutation[i++] = idx;
            }
        }

        int[] filterFields = new int[numFilterFields];
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalNonKeyFields.get(0));
            fieldPermutation[i++] = idx;
            filterFields[0] = idx;
        }

        Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDatabaseName(),
                dataset.getDataverseName(), dataset.getDatasetName(), dataset.getDatasetName());
        PartitioningProperties partitioningProperties = getPartitioningProperties(dataset);

        // prepare callback
        int[] primaryKeyFields = new int[numKeys];
        for (i = 0; i < numKeys; i++) {
            primaryKeyFields[i] = i;
        }
        IModificationOperationCallbackFactory modificationCallbackFactory = dataset
                .getModificationCallbackFactory(storageComponentProvider, primaryIndex, indexOp, primaryKeyFields);
        IIndexDataflowHelperFactory idfh = new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(),
                partitioningProperties.getSplitsProvider());
        IBinaryHashFunctionFactory[] pkHashFunFactories = dataset.getPrimaryHashFunctionFactories(this);
        //TODO(partitioning) rename to static
        ITuplePartitionerFactory partitionerFactory = new FieldHashPartitionerFactory(pkFields, pkHashFunFactories,
                partitioningProperties.getNumberOfPartitions());

        IOperatorDescriptor op;
        if (bulkload) {
            long numElementsHint = getCardinalityPerPartitionHint(dataset);
            op = new LSMIndexBulkLoadOperatorDescriptor(spec, inputRecordDesc, fieldPermutation,
                    StorageConstants.DEFAULT_TREE_FILL_FACTOR, true, numElementsHint, true, idfh, null,
                    BulkLoadUsage.LOAD, dataset.getDatasetId(), null, partitionerFactory,
                    partitioningProperties.getComputeStorageMap());
        } else {
            if (indexOp == IndexOperation.INSERT) {
                ISearchOperationCallbackFactory searchCallbackFactory = dataset
                        .getSearchCallbackFactory(storageComponentProvider, primaryIndex, indexOp, primaryKeyFields);

                Optional<Index> primaryKeyIndex = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx,
                        dataset.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName()).stream()
                        .filter(Index::isPrimaryKeyIndex).findFirst();
                IIndexDataflowHelperFactory pkidfh = null;
                if (primaryKeyIndex.isPresent()) {
                    PartitioningProperties idxPartitioningProperties =
                            getPartitioningProperties(dataset, primaryKeyIndex.get().getIndexName());
                    pkidfh = new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(),
                            idxPartitioningProperties.getSplitsProvider());
                }
                op = createLSMPrimaryInsertOperatorDescriptor(spec, inputRecordDesc, fieldPermutation, idfh, pkidfh,
                        modificationCallbackFactory, searchCallbackFactory, numKeys, filterFields, partitionerFactory,
                        partitioningProperties.getComputeStorageMap());

            } else {
                op = createLSMTreeInsertDeleteOperatorDescriptor(spec, inputRecordDesc, fieldPermutation, indexOp, idfh,
                        null, true, modificationCallbackFactory, partitionerFactory,
                        partitioningProperties.getComputeStorageMap());
            }
        }
        return new Pair<>(op, partitioningProperties.getConstraints());
    }

    protected LSMPrimaryInsertOperatorDescriptor createLSMPrimaryInsertOperatorDescriptor(JobSpecification spec,
            RecordDescriptor inputRecordDesc, int[] fieldPermutation, IIndexDataflowHelperFactory idfh,
            IIndexDataflowHelperFactory pkidfh, IModificationOperationCallbackFactory modificationCallbackFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, int numKeys, int[] filterFields,
            ITuplePartitionerFactory tuplePartitionerFactory, int[][] partitionsMap) {
        // this can be used by extensions to pick up their own operators
        return new LSMPrimaryInsertOperatorDescriptor(spec, inputRecordDesc, fieldPermutation, idfh, pkidfh,
                modificationCallbackFactory, searchCallbackFactory, numKeys, filterFields, tuplePartitionerFactory,
                partitionsMap);
    }

    protected LSMTreeInsertDeleteOperatorDescriptor createLSMTreeInsertDeleteOperatorDescriptor(
            IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc, int[] fieldPermutation, IndexOperation op,
            IIndexDataflowHelperFactory indexHelperFactory, ITupleFilterFactory tupleFilterFactory, boolean isPrimary,
            IModificationOperationCallbackFactory modCallbackFactory, ITuplePartitionerFactory tuplePartitionerFactory,
            int[][] partitionsMap) {
        return new LSMTreeInsertDeleteOperatorDescriptor(spec, outRecDesc, fieldPermutation, op, indexHelperFactory,
                tupleFilterFactory, isPrimary, modCallbackFactory, tuplePartitionerFactory, partitionsMap);
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexModificationRuntime(IndexOperation indexOp,
            IDataSourceIndex<String, DataSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            ILogicalExpression filterExpr, ILogicalExpression prevFilterExpr, RecordDescriptor inputRecordDesc,
            JobGenContext context, JobSpecification spec, boolean bulkload, LogicalVariable operationVar,
            List<LogicalVariable> prevSecondaryKeys, LogicalVariable prevAdditionalFilteringKey,
            List<List<AlgebricksPipeline>> secondaryKeysPipelines, IOperatorSchema pipelineTopSchema)
            throws AlgebricksException {
        String indexName = dataSourceIndex.getId();
        DataverseName dataverseName = dataSourceIndex.getDataSource().getId().getDataverseName();
        String database = dataSourceIndex.getDataSource().getId().getDatabaseName();
        String datasetName = dataSourceIndex.getDataSource().getId().getDatasourceName();

        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, database, dataverseName, datasetName);
        Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDatabaseName(),
                dataset.getDataverseName(), dataset.getDatasetName(), indexName);

        ArrayList<LogicalVariable> prevAdditionalFilteringKeys = null;
        if (indexOp == IndexOperation.UPSERT && prevAdditionalFilteringKey != null) {
            prevAdditionalFilteringKeys = new ArrayList<>();
            prevAdditionalFilteringKeys.add(prevAdditionalFilteringKey);
        }

        // If we have a pipeline, then we need to pass the schema of the pipeline to the filter factory.
        AsterixTupleFilterFactory filterFactory;
        AsterixTupleFilterFactory prevFilterFactory;
        if (pipelineTopSchema != null) {
            IOperatorSchema[] schemasForFilterFactory = new IOperatorSchema[inputSchemas.length + 1];
            System.arraycopy(inputSchemas, 0, schemasForFilterFactory, 0, inputSchemas.length);
            schemasForFilterFactory[inputSchemas.length] = pipelineTopSchema;
            filterFactory = createTupleFilterFactory(schemasForFilterFactory, typeEnv, filterExpr, context);
            prevFilterFactory = createTupleFilterFactory(schemasForFilterFactory, typeEnv, prevFilterExpr, context);
        } else {
            filterFactory = createTupleFilterFactory(inputSchemas, typeEnv, filterExpr, context);
            prevFilterFactory = createTupleFilterFactory(inputSchemas, typeEnv, prevFilterExpr, context);
        }

        switch (secondaryIndex.getIndexType()) {
            case BTREE:
                return getBTreeModificationRuntime(database, dataverseName, datasetName, indexName, propagatedSchema,
                        primaryKeys, secondaryKeys, additionalNonKeyFields, filterFactory, prevFilterFactory,
                        inputRecordDesc, context, spec, indexOp, bulkload, operationVar, prevSecondaryKeys,
                        prevAdditionalFilteringKeys);
            case ARRAY:
                if (bulkload) {
                    // In the case of bulk-load, we do not handle any nested plans. We perform the exact same behavior
                    // as a normal B-Tree bulk load.
                    return getBTreeModificationRuntime(database, dataverseName, datasetName, indexName,
                            propagatedSchema, primaryKeys, secondaryKeys, additionalNonKeyFields, filterFactory,
                            prevFilterFactory, inputRecordDesc, context, spec, indexOp, bulkload, operationVar,
                            prevSecondaryKeys, prevAdditionalFilteringKeys);
                } else {
                    return getArrayIndexModificationRuntime(database, dataverseName, datasetName, indexName,
                            propagatedSchema, primaryKeys, additionalNonKeyFields, inputRecordDesc, spec, indexOp,
                            operationVar, secondaryKeysPipelines);
                }
            case RTREE:
                return getRTreeModificationRuntime(database, dataverseName, datasetName, indexName, propagatedSchema,
                        primaryKeys, secondaryKeys, additionalNonKeyFields, filterFactory, prevFilterFactory,
                        inputRecordDesc, context, spec, indexOp, bulkload, operationVar, prevSecondaryKeys,
                        prevAdditionalFilteringKeys);
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
                return getInvertedIndexModificationRuntime(database, dataverseName, datasetName, indexName,
                        propagatedSchema, primaryKeys, secondaryKeys, additionalNonKeyFields, filterFactory,
                        prevFilterFactory, inputRecordDesc, context, spec, indexOp, secondaryIndex.getIndexType(),
                        bulkload, operationVar, prevSecondaryKeys, prevAdditionalFilteringKeys);
            default:
                throw new AlgebricksException(
                        indexOp.name() + " not implemented for index type: " + secondaryIndex.getIndexType());
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getBTreeModificationRuntime(String database,
            DataverseName dataverseName, String datasetName, String indexName, IOperatorSchema propagatedSchema,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalNonKeyFields, AsterixTupleFilterFactory filterFactory,
            AsterixTupleFilterFactory prevFilterFactory, RecordDescriptor inputRecordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp, boolean bulkload, LogicalVariable operationVar,
            List<LogicalVariable> prevSecondaryKeys, List<LogicalVariable> prevAdditionalFilteringKeys)
            throws AlgebricksException {
        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, database, dataverseName, datasetName);
        int numKeys = primaryKeys.size() + secondaryKeys.size();
        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;

        // generate field permutations
        int[] fieldPermutation = new int[numKeys + numFilterFields];
        int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
        int[] pkFields = new int[primaryKeys.size()];
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
            pkFields[j] = idx;
            modificationCallbackPrimaryKeyFields[j] = i;
            i++;
            j++;
        }

        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalNonKeyFields.get(0));
            fieldPermutation[numKeys] = idx;
        }

        int[] prevFieldPermutation = null;
        if (indexOp == IndexOperation.UPSERT) {
            // generate field permutations for prev record
            prevFieldPermutation = new int[numKeys + numFilterFields];
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
        }
        try {
            // Index parameters.
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDatabaseName(),
                    dataset.getDataverseName(), dataset.getDatasetName(), indexName);
            PartitioningProperties partitioningProperties =
                    getPartitioningProperties(dataset, secondaryIndex.getIndexName());
            // prepare callback
            IModificationOperationCallbackFactory modificationCallbackFactory = dataset.getModificationCallbackFactory(
                    storageComponentProvider, secondaryIndex, indexOp, modificationCallbackPrimaryKeyFields);
            IIndexDataflowHelperFactory idfh = new IndexDataflowHelperFactory(
                    storageComponentProvider.getStorageManager(), partitioningProperties.getSplitsProvider());
            IBinaryHashFunctionFactory[] pkHashFunFactories = dataset.getPrimaryHashFunctionFactories(this);
            ITuplePartitionerFactory partitionerFactory = new FieldHashPartitionerFactory(pkFields, pkHashFunFactories,
                    partitioningProperties.getNumberOfPartitions());

            IOperatorDescriptor op;
            if (bulkload) {
                long numElementsHint = getCardinalityPerPartitionHint(dataset);
                op = new LSMIndexBulkLoadOperatorDescriptor(spec, inputRecordDesc, fieldPermutation,
                        StorageConstants.DEFAULT_TREE_FILL_FACTOR, false, numElementsHint, false, idfh, null,
                        BulkLoadUsage.LOAD, dataset.getDatasetId(), filterFactory, partitionerFactory,
                        partitioningProperties.getComputeStorageMap());
            } else if (indexOp == IndexOperation.UPSERT) {
                int operationFieldIndex = propagatedSchema.findVariable(operationVar);
                op = new LSMSecondaryUpsertOperatorDescriptor(spec, inputRecordDesc, fieldPermutation, idfh,
                        filterFactory, prevFilterFactory, modificationCallbackFactory, operationFieldIndex,
                        BinaryIntegerInspector.FACTORY, prevFieldPermutation, partitionerFactory,
                        partitioningProperties.getComputeStorageMap());
            } else {
                op = new LSMTreeInsertDeleteOperatorDescriptor(spec, inputRecordDesc, fieldPermutation, indexOp, idfh,
                        filterFactory, false, modificationCallbackFactory, partitionerFactory,
                        partitioningProperties.getComputeStorageMap());
            }
            return new Pair<>(op, partitioningProperties.getConstraints());
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getArrayIndexModificationRuntime(String database,
            DataverseName dataverseName, String datasetName, String indexName, IOperatorSchema propagatedSchema,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> additionalNonKeyFields,
            RecordDescriptor inputRecordDesc, JobSpecification spec, IndexOperation indexOp,
            LogicalVariable operationVar, List<List<AlgebricksPipeline>> secondaryKeysPipelines)
            throws AlgebricksException {

        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, database, dataverseName, datasetName);
        int numPrimaryKeys = primaryKeys.size();
        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;

        // Generate field permutations (this only includes primary keys and filter fields).
        int[] fieldPermutation = new int[numPrimaryKeys + numFilterFields];
        int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
        int[] pkFields = new int[primaryKeys.size()];
        int i = 0;
        int j = 0;
        for (LogicalVariable varKey : primaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            pkFields[j] = idx;
            modificationCallbackPrimaryKeyFields[j] = i;
            i++;
            j++;
        }
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalNonKeyFields.get(0));
            fieldPermutation[numPrimaryKeys] = idx;
        }

        try {
            // Index parameters.
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDatabaseName(),
                    dataset.getDataverseName(), dataset.getDatasetName(), indexName);

            PartitioningProperties partitioningProperties =
                    getPartitioningProperties(dataset, secondaryIndex.getIndexName());
            // Prepare callback.
            IModificationOperationCallbackFactory modificationCallbackFactory = dataset.getModificationCallbackFactory(
                    storageComponentProvider, secondaryIndex, indexOp, modificationCallbackPrimaryKeyFields);
            IIndexDataflowHelperFactory idfh = new IndexDataflowHelperFactory(
                    storageComponentProvider.getStorageManager(), partitioningProperties.getSplitsProvider());
            IBinaryHashFunctionFactory[] pkHashFunFactories = dataset.getPrimaryHashFunctionFactories(this);
            ITuplePartitionerFactory tuplePartitionerFactory = new FieldHashPartitionerFactory(pkFields,
                    pkHashFunFactories, partitioningProperties.getNumberOfPartitions());

            IOperatorDescriptor op;
            if (indexOp == IndexOperation.UPSERT) {
                int operationFieldIndex = propagatedSchema.findVariable(operationVar);
                op = new LSMSecondaryUpsertWithNestedPlanOperatorDescriptor(spec, inputRecordDesc, fieldPermutation,
                        idfh, modificationCallbackFactory, operationFieldIndex, BinaryIntegerInspector.FACTORY,
                        secondaryKeysPipelines.get(0), secondaryKeysPipelines.get(1), tuplePartitionerFactory,
                        partitioningProperties.getComputeStorageMap());
            } else {
                op = new LSMSecondaryInsertDeleteWithNestedPlanOperatorDescriptor(spec, inputRecordDesc,
                        fieldPermutation, indexOp, idfh, modificationCallbackFactory, secondaryKeysPipelines.get(0),
                        tuplePartitionerFactory, partitioningProperties.getComputeStorageMap());
            }
            return new Pair<>(op, partitioningProperties.getConstraints());
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getRTreeModificationRuntime(String database,
            DataverseName dataverseName, String datasetName, String indexName, IOperatorSchema propagatedSchema,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalNonKeyFields, AsterixTupleFilterFactory filterFactory,
            AsterixTupleFilterFactory prevFilterFactory, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp, boolean bulkload, LogicalVariable operationVar,
            List<LogicalVariable> prevSecondaryKeys, List<LogicalVariable> prevAdditionalFilteringKeys)
            throws AlgebricksException {
        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, database, dataverseName, datasetName);
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getItemTypeDatabaseName(),
                dataset.getItemTypeDataverseName(), itemTypeName).getDatatype();
        validateRecordType(itemType);
        ARecordType recType = (ARecordType) itemType;
        Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDatabaseName(),
                dataset.getDataverseName(), dataset.getDatasetName(), indexName);
        Index.ValueIndexDetails secondaryIndexDetails = (Index.ValueIndexDetails) secondaryIndex.getIndexDetails();
        List<List<String>> secondaryKeyExprs = secondaryIndexDetails.getKeyFieldNames();
        List<IAType> secondaryKeyTypes = secondaryIndexDetails.getKeyFieldTypes();
        Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryIndex, secondaryKeyTypes.get(0),
                secondaryKeyExprs.get(0), recType);
        IAType spatialType = keyPairType.first;
        int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numSecondaryKeys = dimension * 2;
        int numPrimaryKeys = primaryKeys.size();
        int numKeys = numSecondaryKeys + numPrimaryKeys;

        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;
        int[] fieldPermutation = new int[numKeys + numFilterFields];
        int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
        int[] pkFields = new int[primaryKeys.size()];
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
            pkFields[j] = idx;
            modificationCallbackPrimaryKeyFields[j] = i;
            i++;
            j++;
        }

        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalNonKeyFields.get(0));
            fieldPermutation[numKeys] = idx;
        }

        int[] prevFieldPermutation = null;
        if (indexOp == IndexOperation.UPSERT) {
            // Get field permutation for previous value
            prevFieldPermutation = new int[numKeys + numFilterFields];
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
        }

        PartitioningProperties partitioningProperties =
                getPartitioningProperties(dataset, secondaryIndex.getIndexName());

        // prepare callback
        IModificationOperationCallbackFactory modificationCallbackFactory = dataset.getModificationCallbackFactory(
                storageComponentProvider, secondaryIndex, indexOp, modificationCallbackPrimaryKeyFields);
        IIndexDataflowHelperFactory indexDataflowHelperFactory = new IndexDataflowHelperFactory(
                storageComponentProvider.getStorageManager(), partitioningProperties.getSplitsProvider());
        IBinaryHashFunctionFactory[] pkHashFunFactories = dataset.getPrimaryHashFunctionFactories(this);
        ITuplePartitionerFactory partitionerFactory = new FieldHashPartitionerFactory(pkFields, pkHashFunFactories,
                partitioningProperties.getNumberOfPartitions());

        IOperatorDescriptor op;
        if (bulkload) {
            long numElementsHint = getCardinalityPerPartitionHint(dataset);
            op = new LSMIndexBulkLoadOperatorDescriptor(spec, recordDesc, fieldPermutation,
                    StorageConstants.DEFAULT_TREE_FILL_FACTOR, false, numElementsHint, false,
                    indexDataflowHelperFactory, null, BulkLoadUsage.LOAD, dataset.getDatasetId(), filterFactory,
                    partitionerFactory, partitioningProperties.getComputeStorageMap());
        } else if (indexOp == IndexOperation.UPSERT) {
            int operationFieldIndex = propagatedSchema.findVariable(operationVar);
            op = new LSMSecondaryUpsertOperatorDescriptor(spec, recordDesc, fieldPermutation,
                    indexDataflowHelperFactory, filterFactory, prevFilterFactory, modificationCallbackFactory,
                    operationFieldIndex, BinaryIntegerInspector.FACTORY, prevFieldPermutation, partitionerFactory,
                    partitioningProperties.getComputeStorageMap());
        } else {
            op = new LSMTreeInsertDeleteOperatorDescriptor(spec, recordDesc, fieldPermutation, indexOp,
                    indexDataflowHelperFactory, filterFactory, false, modificationCallbackFactory, partitionerFactory,
                    partitioningProperties.getComputeStorageMap());
        }
        return new Pair<>(op, partitioningProperties.getConstraints());
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInvertedIndexModificationRuntime(
            String database, DataverseName dataverseName, String datasetName, String indexName,
            IOperatorSchema propagatedSchema, List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalNonKeyFields, AsterixTupleFilterFactory filterFactory,
            AsterixTupleFilterFactory prevFilterFactory, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp, IndexType indexType, boolean bulkload,
            LogicalVariable operationVar, List<LogicalVariable> prevSecondaryKeys,
            List<LogicalVariable> prevAdditionalFilteringKeys) throws AlgebricksException {
        // Check the index is length-partitioned or not.
        boolean isPartitioned;
        isPartitioned = indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX;

        // Sanity checks.
        if (primaryKeys.size() > 1) {
            throw new AlgebricksException(
                    "Cannot create inverted index on " + dataset(PLURAL) + " with composite primary key.");
        }
        // The size of secondaryKeys can be two if it receives input from its
        // TokenizeOperator- [token, number of token]
        if ((secondaryKeys.size() > 1 && !isPartitioned) || (secondaryKeys.size() > 2 && isPartitioned)) {
            throw new AlgebricksException("Cannot create composite inverted index on multiple fields.");
        }
        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, database, dataverseName, datasetName);
        // For tokenization, sorting and loading.
        // One token (+ optional partitioning field) + primary keys: [token,
        // number of token, PK]
        int numKeys = primaryKeys.size() + secondaryKeys.size();
        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;

        // generate field permutations
        int[] fieldPermutation = new int[numKeys + numFilterFields];
        int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
        int[] pkFields = new int[primaryKeys.size()];
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
            pkFields[j] = idx;
            modificationCallbackPrimaryKeyFields[j] = i;
            i++;
            j++;
        }
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalNonKeyFields.get(0));
            fieldPermutation[numKeys] = idx;
        }

        int[] prevFieldPermutation = null;
        if (indexOp == IndexOperation.UPSERT) {
            // Find permutations for prev value
            prevFieldPermutation = new int[numKeys + numFilterFields];
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
        }
        try {
            // Index parameters.
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDatabaseName(),
                    dataset.getDataverseName(), dataset.getDatasetName(), indexName);

            PartitioningProperties partitioningProperties =
                    getPartitioningProperties(dataset, secondaryIndex.getIndexName());

            // prepare callback
            IModificationOperationCallbackFactory modificationCallbackFactory = dataset.getModificationCallbackFactory(
                    storageComponentProvider, secondaryIndex, indexOp, modificationCallbackPrimaryKeyFields);
            IIndexDataflowHelperFactory indexDataFlowFactory = new IndexDataflowHelperFactory(
                    storageComponentProvider.getStorageManager(), partitioningProperties.getSplitsProvider());
            IBinaryHashFunctionFactory[] pkHashFunFactories = dataset.getPrimaryHashFunctionFactories(this);
            ITuplePartitionerFactory partitionerFactory = new FieldHashPartitionerFactory(pkFields, pkHashFunFactories,
                    partitioningProperties.getNumberOfPartitions());

            IOperatorDescriptor op;
            if (bulkload) {
                long numElementsHint = getCardinalityPerPartitionHint(dataset);
                op = new LSMIndexBulkLoadOperatorDescriptor(spec, recordDesc, fieldPermutation,
                        StorageConstants.DEFAULT_TREE_FILL_FACTOR, false, numElementsHint, false, indexDataFlowFactory,
                        null, BulkLoadUsage.LOAD, dataset.getDatasetId(), filterFactory, partitionerFactory,
                        partitioningProperties.getComputeStorageMap());
            } else if (indexOp == IndexOperation.UPSERT) {
                int upsertOperationFieldIndex = propagatedSchema.findVariable(operationVar);
                op = new LSMSecondaryUpsertOperatorDescriptor(spec, recordDesc, fieldPermutation, indexDataFlowFactory,
                        filterFactory, prevFilterFactory, modificationCallbackFactory, upsertOperationFieldIndex,
                        BinaryIntegerInspector.FACTORY, prevFieldPermutation, partitionerFactory,
                        partitioningProperties.getComputeStorageMap());
            } else {
                op = new LSMTreeInsertDeleteOperatorDescriptor(spec, recordDesc, fieldPermutation, indexOp,
                        indexDataFlowFactory, filterFactory, false, modificationCallbackFactory, partitionerFactory,
                        partitioningProperties.getComputeStorageMap());
            }
            return new Pair<>(op, partitioningProperties.getConstraints());
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    // Get a Tokenizer for the bulk-loading data into a n-gram or keyword index.
    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getBinaryTokenizerRuntime(String database,
            DataverseName dataverseName, String datasetName, String indexName, IOperatorSchema inputSchema,
            IOperatorSchema propagatedSchema, List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            RecordDescriptor recordDesc, JobSpecification spec, IndexType indexType) throws AlgebricksException {

        // Sanity checks.
        if (primaryKeys.size() > 1) {
            throw new AlgebricksException("Cannot tokenize composite primary key.");
        }
        if (secondaryKeys.size() > 1) {
            throw new AlgebricksException("Cannot tokenize composite secondary key fields.");
        }

        boolean isPartitioned;
        isPartitioned = indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX;

        // Number of Keys that needs to be propagated
        int numKeys = inputSchema.getSize();

        // Get the rest of Logical Variables that are not (PK or SK) and each
        // variable's positions.
        // These variables will be propagated through TokenizeOperator.
        List<LogicalVariable> otherKeys = new ArrayList<>();
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

        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, database, dataverseName, datasetName);
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType;
        try {
            itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getItemTypeDatabaseName(),
                    dataset.getItemTypeDataverseName(), itemTypeName).getDatatype();

            if (itemType.getTypeTag() != ATypeTag.OBJECT) {
                throw new AlgebricksException("Only record types can be tokenized.");
            }

            ARecordType recType = (ARecordType) itemType;

            // Index parameters.
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDatabaseName(),
                    dataset.getDataverseName(), dataset.getDatasetName(), indexName);
            Index.TextIndexDetails secondaryIndexDetails = (Index.TextIndexDetails) secondaryIndex.getIndexDetails();

            List<List<String>> secondaryKeyExprs = secondaryIndexDetails.getKeyFieldNames();
            List<IAType> secondaryKeyTypeEntries = secondaryIndexDetails.getKeyFieldTypes();

            int numTokenFields = (!isPartitioned) ? secondaryKeys.size() : secondaryKeys.size() + 1;
            ITypeTraits[] tokenTypeTraits = new ITypeTraits[numTokenFields];
            ITypeTraits[] invListsTypeTraits = new ITypeTraits[primaryKeys.size()];

            // Find the key type of the secondary key. If it's a derived type,
            // return the derived type.
            // e.g. UNORDERED LIST -> return UNORDERED LIST type
            IAType secondaryKeyType;
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryIndex,
                    secondaryKeyTypeEntries.get(0), secondaryKeyExprs.get(0), recType);
            secondaryKeyType = keyPairType.first;
            List<List<String>> partitioningKeys = dataset.getPrimaryKeys();
            i = 0;
            for (List<String> partitioningKey : partitioningKeys) {
                IAType keyType = recType.getSubFieldType(partitioningKey);
                invListsTypeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }

            tokenTypeTraits[0] = NonTaggedFormatUtil.getTokenTypeTrait(secondaryKeyType);
            if (isPartitioned) {
                // The partitioning field is hardcoded to be a short *without*
                // an Asterix type tag.
                tokenTypeTraits[1] = ShortPointable.TYPE_TRAITS;
            }

            IBinaryTokenizerFactory tokenizerFactory = NonTaggedFormatUtil.getBinaryTokenizerFactory(
                    secondaryKeyType.getTypeTag(), indexType, secondaryIndexDetails.getGramLength());
            IFullTextConfigEvaluatorFactory fullTextConfigEvaluatorFactory =
                    FullTextUtil.fetchFilterAndCreateConfigEvaluator(this, secondaryIndex.getDatabaseName(),
                            secondaryIndex.getDataverseName(), secondaryIndexDetails.getFullTextConfigName());

            PartitioningProperties partitioningProperties =
                    getPartitioningProperties(dataset, secondaryIndex.getIndexName());

            // Generate Output Record format
            ISerializerDeserializer<?>[] tokenKeyPairFields = new ISerializerDeserializer[numTokenKeyPairFields];
            ITypeTraits[] tokenKeyPairTypeTraits = new ITypeTraits[numTokenKeyPairFields];
            ISerializerDeserializerProvider serdeProvider = getDataFormat().getSerdeProvider();

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

            //TODO(partitioning) check
            tokenizerOp = new BinaryTokenizerOperatorDescriptor(spec, tokenKeyPairRecDesc, tokenizerFactory,
                    fullTextConfigEvaluatorFactory, docField, keyFields, isPartitioned, true, false,
                    MissingWriterFactory.INSTANCE);
            return new Pair<>(tokenizerOp, partitioningProperties.getConstraints());
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public AsterixTupleFilterFactory createTupleFilterFactory(IOperatorSchema[] inputSchemas,
            IVariableTypeEnvironment typeEnv, ILogicalExpression filterExpr, JobGenContext context)
            throws AlgebricksException {
        // No filtering condition.
        if (filterExpr == null) {
            return null;
        }
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        IScalarEvaluatorFactory filterEvalFactory =
                expressionRuntimeProvider.createEvaluatorFactory(filterExpr, typeEnv, inputSchemas, context);
        return new AsterixTupleFilterFactory(filterEvalFactory, context.getBinaryBooleanInspectorFactory());
    }

    private void validateRecordType(IAType itemType) throws AlgebricksException {
        if (itemType.getTypeTag() != ATypeTag.OBJECT) {
            throw new AlgebricksException("Only record types can be indexed.");
        }
    }

    public IStorageComponentProvider getStorageComponentProvider() {
        return storageComponentProvider;
    }

    public Namespace resolve(List<String> multiIdent) throws AsterixException {
        return namespaceResolver.resolve(multiIdent);
    }

    public PartitioningProperties getPartitioningProperties(Index idx) throws AlgebricksException {
        Dataset ds = findDataset(idx.getDatabaseName(), idx.getDataverseName(), idx.getDatasetName());
        return getPartitioningProperties(ds, idx.getIndexName());
    }

    public PartitioningProperties getPartitioningProperties(Dataset ds) throws AlgebricksException {
        return getPartitioningProperties(ds, ds.getDatasetName());
    }

    public PartitioningProperties getPartitioningProperties(Dataset ds, String indexName) throws AlgebricksException {
        return dataPartitioningProvider.getPartitioningProperties(mdTxnCtx, ds, indexName);
    }

    public PartitioningProperties getPartitioningProperties(Feed feed) throws AlgebricksException {
        return dataPartitioningProvider.getPartitioningProperties(feed);
    }

    public List<Index> getSecondaryIndexes(Dataset ds) throws AlgebricksException {
        return getDatasetIndexes(ds.getDatabaseName(), ds.getDataverseName(), ds.getDatasetName()).stream()
                .filter(idx -> idx.isSecondaryIndex() && !idx.isSampleIndex()).collect(Collectors.toList());
    }

    public LockList getLocks() {
        return locks;
    }

    public ICcApplicationContext getApplicationContext() {
        return appCtx;
    }

    public ITxnIdFactory getTxnIdFactory() {
        return appCtx.getTxnIdFactory();
    }

    public ICompressionManager getCompressionManager() {
        return appCtx.getCompressionManager();
    }

    public void validateNamespaceName(Namespace namespace, SourceLocation srcLoc) throws AlgebricksException {
        validateDatabaseName(namespace.getDatabaseName(), srcLoc);
        validateDataverseName(namespace.getDataverseName(), srcLoc);
    }

    public void validateDatabaseName(String databaseName, SourceLocation srcLoc) throws AlgebricksException {
        validateDatabaseObjectNameImpl(databaseName, srcLoc);
        validateChars(databaseName, srcLoc);
    }

    public void validateDataverseName(DataverseName dataverseName, SourceLocation sourceLoc)
            throws AlgebricksException {
        List<String> dvParts = dataverseName.getParts();
        validatePartsLimit(dataverseName, dvParts, sourceLoc);
        int totalLengthUTF8 = 0;
        for (String dvNamePart : dvParts) {
            validateDatabaseObjectNameImpl(dvNamePart, sourceLoc);
            if (totalLengthUTF8 == 0 && StoragePathUtil.DATAVERSE_CONTINUATION_MARKER == dvNamePart.codePointAt(0)) {
                throw new AsterixException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, sourceLoc, dvNamePart);
            }
            if (namespaceResolver.isUsingDatabase()) {
                validateChars(dvNamePart, sourceLoc);
            }
            totalLengthUTF8 += dvNamePart.getBytes(StandardCharsets.UTF_8).length;
        }
        if (totalLengthUTF8 > MetadataConstants.DATAVERSE_NAME_TOTAL_LENGTH_LIMIT_UTF8) {
            throw new AsterixException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, sourceLoc, dataverseName.toString());
        }
    }

    public IExternalFilterEvaluatorFactory createExternalFilterEvaluatorFactory(JobGenContext context,
            IVariableTypeEnvironment typeEnv, IProjectionFiltrationInfo projectionFiltrationInfo,
            Map<String, String> properties) throws AlgebricksException {
        return IndexUtil.createExternalFilterEvaluatorFactory(context, typeEnv, projectionFiltrationInfo, properties);
    }

    public void validateDatabaseObjectName(Namespace namespace, String objectName, SourceLocation sourceLoc)
            throws AlgebricksException {
        if (namespace != null) {
            validateNamespaceName(namespace, sourceLoc);
        }
        validateDatabaseObjectNameImpl(objectName, sourceLoc);
        if (namespaceResolver.isUsingDatabase()) {
            validateChars(objectName, sourceLoc);
        }
    }

    private void validateDatabaseObjectNameImpl(String name, SourceLocation sourceLoc) throws AlgebricksException {
        if (name == null || name.isEmpty()) {
            throw new AsterixException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, sourceLoc, "");
        }
        if (Character.isWhitespace(name.codePointAt(0)) || METADATA_OBJECT_NAME_INVALID_CHARS.matcher(name).find()) {
            throw new AsterixException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, sourceLoc, name);
        }
        int lengthUTF8 = name.getBytes(StandardCharsets.UTF_8).length;
        if (lengthUTF8 > MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8) {
            throw new AsterixException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, sourceLoc, name);
        }
    }

    private void validatePartsLimit(DataverseName dvName, List<String> parts, SourceLocation srcLoc)
            throws AsterixException {
        if (namespaceResolver.isUsingDatabase() && parts.size() != MetadataConstants.DB_SCOPE_PARTS_COUNT) {
            throw new AsterixException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, srcLoc, dvName);
        }
    }

    private static void validateChars(String name, SourceLocation srcLoc) throws AsterixException {
        for (int off = 0, len = name.length(); off < len;) {
            int codePointChar = name.codePointAt(off);
            if (!Character.isLetterOrDigit(codePointChar)) {
                if (codePointChar != '_' && codePointChar != '-') {
                    throw new AsterixException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, srcLoc, name);
                }
            }
            off += Character.charCount(codePointChar);
        }
    }
}
