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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.dataflow.LSMTreeInsertDeleteOperatorDescriptor;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.metadata.LockList;
import org.apache.asterix.common.storage.ICompressionManager;
import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.dataflow.data.nontagged.MissingWriterFactory;
import org.apache.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import org.apache.asterix.external.adapter.factory.LookupAdapterFactory;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.api.IDataSourceAdapter;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.external.operators.ExternalBTreeSearchOperatorDescriptor;
import org.apache.asterix.external.operators.ExternalLookupOperatorDescriptor;
import org.apache.asterix.external.operators.ExternalRTreeSearchOperatorDescriptor;
import org.apache.asterix.external.operators.ExternalScanOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.provider.AdapterFactoryProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.formats.nontagged.BinaryBooleanInspector;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.LinearizeComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.dataset.hints.DatasetHints.DatasetCardinalityHint;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;
import org.apache.asterix.metadata.lock.ExternalDatasetsRegistry;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.metadata.utils.SplitsAndConstraintsUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionExtensionManager;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.runtime.base.AsterixTupleFilterFactory;
import org.apache.asterix.runtime.formats.FormatUtils;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor.BulkLoadUsage;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMSecondaryUpsertOperatorDescriptor;
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
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
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
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.rtree.dataflow.RTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.common.IStorageManager;

public class MetadataProvider implements IMetadataProvider<DataSourceId, String> {

    private final ICcApplicationContext appCtx;
    private final IStorageComponentProvider storageComponentProvider;
    private final StorageProperties storageProperties;
    private final IFunctionManager functionManager;
    private final LockList locks;
    private final Map<String, Object> config;
    private final Set<Dataset> txnAccessedDatasets;

    private Dataverse defaultDataverse;
    private MetadataTransactionContext mdTxnCtx;
    private boolean isWriteTransaction;
    private IAWriterFactory writerFactory;
    private FileSplit outputFile;
    private boolean asyncResults;
    private long maxResultReads;
    private ResultSetId resultSetId;
    private IResultSerializerFactoryProvider resultSerializerFactoryProvider;
    private TxnId txnId;
    private Map<String, Integer> externalDataLocks;
    private boolean blockingOperatorDisabled = false;

    public MetadataProvider(ICcApplicationContext appCtx, Dataverse defaultDataverse) {
        this.appCtx = appCtx;
        setDefaultDataverse(defaultDataverse);
        this.storageComponentProvider = appCtx.getStorageComponentProvider();
        storageProperties = appCtx.getStorageProperties();
        functionManager = ((IFunctionExtensionManager) appCtx.getExtensionManager()).getFunctionManager();
        locks = new LockList();
        config = new HashMap<>();
        txnAccessedDatasets = new HashSet<>();
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

    public void setDefaultDataverse(Dataverse defaultDataverse) {
        this.defaultDataverse = defaultDataverse == null ? MetadataBuiltinEntities.DEFAULT_DATAVERSE : defaultDataverse;
    }

    public Dataverse getDefaultDataverse() {
        return defaultDataverse;
    }

    public String getDefaultDataverseName() {
        return defaultDataverse.getDataverseName();
    }

    public void setWriteTransaction(boolean writeTransaction) {
        this.isWriteTransaction = writeTransaction;
    }

    public void setWriterFactory(IAWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    public void setMetadataTxnContext(MetadataTransactionContext mdTxnCtx) {
        this.mdTxnCtx = mdTxnCtx;
        txnAccessedDatasets.clear();
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

    public void setResultSerializerFactoryProvider(IResultSerializerFactoryProvider rafp) {
        this.resultSerializerFactoryProvider = rafp;
    }

    public IResultSerializerFactoryProvider getResultSerializerFactoryProvider() {
        return resultSerializerFactoryProvider;
    }

    public boolean isWriteTransaction() {
        // The transaction writes persistent datasets.
        return isWriteTransaction;
    }

    public IFunctionManager getFunctionManager() {
        return functionManager;
    }

    public IDataFormat getDataFormat() {
        return FormatUtils.getDefaultFormat();
    }

    public StorageProperties getStorageProperties() {
        return storageProperties;
    }

    public Map<String, Integer> getExternalDataLocks() {
        return externalDataLocks;
    }

    public void setExternalDataLocks(Map<String, Integer> locks) {
        this.externalDataLocks = locks;
    }

    /**
     * Retrieve the Output RecordType, as defined by "set output-record-type".
     */
    public ARecordType findOutputRecordType() throws AlgebricksException {
        return MetadataManagerUtil.findOutputRecordType(mdTxnCtx, getDefaultDataverseName(),
                getProperty("output-record-type"));
    }

    public Dataset findDataset(String dataverse, String dataset) throws AlgebricksException {
        String dv =
                dataverse == null ? (defaultDataverse == null ? null : defaultDataverse.getDataverseName()) : dataverse;
        if (dv == null) {
            return null;
        }
        String fqName = dv + '.' + dataset;
        appCtx.getMetadataLockManager().acquireDataverseReadLock(locks, dv);
        appCtx.getMetadataLockManager().acquireDatasetReadLock(locks, fqName);
        return MetadataManagerUtil.findDataset(mdTxnCtx, dv, dataset);
    }

    public INodeDomain findNodeDomain(String nodeGroupName) throws AlgebricksException {
        return MetadataManagerUtil.findNodeDomain(appCtx.getClusterStateManager(), mdTxnCtx, nodeGroupName);
    }

    public List<String> findNodes(String nodeGroupName) throws AlgebricksException {
        return MetadataManagerUtil.findNodes(mdTxnCtx, nodeGroupName);
    }

    public IAType findType(String dataverse, String typeName) throws AlgebricksException {
        return MetadataManagerUtil.findType(mdTxnCtx, dataverse, typeName);
    }

    public IAType findType(Dataset dataset) throws AlgebricksException {
        return findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
    }

    public IAType findMetaType(Dataset dataset) throws AlgebricksException {
        return findType(dataset.getMetaItemTypeDataverseName(), dataset.getMetaItemTypeName());
    }

    public Feed findFeed(String dataverse, String feedName) throws AlgebricksException {
        return MetadataManagerUtil.findFeed(mdTxnCtx, dataverse, feedName);
    }

    public FeedConnection findFeedConnection(String dataverseName, String feedName, String datasetName)
            throws AlgebricksException {
        return MetadataManagerUtil.findFeedConnection(mdTxnCtx, dataverseName, feedName, datasetName);
    }

    public FeedPolicyEntity findFeedPolicy(String dataverse, String policyName) throws AlgebricksException {
        return MetadataManagerUtil.findFeedPolicy(mdTxnCtx, dataverse, policyName);
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
        Index secondaryIndex = getIndex(dataset.getDataverseName(), dataset.getDatasetName(), indexId);
        return (secondaryIndex != null)
                ? new DataSourceIndex(secondaryIndex, dataset.getDataverseName(), dataset.getDatasetName(), this)
                : null;
    }

    public Index getIndex(String dataverseName, String datasetName, String indexName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, indexName);
    }

    public List<Index> getDatasetIndexes(String dataverseName, String datasetName) throws AlgebricksException {
        return MetadataManagerUtil.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
    }

    @Override
    public IFunctionInfo lookupFunction(FunctionIdentifier fid) {
        return BuiltinFunctions.lookupFunction(fid);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(
            IDataSource<DataSourceId> dataSource, List<LogicalVariable> scanVariables,
            List<LogicalVariable> projectVariables, boolean projectPushed, List<LogicalVariable> minFilterVars,
            List<LogicalVariable> maxFilterVars, ITupleFilterFactory tupleFilterFactory, long outputLimit,
            IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec,
            Object implConfig) throws AlgebricksException {
        return ((DataSource) dataSource).buildDatasourceScanRuntime(this, dataSource, scanVariables, projectVariables,
                projectPushed, minFilterVars, maxFilterVars, tupleFilterFactory, outputLimit, opSchema, typeEnv,
                context, jobSpec, implConfig);
    }

    protected Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildLoadableDatasetScan(
            JobSpecification jobSpec, IAdapterFactory adapterFactory, RecordDescriptor rDesc)
            throws AlgebricksException {
        ExternalScanOperatorDescriptor dataScanner = new ExternalScanOperatorDescriptor(jobSpec, rDesc, adapterFactory);
        try {
            return new Pair<>(dataScanner, adapterFactory.getPartitionConstraint());
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    public Dataverse findDataverse(String dataverseName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
    }

    public Triple<IOperatorDescriptor, AlgebricksPartitionConstraint, IAdapterFactory> buildFeedIntakeRuntime(
            JobSpecification jobSpec, Feed feed, FeedPolicyAccessor policyAccessor) throws Exception {
        Triple<IAdapterFactory, RecordDescriptor, IDataSourceAdapter.AdapterType> factoryOutput;
        factoryOutput =
                FeedMetadataUtil.getFeedFactoryAndOutput(feed, policyAccessor, mdTxnCtx, getApplicationContext());
        ARecordType recordType =
                FeedMetadataUtil.getOutputType(feed, feed.getConfiguration().get(ExternalDataConstants.KEY_TYPE_NAME));
        IAdapterFactory adapterFactory = factoryOutput.first;
        FeedIntakeOperatorDescriptor feedIngestor = null;
        switch (factoryOutput.third) {
            case INTERNAL:
                feedIngestor = new FeedIntakeOperatorDescriptor(jobSpec, feed, adapterFactory, recordType,
                        policyAccessor, factoryOutput.second);
                break;
            case EXTERNAL:
                String libraryName = feed.getConfiguration().get(ExternalDataConstants.KEY_ADAPTER_NAME).trim()
                        .split(FeedConstants.NamingConstants.LIBRARY_NAME_SEPARATOR)[0];
                feedIngestor = new FeedIntakeOperatorDescriptor(jobSpec, feed, libraryName,
                        adapterFactory.getClass().getName(), recordType, policyAccessor, factoryOutput.second);
                break;
            default:
                break;
        }

        AlgebricksPartitionConstraint partitionConstraint = adapterFactory.getPartitionConstraint();
        return new Triple<>(feedIngestor, partitionConstraint, adapterFactory);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildBtreeRuntime(JobSpecification jobSpec,
            IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv, JobGenContext context, boolean retainInput,
            boolean retainMissing, Dataset dataset, String indexName, int[] lowKeyFields, int[] highKeyFields,
            boolean lowKeyInclusive, boolean highKeyInclusive, boolean propagateFilter, int[] minFilterFieldIndexes,
            int[] maxFilterFieldIndexes, ITupleFilterFactory tupleFilterFactory, long outputLimit,
            boolean isIndexOnlyPlan) throws AlgebricksException {
        boolean isSecondary = true;
        Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                dataset.getDatasetName(), dataset.getDatasetName());
        if (primaryIndex != null && (dataset.getDatasetType() != DatasetType.EXTERNAL)) {
            isSecondary = !indexName.equals(primaryIndex.getIndexName());
        }
        Index theIndex = isSecondary ? MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                dataset.getDatasetName(), indexName) : primaryIndex;
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc =
                getSplitProviderAndConstraints(dataset, theIndex.getIndexName());
        int[] primaryKeyFields = new int[numPrimaryKeys];
        for (int i = 0; i < numPrimaryKeys; i++) {
            primaryKeyFields[i] = i;
        }

        int[] primaryKeyFieldsInSecondaryIndex = null;
        byte[] successValueForIndexOnlyPlan = null;
        byte[] failValueForIndexOnlyPlan = null;
        boolean proceedIndexOnlyPlan = isIndexOnlyPlan && isSecondary;
        if (proceedIndexOnlyPlan) {
            int numSecondaryKeys = theIndex.getKeyFieldNames().size();
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
        IIndexDataflowHelperFactory indexHelperFactory = new IndexDataflowHelperFactory(storageManager, spPc.first);
        BTreeSearchOperatorDescriptor btreeSearchOp;

        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            btreeSearchOp = new BTreeSearchOperatorDescriptor(jobSpec, outputRecDesc, lowKeyFields, highKeyFields,
                    lowKeyInclusive, highKeyInclusive, indexHelperFactory, retainInput, retainMissing,
                    context.getMissingWriterFactory(), searchCallbackFactory, minFilterFieldIndexes,
                    maxFilterFieldIndexes, propagateFilter, tupleFilterFactory, outputLimit, proceedIndexOnlyPlan,
                    failValueForIndexOnlyPlan, successValueForIndexOnlyPlan);
        } else {
            btreeSearchOp = new ExternalBTreeSearchOperatorDescriptor(jobSpec, outputRecDesc, lowKeyFields,
                    highKeyFields, lowKeyInclusive, highKeyInclusive, indexHelperFactory, retainInput, retainMissing,
                    context.getMissingWriterFactory(), searchCallbackFactory, minFilterFieldIndexes,
                    maxFilterFieldIndexes, ExternalDatasetsRegistry.INSTANCE.getAndLockDatasetVersion(dataset, this));
        }
        return new Pair<>(btreeSearchOp, spPc.second);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildRtreeRuntime(JobSpecification jobSpec,
            List<LogicalVariable> outputVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            JobGenContext context, boolean retainInput, boolean retainMissing, Dataset dataset, String indexName,
            int[] keyFields, boolean propagateFilter, int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes,
            boolean isIndexOnlyPlan) throws AlgebricksException {
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                dataset.getDatasetName(), indexName);
        if (secondaryIndex == null) {
            throw new AlgebricksException(
                    "Code generation error: no index " + indexName + " for dataset " + dataset.getDatasetName());
        }
        RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc =
                getSplitProviderAndConstraints(dataset, secondaryIndex.getIndexName());
        int[] primaryKeyFields = new int[numPrimaryKeys];
        for (int i = 0; i < numPrimaryKeys; i++) {
            primaryKeyFields[i] = i;
        }

        int[] primaryKeyFieldsInSecondaryIndex = null;
        byte[] successValueForIndexOnlyPlan = null;
        byte[] failValueForIndexOnlyPlan = null;
        if (isIndexOnlyPlan) {
            ARecordType recType = (ARecordType) findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
            List<List<String>> secondaryKeyFields = secondaryIndex.getKeyFieldNames();
            List<IAType> secondaryKeyTypes = secondaryIndex.getKeyFieldTypes();
            Pair<IAType, Boolean> keyTypePair =
                    Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(0), secondaryKeyFields.get(0), recType);
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
        IIndexDataflowHelperFactory indexDataflowHelperFactory =
                new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(), spPc.first);
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            rtreeSearchOp = new RTreeSearchOperatorDescriptor(jobSpec, outputRecDesc, keyFields, true, true,
                    indexDataflowHelperFactory, retainInput, retainMissing, context.getMissingWriterFactory(),
                    searchCallbackFactory, minFilterFieldIndexes, maxFilterFieldIndexes, propagateFilter,
                    isIndexOnlyPlan, failValueForIndexOnlyPlan, successValueForIndexOnlyPlan);
        } else {
            // Create the operator
            rtreeSearchOp = new ExternalRTreeSearchOperatorDescriptor(jobSpec, outputRecDesc, keyFields, true, true,
                    indexDataflowHelperFactory, retainInput, retainMissing, context.getMissingWriterFactory(),
                    searchCallbackFactory, minFilterFieldIndexes, maxFilterFieldIndexes,
                    ExternalDatasetsRegistry.INSTANCE.getAndLockDatasetVersion(dataset, this));
        }

        return new Pair<>(rtreeSearchOp, spPc.second);
    }

    @Override
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc) {
        FileSplitDataSink fsds = (FileSplitDataSink) sink;
        FileSplitSinkId fssi = fsds.getId();
        FileSplit fs = fssi.getFileSplit();
        File outFile = new File(fs.getPath());
        String nodeId = fs.getNodeName();

        SinkWriterRuntimeFactory runtime =
                new SinkWriterRuntimeFactory(printColumns, printerFactories, outFile, getWriterFactory(), inputDesc);
        AlgebricksPartitionConstraint apc = new AlgebricksAbsolutePartitionConstraint(new String[] { nodeId });
        return new Pair<>(runtime, apc);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getResultHandleRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc,
            IResultMetadata metadata, JobSpecification spec) throws AlgebricksException {
        ResultSetDataSink rsds = (ResultSetDataSink) sink;
        ResultSetSinkId rssId = rsds.getId();
        ResultSetId rsId = rssId.getResultSetId();
        ResultWriterOperatorDescriptor resultWriter = null;
        try {
            IResultSerializerFactory resultSerializedAppenderFactory = resultSerializerFactoryProvider
                    .getAqlResultSerializerFactoryProvider(printColumns, printerFactories, getWriterFactory());
            resultWriter = new ResultWriterOperatorDescriptor(spec, rsId, metadata, getResultAsyncMode(),
                    resultSerializedAppenderFactory, getMaxResultReads());
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        return new Pair<>(resultWriter, null);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(
            IDataSource<DataSourceId> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payload, List<LogicalVariable> additionalNonKeyFields, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        String dataverseName = dataSource.getId().getDataverseName();
        String datasetName = dataSource.getId().getDatasourceName();
        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, dataverseName, datasetName);
        int numKeys = keys.size();
        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;

        // move key fields to front
        int[] fieldPermutation = new int[numKeys + 1 + numFilterFields];
        int i = 0;
        for (LogicalVariable varKey : keys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            i++;
        }
        fieldPermutation[numKeys] = propagatedSchema.findVariable(payload);
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalNonKeyFields.get(0));
            fieldPermutation[numKeys + 1] = idx;
        }

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                getSplitProviderAndConstraints(dataset);
        long numElementsHint = getCardinalityPerPartitionHint(dataset);
        // TODO
        // figure out the right behavior of the bulkload and then give the
        // right callback
        // (ex. what's the expected behavior when there is an error during
        // bulkload?)
        IIndexDataflowHelperFactory indexHelperFactory =
                new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(), splitsAndConstraint.first);
        LSMIndexBulkLoadOperatorDescriptor btreeBulkLoad = new LSMIndexBulkLoadOperatorDescriptor(spec, null,
                fieldPermutation, StorageConstants.DEFAULT_TREE_FILL_FACTOR, false, numElementsHint, true,
                indexHelperFactory, null, BulkLoadUsage.LOAD, dataset.getDatasetId(), null);
        return new Pair<>(btreeBulkLoad, splitsAndConstraint.second);
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
            RecordDescriptor inputRecordDesc, JobGenContext context, JobSpecification spec) throws AlgebricksException {
        return getInsertOrDeleteRuntime(IndexOperation.DELETE, dataSource, propagatedSchema, keys, payload,
                additionalNonKeyFields, inputRecordDesc, context, spec, false, null);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<String, DataSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec,
            boolean bulkload) throws AlgebricksException {
        return getIndexInsertOrDeleteOrUpsertRuntime(IndexOperation.INSERT, dataSourceIndex, propagatedSchema,
                inputSchemas, typeEnv, primaryKeys, secondaryKeys, additionalNonKeyFields, filterExpr, recordDesc,
                context, spec, bulkload, null, null, null);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<String, DataSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec)
            throws AlgebricksException {
        return getIndexInsertOrDeleteOrUpsertRuntime(IndexOperation.DELETE, dataSourceIndex, propagatedSchema,
                inputSchemas, typeEnv, primaryKeys, secondaryKeys, additionalNonKeyFields, filterExpr, recordDesc,
                context, spec, false, null, null, null);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexUpsertRuntime(
            IDataSourceIndex<String, DataSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalFilteringKeys,
            ILogicalExpression filterExpr, LogicalVariable upsertIndicatorVar, List<LogicalVariable> prevSecondaryKeys,
            LogicalVariable prevAdditionalFilteringKey, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        return getIndexInsertOrDeleteOrUpsertRuntime(IndexOperation.UPSERT, dataSourceIndex, propagatedSchema,
                inputSchemas, typeEnv, primaryKeys, secondaryKeys, additionalFilteringKeys, filterExpr, recordDesc,
                context, spec, false, upsertIndicatorVar, prevSecondaryKeys, prevAdditionalFilteringKey);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getTokenizerRuntime(
            IDataSourceIndex<String, DataSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec, boolean bulkload) throws AlgebricksException {

        String indexName = dataSourceIndex.getId();
        String dataverseName = dataSourceIndex.getDataSource().getId().getDataverseName();
        String datasetName = dataSourceIndex.getDataSource().getId().getDatasourceName();

        IOperatorSchema inputSchema;
        if (inputSchemas.length > 0) {
            inputSchema = inputSchemas[0];
        } else {
            throw new AlgebricksException("TokenizeOperator can not operate without any input variable.");
        }

        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, dataverseName, datasetName);
        Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                dataset.getDatasetName(), indexName);
        // TokenizeOperator only supports a keyword or n-gram index.
        switch (secondaryIndex.getIndexType()) {
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
                return getBinaryTokenizerRuntime(dataverseName, datasetName, indexName, inputSchema, propagatedSchema,
                        primaryKeys, secondaryKeys, recordDesc, spec, secondaryIndex.getIndexType());
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
     *         dataset per partition
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
        int numPartitions = 0;
        List<String> nodeGroup =
                MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, dataset.getNodeGroupName()).getNodeNames();
        IClusterStateManager csm = appCtx.getClusterStateManager();
        for (String nd : nodeGroup) {
            numPartitions += csm.getNodePartitionsCount(nd);
        }
        return numElementsHint / numPartitions;
    }

    protected IAdapterFactory getConfiguredAdapterFactory(Dataset dataset, String adapterName,
            Map<String, String> configuration, ARecordType itemType, ARecordType metaType) throws AlgebricksException {
        try {
            configuration.put(ExternalDataConstants.KEY_DATAVERSE, dataset.getDataverseName());
            IAdapterFactory adapterFactory = AdapterFactoryProvider.getAdapterFactory(
                    getApplicationContext().getServiceContext(), adapterName, configuration, itemType, metaType);

            // check to see if dataset is indexed
            Index filesIndex =
                    MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(), dataset.getDatasetName(),
                            dataset.getDatasetName().concat(IndexingConstants.EXTERNAL_FILE_INDEX_NAME_SUFFIX));

            if (filesIndex != null && filesIndex.getPendingOp() == 0) {
                // get files
                List<ExternalFile> files = MetadataManager.INSTANCE.getDatasetExternalFiles(mdTxnCtx, dataset);
                Iterator<ExternalFile> iterator = files.iterator();
                while (iterator.hasNext()) {
                    if (iterator.next().getPendingOp() != ExternalFilePendingOp.NO_OP) {
                        iterator.remove();
                    }
                }
            }

            return adapterFactory;
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

    public Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitAndConstraints(String dataverse) {
        return SplitsAndConstraintsUtil.getDataverseSplitProviderAndConstraints(appCtx.getClusterStateManager(),
                dataverse);
    }

    public FileSplit[] splitsForIndex(MetadataTransactionContext mdTxnCtx, Dataset dataset, String indexName)
            throws AlgebricksException {
        return SplitsAndConstraintsUtil.getIndexSplits(dataset, indexName, mdTxnCtx, appCtx.getClusterStateManager());
    }

    public DatasourceAdapter getAdapter(MetadataTransactionContext mdTxnCtx, String dataverseName, String adapterName)
            throws AlgebricksException {
        DatasourceAdapter adapter;
        // search in default namespace (built-in adapter)
        adapter = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME, adapterName);

        // search in dataverse (user-defined adapter)
        if (adapter == null) {
            adapter = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, dataverseName, adapterName);
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
        try {
            // Get data type
            ARecordType itemType =
                    (ARecordType) MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                            dataset.getDataverseName(), dataset.getItemTypeName()).getDatatype();
            ExternalDatasetDetails datasetDetails = (ExternalDatasetDetails) dataset.getDatasetDetails();
            LookupAdapterFactory<?> adapterFactory = AdapterFactoryProvider.getLookupAdapterFactory(
                    getApplicationContext().getServiceContext(), datasetDetails.getProperties(), itemType, ridIndexes,
                    retainInput, retainMissing, context.getMissingWriterFactory());
            String fileIndexName = IndexingConstants.getFilesIndexName(dataset.getDatasetName());
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc =
                    metadataProvider.getSplitProviderAndConstraints(dataset, fileIndexName);
            Index fileIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), fileIndexName);
            // Create the file index data flow helper
            IIndexDataflowHelperFactory indexDataflowHelperFactory =
                    new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(), spPc.first);
            // Create the out record descriptor, appContext and fileSplitProvider for the
            // files index
            RecordDescriptor outRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
            ISearchOperationCallbackFactory searchOpCallbackFactory =
                    dataset.getSearchCallbackFactory(storageComponentProvider, fileIndex, IndexOperation.SEARCH, null);
            // Create the operator
            ExternalLookupOperatorDescriptor op = new ExternalLookupOperatorDescriptor(jobSpec, adapterFactory,
                    outRecDesc, indexDataflowHelperFactory, searchOpCallbackFactory,
                    ExternalDatasetsRegistry.INSTANCE.getAndLockDatasetVersion(dataset, this));
            return new Pair<>(op, spPc.second);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getUpsertRuntime(
            IDataSource<DataSourceId> dataSource, IOperatorSchema inputSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, LogicalVariable payload, List<LogicalVariable> filterKeys,
            List<LogicalVariable> additionalNonFilterFields, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        String datasetName = dataSource.getId().getDatasourceName();
        Dataset dataset = findDataset(dataSource.getId().getDataverseName(), datasetName);
        if (dataset == null) {
            throw new AlgebricksException(
                    "Unknown dataset " + datasetName + " in dataverse " + dataSource.getId().getDataverseName());
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
        // set the filters' permutations.
        if (numFilterFields > 0) {
            int idx = inputSchema.findVariable(filterKeys.get(0));
            fieldPermutation[i++] = idx;
        }

        if (additionalNonFilterFields != null) {
            for (LogicalVariable var : additionalNonFilterFields) {
                int idx = inputSchema.findVariable(var);
                fieldPermutation[i++] = idx;
            }
        }
        return DatasetUtil.createPrimaryIndexUpsertOp(spec, this, dataset, recordDesc, fieldPermutation,
                context.getMissingWriterFactory());
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDatasetDataScannerRuntime(
            JobSpecification jobSpec, IAType itemType, IAdapterFactory adapterFactory) throws AlgebricksException {
        if (itemType.getTypeTag() != ATypeTag.OBJECT) {
            throw new AlgebricksException("Can only scan datasets of records.");
        }

        ISerializerDeserializer<?> payloadSerde =
                getDataFormat().getSerdeProvider().getSerializerDeserializer(itemType);
        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] { payloadSerde });

        ExternalScanOperatorDescriptor dataScanner =
                new ExternalScanOperatorDescriptor(jobSpec, scannerDesc, adapterFactory);

        AlgebricksPartitionConstraint constraint;
        try {
            constraint = adapterFactory.getPartitionConstraint();
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }

        return new Pair<>(dataScanner, constraint);
    }

    private Pair<IBinaryComparatorFactory[], ITypeTraits[]> getComparatorFactoriesAndTypeTraitsOfSecondaryBTreeIndex(
            List<List<String>> sidxKeyFieldNames, List<IAType> sidxKeyFieldTypes, List<List<String>> pidxKeyFieldNames,
            ARecordType recType, DatasetType dsType, boolean hasMeta, List<Integer> primaryIndexKeyIndicators,
            List<Integer> secondaryIndexIndicators, ARecordType metaType) throws AlgebricksException {

        IBinaryComparatorFactory[] comparatorFactories;
        ITypeTraits[] typeTraits;
        int sidxKeyFieldCount = sidxKeyFieldNames.size();
        int pidxKeyFieldCount = pidxKeyFieldNames.size();
        typeTraits = new ITypeTraits[sidxKeyFieldCount + pidxKeyFieldCount];
        comparatorFactories = new IBinaryComparatorFactory[sidxKeyFieldCount + pidxKeyFieldCount];

        int i = 0;
        for (; i < sidxKeyFieldCount; ++i) {
            Pair<IAType, Boolean> keyPairType =
                    Index.getNonNullableOpenFieldType(sidxKeyFieldTypes.get(i), sidxKeyFieldNames.get(i),
                            (hasMeta && secondaryIndexIndicators.get(i).intValue() == 1) ? metaType : recType);
            IAType keyType = keyPairType.first;
            comparatorFactories[i] = BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType, true);
            typeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(keyType);
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
            comparatorFactories[i] = BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType, true);
            typeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(keyType);
        }

        return new Pair<>(comparatorFactories, typeTraits);
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertOrDeleteRuntime(IndexOperation indexOp,
            IDataSource<DataSourceId> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payload, List<LogicalVariable> additionalNonKeyFields, RecordDescriptor inputRecordDesc,
            JobGenContext context, JobSpecification spec, boolean bulkload,
            List<LogicalVariable> additionalNonFilteringFields) throws AlgebricksException {

        String datasetName = dataSource.getId().getDatasourceName();
        Dataset dataset =
                MetadataManagerUtil.findExistingDataset(mdTxnCtx, dataSource.getId().getDataverseName(), datasetName);
        int numKeys = keys.size();
        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;
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
        int[] filterFields = new int[numFilterFields];
        if (numFilterFields > 0) {
            int idx = propagatedSchema.findVariable(additionalNonKeyFields.get(0));
            fieldPermutation[i++] = idx;
            filterFields[0] = idx;
        }
        if (additionalNonFilteringFields != null) {
            for (LogicalVariable variable : additionalNonFilteringFields) {
                int idx = propagatedSchema.findVariable(variable);
                fieldPermutation[i++] = idx;
            }
        }

        Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                dataset.getDatasetName(), dataset.getDatasetName());
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                getSplitProviderAndConstraints(dataset);

        // prepare callback
        int[] primaryKeyFields = new int[numKeys];
        for (i = 0; i < numKeys; i++) {
            primaryKeyFields[i] = i;
        }
        IModificationOperationCallbackFactory modificationCallbackFactory = dataset
                .getModificationCallbackFactory(storageComponentProvider, primaryIndex, indexOp, primaryKeyFields);
        IIndexDataflowHelperFactory idfh =
                new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(), splitsAndConstraint.first);
        IOperatorDescriptor op;
        if (bulkload) {
            long numElementsHint = getCardinalityPerPartitionHint(dataset);
            op = new LSMIndexBulkLoadOperatorDescriptor(spec, inputRecordDesc, fieldPermutation,
                    StorageConstants.DEFAULT_TREE_FILL_FACTOR, true, numElementsHint, true, idfh, null,
                    BulkLoadUsage.LOAD, dataset.getDatasetId(), null);
        } else {
            if (indexOp == IndexOperation.INSERT) {
                ISearchOperationCallbackFactory searchCallbackFactory = dataset
                        .getSearchCallbackFactory(storageComponentProvider, primaryIndex, indexOp, primaryKeyFields);

                Optional<Index> primaryKeyIndex = MetadataManager.INSTANCE
                        .getDatasetIndexes(mdTxnCtx, dataset.getDataverseName(), dataset.getDatasetName()).stream()
                        .filter(index -> index.isPrimaryKeyIndex()).findFirst();
                IIndexDataflowHelperFactory pkidfh = null;
                if (primaryKeyIndex.isPresent()) {
                    Pair<IFileSplitProvider, AlgebricksPartitionConstraint> primaryKeySplitsAndConstraint =
                            getSplitProviderAndConstraints(dataset, primaryKeyIndex.get().getIndexName());
                    pkidfh = new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(),
                            primaryKeySplitsAndConstraint.first);
                }
                op = new LSMPrimaryInsertOperatorDescriptor(spec, inputRecordDesc, fieldPermutation, idfh, pkidfh,
                        modificationCallbackFactory, searchCallbackFactory, numKeys, filterFields);

            } else {
                op = new LSMTreeInsertDeleteOperatorDescriptor(spec, inputRecordDesc, fieldPermutation, indexOp, idfh,
                        null, true, modificationCallbackFactory);
            }
        }
        return new Pair<>(op, splitsAndConstraint.second);
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertOrDeleteOrUpsertRuntime(
            IndexOperation indexOp, IDataSourceIndex<String, DataSourceId> dataSourceIndex,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalNonKeyFields, ILogicalExpression filterExpr,
            RecordDescriptor inputRecordDesc, JobGenContext context, JobSpecification spec, boolean bulkload,
            LogicalVariable upsertIndicatorVar, List<LogicalVariable> prevSecondaryKeys,
            LogicalVariable prevAdditionalFilteringKey) throws AlgebricksException {
        String indexName = dataSourceIndex.getId();
        String dataverseName = dataSourceIndex.getDataSource().getId().getDataverseName();
        String datasetName = dataSourceIndex.getDataSource().getId().getDatasourceName();

        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, dataverseName, datasetName);
        Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                dataset.getDatasetName(), indexName);

        ArrayList<LogicalVariable> prevAdditionalFilteringKeys = null;
        if (indexOp == IndexOperation.UPSERT && prevAdditionalFilteringKey != null) {
            prevAdditionalFilteringKeys = new ArrayList<>();
            prevAdditionalFilteringKeys.add(prevAdditionalFilteringKey);
        }
        AsterixTupleFilterFactory filterFactory = createTupleFilterFactory(inputSchemas, typeEnv, filterExpr, context);
        switch (secondaryIndex.getIndexType()) {
            case BTREE:
                return getBTreeRuntime(dataverseName, datasetName, indexName, propagatedSchema, primaryKeys,
                        secondaryKeys, additionalNonKeyFields, filterFactory, inputRecordDesc, context, spec, indexOp,
                        bulkload, upsertIndicatorVar, prevSecondaryKeys, prevAdditionalFilteringKeys);
            case RTREE:
                return getRTreeRuntime(dataverseName, datasetName, indexName, propagatedSchema, primaryKeys,
                        secondaryKeys, additionalNonKeyFields, filterFactory, inputRecordDesc, context, spec, indexOp,
                        bulkload, upsertIndicatorVar, prevSecondaryKeys, prevAdditionalFilteringKeys);
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
                return getInvertedIndexRuntime(dataverseName, datasetName, indexName, propagatedSchema, primaryKeys,
                        secondaryKeys, additionalNonKeyFields, filterFactory, inputRecordDesc, context, spec, indexOp,
                        secondaryIndex.getIndexType(), bulkload, upsertIndicatorVar, prevSecondaryKeys,
                        prevAdditionalFilteringKeys);
            default:
                throw new AlgebricksException(
                        indexOp.name() + "Insert, upsert, and delete not implemented for index type: "
                                + secondaryIndex.getIndexType());
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getBTreeRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            AsterixTupleFilterFactory filterFactory, RecordDescriptor inputRecordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp, boolean bulkload, LogicalVariable upsertIndicatorVar,
            List<LogicalVariable> prevSecondaryKeys, List<LogicalVariable> prevAdditionalFilteringKeys)
            throws AlgebricksException {
        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, dataverseName, datasetName);
        int numKeys = primaryKeys.size() + secondaryKeys.size();
        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;

        // generate field permutations
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
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    getSplitProviderAndConstraints(dataset, secondaryIndex.getIndexName());
            // prepare callback
            IModificationOperationCallbackFactory modificationCallbackFactory = dataset.getModificationCallbackFactory(
                    storageComponentProvider, secondaryIndex, indexOp, modificationCallbackPrimaryKeyFields);
            IIndexDataflowHelperFactory idfh = new IndexDataflowHelperFactory(
                    storageComponentProvider.getStorageManager(), splitsAndConstraint.first);
            IOperatorDescriptor op;
            if (bulkload) {
                long numElementsHint = getCardinalityPerPartitionHint(dataset);
                op = new LSMIndexBulkLoadOperatorDescriptor(spec, inputRecordDesc, fieldPermutation,
                        StorageConstants.DEFAULT_TREE_FILL_FACTOR, false, numElementsHint, false, idfh, null,
                        BulkLoadUsage.LOAD, dataset.getDatasetId(), filterFactory);
            } else if (indexOp == IndexOperation.UPSERT) {
                int upsertIndicatorFieldIndex = propagatedSchema.findVariable(upsertIndicatorVar);
                op = new LSMSecondaryUpsertOperatorDescriptor(spec, inputRecordDesc, fieldPermutation, idfh,
                        filterFactory, modificationCallbackFactory, upsertIndicatorFieldIndex,
                        BinaryBooleanInspector.FACTORY, prevFieldPermutation);
            } else {
                op = new LSMTreeInsertDeleteOperatorDescriptor(spec, inputRecordDesc, fieldPermutation, indexOp, idfh,
                        filterFactory, false, modificationCallbackFactory);
            }
            return new Pair<>(op, splitsAndConstraint.second);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getRTreeRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            AsterixTupleFilterFactory filterFactory, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp, boolean bulkload, LogicalVariable upsertIndicatorVar,
            List<LogicalVariable> prevSecondaryKeys, List<LogicalVariable> prevAdditionalFilteringKeys)
            throws AlgebricksException {
        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, dataverseName, datasetName);
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType = MetadataManager.INSTANCE
                .getDatatype(mdTxnCtx, dataset.getItemTypeDataverseName(), itemTypeName).getDatatype();
        validateRecordType(itemType);
        ARecordType recType = (ARecordType) itemType;
        Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                dataset.getDatasetName(), indexName);
        List<List<String>> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();
        List<IAType> secondaryKeyTypes = secondaryIndex.getKeyFieldTypes();
        Pair<IAType, Boolean> keyPairType =
                Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(0), secondaryKeyExprs.get(0), recType);
        IAType spatialType = keyPairType.first;
        int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numSecondaryKeys = dimension * 2;
        int numPrimaryKeys = primaryKeys.size();
        int numKeys = numSecondaryKeys + numPrimaryKeys;

        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;
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
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                getSplitProviderAndConstraints(dataset, secondaryIndex.getIndexName());

        // prepare callback
        IModificationOperationCallbackFactory modificationCallbackFactory = dataset.getModificationCallbackFactory(
                storageComponentProvider, secondaryIndex, indexOp, modificationCallbackPrimaryKeyFields);
        IIndexDataflowHelperFactory indexDataflowHelperFactory =
                new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(), splitsAndConstraint.first);
        IOperatorDescriptor op;
        if (bulkload) {
            long numElementsHint = getCardinalityPerPartitionHint(dataset);
            op = new LSMIndexBulkLoadOperatorDescriptor(spec, recordDesc, fieldPermutation,
                    StorageConstants.DEFAULT_TREE_FILL_FACTOR, false, numElementsHint, false,
                    indexDataflowHelperFactory, null, BulkLoadUsage.LOAD, dataset.getDatasetId(), filterFactory);
        } else if (indexOp == IndexOperation.UPSERT) {
            int upsertIndicatorFieldIndex = propagatedSchema.findVariable(upsertIndicatorVar);
            op = new LSMSecondaryUpsertOperatorDescriptor(spec, recordDesc, fieldPermutation,
                    indexDataflowHelperFactory, filterFactory, modificationCallbackFactory, upsertIndicatorFieldIndex,
                    BinaryBooleanInspector.FACTORY, prevFieldPermutation);
        } else {
            op = new LSMTreeInsertDeleteOperatorDescriptor(spec, recordDesc, fieldPermutation, indexOp,
                    indexDataflowHelperFactory, filterFactory, false, modificationCallbackFactory);
        }
        return new Pair<>(op, splitsAndConstraint.second);
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInvertedIndexRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            AsterixTupleFilterFactory filterFactory, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp, IndexType indexType, boolean bulkload,
            LogicalVariable upsertIndicatorVar, List<LogicalVariable> prevSecondaryKeys,
            List<LogicalVariable> prevAdditionalFilteringKeys) throws AlgebricksException {
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
        if ((secondaryKeys.size() > 1 && !isPartitioned) || (secondaryKeys.size() > 2 && isPartitioned)) {
            throw new AlgebricksException("Cannot create composite inverted index on multiple fields.");
        }
        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, dataverseName, datasetName);
        // For tokenization, sorting and loading.
        // One token (+ optional partitioning field) + primary keys: [token,
        // number of token, PK]
        int numKeys = primaryKeys.size() + secondaryKeys.size();
        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;

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
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    getSplitProviderAndConstraints(dataset, secondaryIndex.getIndexName());

            // prepare callback
            IModificationOperationCallbackFactory modificationCallbackFactory = dataset.getModificationCallbackFactory(
                    storageComponentProvider, secondaryIndex, indexOp, modificationCallbackPrimaryKeyFields);
            IIndexDataflowHelperFactory indexDataFlowFactory = new IndexDataflowHelperFactory(
                    storageComponentProvider.getStorageManager(), splitsAndConstraint.first);
            IOperatorDescriptor op;
            if (bulkload) {
                long numElementsHint = getCardinalityPerPartitionHint(dataset);
                op = new LSMIndexBulkLoadOperatorDescriptor(spec, recordDesc, fieldPermutation,
                        StorageConstants.DEFAULT_TREE_FILL_FACTOR, false, numElementsHint, false, indexDataFlowFactory,
                        null, BulkLoadUsage.LOAD, dataset.getDatasetId(), filterFactory);
            } else if (indexOp == IndexOperation.UPSERT) {
                int upsertIndicatorFieldIndex = propagatedSchema.findVariable(upsertIndicatorVar);
                op = new LSMSecondaryUpsertOperatorDescriptor(spec, recordDesc, fieldPermutation, indexDataFlowFactory,
                        filterFactory, modificationCallbackFactory, upsertIndicatorFieldIndex,
                        BinaryBooleanInspector.FACTORY, prevFieldPermutation);
            } else {
                op = new LSMTreeInsertDeleteOperatorDescriptor(spec, recordDesc, fieldPermutation, indexOp,
                        indexDataFlowFactory, filterFactory, false, modificationCallbackFactory);
            }
            return new Pair<>(op, splitsAndConstraint.second);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    // Get a Tokenizer for the bulk-loading data into a n-gram or keyword index.
    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getBinaryTokenizerRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema inputSchema, IOperatorSchema propagatedSchema,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys, RecordDescriptor recordDesc,
            JobSpecification spec, IndexType indexType) throws AlgebricksException {

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

        Dataset dataset = MetadataManagerUtil.findExistingDataset(mdTxnCtx, dataverseName, datasetName);
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType;
        try {
            itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getItemTypeDataverseName(), itemTypeName)
                    .getDatatype();

            if (itemType.getTypeTag() != ATypeTag.OBJECT) {
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
            IAType secondaryKeyType;
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryKeyTypeEntries.get(0),
                    secondaryKeyExprs.get(0), recType);
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
                    secondaryKeyType.getTypeTag(), indexType, secondaryIndex.getGramLength());

            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                    getSplitProviderAndConstraints(dataset, secondaryIndex.getIndexName());

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

            tokenizerOp = new BinaryTokenizerOperatorDescriptor(spec, tokenKeyPairRecDesc, tokenizerFactory, docField,
                    keyFields, isPartitioned, true, false, MissingWriterFactory.INSTANCE);
            return new Pair<>(tokenizerOp, splitsAndConstraint.second);
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

    public Pair<IFileSplitProvider, AlgebricksPartitionConstraint> getSplitProviderAndConstraints(Dataset ds)
            throws AlgebricksException {
        return getSplitProviderAndConstraints(ds, ds.getDatasetName());
    }

    public Pair<IFileSplitProvider, AlgebricksPartitionConstraint> getSplitProviderAndConstraints(Dataset ds,
            String indexName) throws AlgebricksException {
        FileSplit[] splits = splitsForIndex(mdTxnCtx, ds, indexName);
        return StoragePathUtil.splitProviderAndPartitionConstraints(splits);
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

    public void addAccessedDataset(Dataset dataset) {
        txnAccessedDatasets.add(dataset);
    }

    public Set<Dataset> getAccessedDatasets() {
        return Collections.unmodifiableSet(txnAccessedDatasets);
    }
}
