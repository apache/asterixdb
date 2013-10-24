/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.metadata.declared;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.config.AsterixStorageProperties;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.context.AsterixVirtualBufferCacheProvider;
import edu.uci.ics.asterix.common.context.ITransactionSubsystemProvider;
import edu.uci.ics.asterix.common.context.TransactionSubsystemProvider;
import edu.uci.ics.asterix.common.dataflow.AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor;
import edu.uci.ics.asterix.common.dataflow.AsterixLSMTreeInsertDeleteOperatorDescriptor;
import edu.uci.ics.asterix.common.dataflow.IAsterixApplicationContextInfo;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMInvertedIndexIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMRTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.parse.IParseFileSplitsDecl;
import edu.uci.ics.asterix.common.transactions.IRecoveryManager.ResourceType;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.dataflow.data.nontagged.valueproviders.AqlPrimitiveValueProviderFactory;
import edu.uci.ics.asterix.external.adapter.factory.HDFSAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.IAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.IGenericDatasetAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.ITypedDatasetAdapterFactory;
import edu.uci.ics.asterix.external.data.operator.ExternalDataScanOperatorDescriptor;
import edu.uci.ics.asterix.external.data.operator.FeedIntakeOperatorDescriptor;
import edu.uci.ics.asterix.external.data.operator.FeedMessageOperatorDescriptor;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.ITypedDatasourceAdapter;
import edu.uci.ics.asterix.external.feed.lifecycle.FeedId;
import edu.uci.ics.asterix.external.feed.lifecycle.IFeedMessage;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.dataset.hints.DatasetHints.DatasetCardinalityHint;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.base.AsterixTupleFilterFactory;
import edu.uci.ics.asterix.runtime.formats.FormatUtils;
import edu.uci.ics.asterix.runtime.formats.NonTaggedDataFormat;
import edu.uci.ics.asterix.runtime.job.listener.JobEventListenerFactory;
import edu.uci.ics.asterix.transaction.management.opcallbacks.PrimaryIndexInstantSearchOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.opcallbacks.PrimaryIndexModificationOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerProvider;
import edu.uci.ics.asterix.transaction.management.opcallbacks.PrimaryIndexSearchOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.opcallbacks.SecondaryIndexModificationOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import edu.uci.ics.asterix.transaction.management.opcallbacks.SecondaryIndexSearchOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.data.IAWriterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.SinkWriterRuntimeFactory;
import edu.uci.ics.hyracks.api.context.ICCContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IResultSerializerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.dataflow.LSMRTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.am.rtree.linearize.HilbertDoubleComparatorFactory;
import edu.uci.ics.hyracks.storage.am.rtree.linearize.ZCurveDoubleComparatorFactory;
import edu.uci.ics.hyracks.storage.am.rtree.linearize.ZCurveIntComparatorFactory;

public class AqlMetadataProvider implements IMetadataProvider<AqlSourceId, String> {
    private static Logger LOGGER = Logger.getLogger(AqlMetadataProvider.class.getName());
    private MetadataTransactionContext mdTxnCtx;
    private boolean isWriteTransaction;
    private Map<String, String[]> stores;
    private Map<String, String> config;
    private IAWriterFactory writerFactory;
    private FileSplit outputFile;
    private boolean asyncResults;
    private ResultSetId resultSetId;
    private IResultSerializerFactoryProvider resultSerializerFactoryProvider;

    private final Dataverse defaultDataverse;
    private JobId jobId;

    private final AsterixStorageProperties storageProperties;

    private static final Map<String, String> adapterFactoryMapping = initializeAdapterFactoryMapping();
    private static Scheduler hdfsScheduler;

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
        ICCContext ccContext = AsterixAppContextInfo.getInstance().getCCApplicationContext().getCCContext();
        try {
            if (hdfsScheduler == null) {
                //set the singleton hdfs scheduler
                hdfsScheduler = new Scheduler(ccContext.getClusterControllerInfo().getClientNetAddress(), ccContext
                        .getClusterControllerInfo().getClientNetPort());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    @Override
    public AqlDataSource findDataSource(AqlSourceId id) throws AlgebricksException {
        AqlSourceId aqlId = (AqlSourceId) id;
        try {
            return lookupSourceInMetadata(aqlId);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public boolean isWriteTransaction() {
        return isWriteTransaction;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(
            IDataSource<AqlSourceId> dataSource, List<LogicalVariable> scanVariables,
            List<LogicalVariable> projectVariables, boolean projectPushed, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig)
            throws AlgebricksException {
        Dataset dataset;
        try {
            dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataSource.getId().getDataverseName(), dataSource
                    .getId().getDatasetName());

            if (dataset == null) {
                throw new AlgebricksException("Unknown dataset " + dataSource.getId().getDatasetName()
                        + " in dataverse " + dataSource.getId().getDataverseName());
            }
            switch (dataset.getDatasetType()) {
                case FEED:
                    if (dataSource instanceof ExternalFeedDataSource) {
                        return buildExternalDatasetScan(jobSpec, dataset, dataSource);
                    } else {
                        return buildInternalDatasetScan(jobSpec, scanVariables, opSchema, typeEnv, dataset, dataSource,
                                context, implConfig);

                    }
                case INTERNAL: {
                    return buildInternalDatasetScan(jobSpec, scanVariables, opSchema, typeEnv, dataset, dataSource,
                            context, implConfig);
                }
                case EXTERNAL: {
                    return buildExternalDatasetScan(jobSpec, dataset, dataSource);
                }
                default: {
                    throw new IllegalArgumentException();
                }
            }
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildInternalDatasetScan(JobSpecification jobSpec,
            List<LogicalVariable> outputVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            Dataset dataset, IDataSource<AqlSourceId> dataSource, JobGenContext context, Object implConfig)
            throws AlgebricksException, MetadataException {
        AqlSourceId asid = dataSource.getId();
        String dataverseName = asid.getDataverseName();
        String datasetName = asid.getDatasetName();
        Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, datasetName);
        return buildBtreeRuntime(jobSpec, outputVars, opSchema, typeEnv, context, false, dataset,
                primaryIndex.getIndexName(), null, null, true, true, implConfig);
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDatasetScan(JobSpecification jobSpec,
            Dataset dataset, IDataSource<AqlSourceId> dataSource) throws AlgebricksException, MetadataException {
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getDataverseName(), itemTypeName)
                .getDatatype();
        if (dataSource instanceof ExternalFeedDataSource) {
            return buildFeedIntakeRuntime(jobSpec, dataset);
        } else {
            return buildExternalDataScannerRuntime(jobSpec, itemType,
                    (ExternalDatasetDetails) dataset.getDatasetDetails(), NonTaggedDataFormat.INSTANCE);
        }
    }

    @SuppressWarnings("rawtypes")
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDataScannerRuntime(
            JobSpecification jobSpec, IAType itemType, ExternalDatasetDetails datasetDetails, IDataFormat format)
            throws AlgebricksException {
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Can only scan datasets of records.");
        }

        IGenericDatasetAdapterFactory adapterFactory;
        IDatasourceAdapter adapter;
        String adapterName;
        DatasourceAdapter adapterEntity;
        String adapterFactoryClassname;
        try {
            adapterName = datasetDetails.getAdapter();
            adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                    adapterName);
            if (adapterEntity != null) {
                adapterFactoryClassname = adapterEntity.getClassname();
                adapterFactory = (IGenericDatasetAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
            } else {
                adapterFactoryClassname = adapterFactoryMapping.get(adapterName);
                if (adapterFactoryClassname == null) {
                    throw new AlgebricksException(" Unknown adapter :" + adapterName);
                }
                adapterFactory = (IGenericDatasetAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
            }

            adapter = ((IGenericDatasetAdapterFactory) adapterFactory).createAdapter(
                    wrapProperties(datasetDetails.getProperties()), itemType);
        } catch (AlgebricksException ae) {
            throw ae;
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException("Unable to create adapter " + e);
        }

        if (!(adapter.getAdapterType().equals(IDatasourceAdapter.AdapterType.READ) || adapter.getAdapterType().equals(
                IDatasourceAdapter.AdapterType.READ_WRITE))) {
            throw new AlgebricksException("external dataset adapter does not support read operation");
        }
        ARecordType rt = (ARecordType) itemType;

        ISerializerDeserializer payloadSerde = format.getSerdeProvider().getSerializerDeserializer(itemType);
        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] { payloadSerde });

        ExternalDataScanOperatorDescriptor dataScanner = new ExternalDataScanOperatorDescriptor(jobSpec,
                wrapPropertiesEmpty(datasetDetails.getProperties()), rt, scannerDesc, adapterFactory);

        AlgebricksPartitionConstraint constraint;
        try {
            constraint = adapter.getPartitionConstraint();
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }

        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(dataScanner, constraint);
    }

    @SuppressWarnings("rawtypes")
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildScannerRuntime(JobSpecification jobSpec,
            IAType itemType, IParseFileSplitsDecl decl, IDataFormat format) throws AlgebricksException {
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Can only scan datasets of records.");
        }
        ARecordType rt = (ARecordType) itemType;
        ITupleParserFactory tupleParser = format.createTupleParser(rt, decl);
        FileSplit[] splits = decl.getSplits();
        IFileSplitProvider scannerSplitProvider = new ConstantFileSplitProvider(splits);
        ISerializerDeserializer payloadSerde = format.getSerdeProvider().getSerializerDeserializer(itemType);
        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] { payloadSerde });
        IOperatorDescriptor scanner = new FileScanOperatorDescriptor(jobSpec, scannerSplitProvider, tupleParser,
                scannerDesc);
        String[] locs = new String[splits.length];
        for (int i = 0; i < splits.length; i++) {
            locs[i] = splits[i].getNodeName();
        }
        AlgebricksPartitionConstraint apc = new AlgebricksAbsolutePartitionConstraint(locs);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(scanner, apc);
    }

    @SuppressWarnings("rawtypes")
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildFeedIntakeRuntime(JobSpecification jobSpec,
            Dataset dataset) throws AlgebricksException {

        FeedDatasetDetails datasetDetails = (FeedDatasetDetails) dataset.getDatasetDetails();
        DatasourceAdapter adapterEntity;
        IDatasourceAdapter adapter;
        IAdapterFactory adapterFactory;
        IAType adapterOutputType;
        String adapterName;
        String adapterFactoryClassname;

        try {
            adapterName = datasetDetails.getAdapterFactory();
            adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                    adapterName);
            if (adapterEntity != null) {
                adapterFactoryClassname = adapterEntity.getClassname();
                adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
            } else {
                adapterFactoryClassname = adapterFactoryMapping.get(adapterName);
                if (adapterFactoryClassname != null) {
                } else {
                    // adapterName has been provided as a fully qualified
                    // classname
                    adapterFactoryClassname = adapterName;
                }
                adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
            }

            if (adapterFactory instanceof ITypedDatasetAdapterFactory) {
                adapter = ((ITypedDatasetAdapterFactory) adapterFactory).createAdapter(wrapProperties(datasetDetails
                        .getProperties()));
                adapterOutputType = ((ITypedDatasourceAdapter) adapter).getAdapterOutputType();
            } else if (adapterFactory instanceof IGenericDatasetAdapterFactory) {
                String outputTypeName = datasetDetails.getProperties().get(IGenericDatasetAdapterFactory.KEY_TYPE_NAME);
                adapterOutputType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getDataverseName(),
                        outputTypeName).getDatatype();
                adapter = ((IGenericDatasetAdapterFactory) adapterFactory).createAdapter(
                        wrapProperties(datasetDetails.getProperties()), adapterOutputType);
            } else {
                throw new IllegalStateException(" Unknown factory type for " + adapterFactoryClassname);
            }
        } catch (AlgebricksException ae) {
            throw ae;
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException("unable to create adapter  " + e);
        }

        ISerializerDeserializer payloadSerde = NonTaggedDataFormat.INSTANCE.getSerdeProvider()
                .getSerializerDeserializer(adapterOutputType);
        RecordDescriptor feedDesc = new RecordDescriptor(new ISerializerDeserializer[] { payloadSerde });

        FeedIntakeOperatorDescriptor feedIngestor = new FeedIntakeOperatorDescriptor(jobSpec, new FeedId(
                dataset.getDataverseName(), dataset.getDatasetName()), adapterFactoryClassname,
                this.wrapPropertiesEmpty(datasetDetails.getProperties()), (ARecordType) adapterOutputType, feedDesc,
                adapterFactory);

        AlgebricksPartitionConstraint constraint = null;
        try {
            constraint = adapter.getPartitionConstraint();
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(feedIngestor, constraint);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildFeedMessengerRuntime(
            AqlMetadataProvider metadataProvider, JobSpecification jobSpec, FeedDatasetDetails datasetDetails,
            String dataverse, String dataset, List<IFeedMessage> feedMessages) throws AlgebricksException {
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc = metadataProvider
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(dataverse, dataset, dataset);
        FeedMessageOperatorDescriptor feedMessenger = new FeedMessageOperatorDescriptor(jobSpec, dataverse, dataset,
                feedMessages);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(feedMessenger, spPc.second);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildBtreeRuntime(JobSpecification jobSpec,
            List<LogicalVariable> outputVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            JobGenContext context, boolean retainInput, Dataset dataset, String indexName, int[] lowKeyFields,
            int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive, Object implConfig)
            throws AlgebricksException {
        boolean isSecondary = true;
        try {
            Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), dataset.getDatasetName());
            if (primaryIndex != null) {
                isSecondary = !indexName.equals(primaryIndex.getIndexName());
            }
            int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
            RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
            int numKeys = numPrimaryKeys;
            int keysStartIndex = outputRecDesc.getFieldCount() - numKeys - 1;
            ITypeTraits[] typeTraits = null;
            int[] bloomFilterKeyFields;
            if (isSecondary) {
                Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                        dataset.getDatasetName(), indexName);
                int numSecondaryKeys = secondaryIndex.getKeyFieldNames().size();
                numKeys += numSecondaryKeys;
                keysStartIndex = outputVars.size() - numKeys;
                typeTraits = JobGenHelper.variablesToTypeTraits(outputVars, keysStartIndex, numKeys, typeEnv, context);
                bloomFilterKeyFields = new int[numSecondaryKeys];
                for (int i = 0; i < numSecondaryKeys; i++) {
                    bloomFilterKeyFields[i] = i;
                }
            } else {
                typeTraits = JobGenHelper.variablesToTypeTraits(outputVars, keysStartIndex, numKeys + 1, typeEnv,
                        context);
                bloomFilterKeyFields = new int[numPrimaryKeys];
                for (int i = 0; i < numPrimaryKeys; i++) {
                    bloomFilterKeyFields[i] = i;
                }
            }
            IBinaryComparatorFactory[] comparatorFactories = JobGenHelper.variablesToAscBinaryComparatorFactories(
                    outputVars, keysStartIndex, numKeys, typeEnv, context);

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc;
            try {
                spPc = splitProviderAndPartitionConstraintsForInternalOrFeedDataset(dataset.getDataverseName(),
                        dataset.getDatasetName(), indexName);
            } catch (Exception e) {
                throw new AlgebricksException(e);
            }

            ISearchOperationCallbackFactory searchCallbackFactory = null;
            if (isSecondary) {
                searchCallbackFactory = new SecondaryIndexSearchOperationCallbackFactory();
            } else {
                JobId jobId = ((JobEventListenerFactory) jobSpec.getJobletEventListenerFactory()).getJobId();
                int datasetId = dataset.getDatasetId();
                int[] primaryKeyFields = new int[numPrimaryKeys];
                for (int i = 0; i < numPrimaryKeys; i++) {
                    primaryKeyFields[i] = i;
                }

                AqlMetadataImplConfig aqlMetadataImplConfig = (AqlMetadataImplConfig) implConfig;
                ITransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
                if (aqlMetadataImplConfig != null && aqlMetadataImplConfig.isInstantLock()) {
                    searchCallbackFactory = new PrimaryIndexInstantSearchOperationCallbackFactory(jobId, datasetId,
                            primaryKeyFields, txnSubsystemProvider, ResourceType.LSM_BTREE);
                } else {
                    searchCallbackFactory = new PrimaryIndexSearchOperationCallbackFactory(jobId, datasetId,
                            primaryKeyFields, txnSubsystemProvider, ResourceType.LSM_BTREE);
                }
            }
            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            AsterixRuntimeComponentsProvider rtcProvider = AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER;
            BTreeSearchOperatorDescriptor btreeSearchOp = new BTreeSearchOperatorDescriptor(jobSpec, outputRecDesc,
                    appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(), spPc.first,
                    typeTraits, comparatorFactories, bloomFilterKeyFields, lowKeyFields, highKeyFields,
                    lowKeyInclusive, highKeyInclusive, new LSMBTreeDataflowHelperFactory(
                            new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                            compactionInfo.second, isSecondary ? new SecondaryIndexOperationTrackerProvider(
                                    dataset.getDatasetId()) : new PrimaryIndexOperationTrackerProvider(
                                    dataset.getDatasetId()), rtcProvider, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                            storageProperties.getBloomFilterFalsePositiveRate()), retainInput, searchCallbackFactory);

            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeSearchOp, spPc.second);

        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildRtreeRuntime(JobSpecification jobSpec,
            List<LogicalVariable> outputVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            JobGenContext context, boolean retainInput, Dataset dataset, String indexName, int[] keyFields)
            throws AlgebricksException {
        try {
            ARecordType recType = (ARecordType) findType(dataset.getDataverseName(), dataset.getItemTypeName());
            int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();

            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
            if (secondaryIndex == null) {
                throw new AlgebricksException("Code generation error: no index " + indexName + " for dataset "
                        + dataset.getDatasetName());
            }
            List<String> secondaryKeyFields = secondaryIndex.getKeyFieldNames();
            int numSecondaryKeys = secondaryKeyFields.size();
            if (numSecondaryKeys != 1) {
                throw new AlgebricksException(
                        "Cannot use "
                                + numSecondaryKeys
                                + " fields as a key for the R-tree index. There can be only one field as a key for the R-tree index.");
            }
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableKeyFieldType(secondaryKeyFields.get(0), recType);
            IAType keyType = keyTypePair.first;
            if (keyType == null) {
                throw new AlgebricksException("Could not find field " + secondaryKeyFields.get(0) + " in the schema.");
            }
            int numDimensions = NonTaggedFormatUtil.getNumDimensions(keyType.getTypeTag());
            int numNestedSecondaryKeyFields = numDimensions * 2;
            IPrimitiveValueProviderFactory[] valueProviderFactories = new IPrimitiveValueProviderFactory[numNestedSecondaryKeyFields];
            for (int i = 0; i < numNestedSecondaryKeyFields; i++) {
                valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
            }

            RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
            int keysStartIndex = outputRecDesc.getFieldCount() - numNestedSecondaryKeyFields - numPrimaryKeys;
            if (retainInput) {
                keysStartIndex -= numNestedSecondaryKeyFields;
            }
            IBinaryComparatorFactory[] comparatorFactories = JobGenHelper.variablesToAscBinaryComparatorFactories(
                    outputVars, keysStartIndex, numNestedSecondaryKeyFields, typeEnv, context);
            ITypeTraits[] typeTraits = JobGenHelper.variablesToTypeTraits(outputVars, keysStartIndex,
                    numNestedSecondaryKeyFields, typeEnv, context);
            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc = splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
                    dataset.getDataverseName(), dataset.getDatasetName(), indexName);

            IBinaryComparatorFactory[] primaryComparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(
                    dataset, recType, context.getBinaryComparatorFactoryProvider());
            IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());
            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            ISearchOperationCallbackFactory searchCallbackFactory = new SecondaryIndexSearchOperationCallbackFactory();
            RTreeSearchOperatorDescriptor rtreeSearchOp = new RTreeSearchOperatorDescriptor(jobSpec, outputRecDesc,
                    appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(), spPc.first,
                    typeTraits, comparatorFactories, keyFields, new LSMRTreeDataflowHelperFactory(
                            valueProviderFactories, RTreePolicyType.RTREE, primaryComparatorFactories,
                            new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                            compactionInfo.second, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMRTreeIOOperationCallbackFactory.INSTANCE, proposeLinearizer(nestedKeyType.getTypeTag(),
                                    comparatorFactories.length), storageProperties.getBloomFilterFalsePositiveRate()),
                    retainInput, searchCallbackFactory);
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(rtreeSearchOp, spPc.second);

        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    @Override
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc) {
        FileSplitDataSink fsds = (FileSplitDataSink) sink;
        FileSplitSinkId fssi = (FileSplitSinkId) fsds.getId();
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
        ResultSetSinkId rssId = (ResultSetSinkId) rsds.getId();
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
        Dataset dataset = ads.getDataset();
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("No index for external dataset " + dataSourceId);
        }
        try {
            String indexName = (String) indexId;
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
        Dataset dataset = findDataset(aqlId.getDataverseName(), aqlId.getDatasetName());
        if (dataset == null) {
            throw new AlgebricksException("Datasource with id " + aqlId + " was not found.");
        }
        String tName = dataset.getItemTypeName();
        IAType itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, aqlId.getDataverseName(), tName).getDatatype();
        return new AqlDataSource(aqlId, dataset, itemType);
    }

    @Override
    public boolean scannerOperatorIsLeaf(IDataSource<AqlSourceId> dataSource) {
        AqlSourceId asid = dataSource.getId();
        String dataverseName = asid.getDataverseName();
        String datasetName = asid.getDatasetName();
        Dataset dataset = null;
        try {
            dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
        } catch (MetadataException e) {
            throw new IllegalStateException(e);
        }

        if (dataset == null) {
            throw new IllegalArgumentException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }
        return dataset.getDatasetType() == DatasetType.EXTERNAL;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payload, JobGenContext context, JobSpecification spec) throws AlgebricksException {
        String dataverseName = dataSource.getId().getDataverseName();
        String datasetName = dataSource.getId().getDatasetName();
        int numKeys = keys.size();
        // move key fields to front
        int[] fieldPermutation = new int[numKeys + 1];
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

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }

        try {
            Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), dataset.getDatasetName());
            String indexName = primaryIndex.getIndexName();

            String itemTypeName = dataset.getItemTypeName();
            ARecordType itemType = (ARecordType) MetadataManager.INSTANCE.getDatatype(mdTxnCtx,
                    dataset.getDataverseName(), itemTypeName).getDatatype();
            ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType);
            IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                    itemType, context.getBinaryComparatorFactoryProvider());

            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
                    dataSource.getId().getDataverseName(), datasetName, indexName);
            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();

            long numElementsHint = getCardinalityPerPartitionHint(dataset);

            // TODO
            // figure out the right behavior of the bulkload and then give the
            // right callback
            // (ex. what's the expected behavior when there is an error during
            // bulkload?)
            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            TreeIndexBulkLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                    appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                    splitsAndConstraint.first, typeTraits, comparatorFactories, bloomFilterKeyFields, fieldPermutation,
                    GlobalConfig.DEFAULT_BTREE_FILL_FACTOR, false, numElementsHint, true,
                    new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                            compactionInfo.first, compactionInfo.second, new PrimaryIndexOperationTrackerProvider(
                                    dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE,
                            storageProperties.getBloomFilterFalsePositiveRate()), NoOpOperationCallbackFactory.INSTANCE);
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeBulkLoad,
                    splitsAndConstraint.second);
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertOrDeleteRuntime(IndexOperation indexOp,
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payload, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        String datasetName = dataSource.getId().getDatasetName();
        int numKeys = keys.size();
        // Move key fields to front.
        int[] fieldPermutation = new int[numKeys + 1];
        int[] bloomFilterKeyFields = new int[numKeys];
        int i = 0;
        for (LogicalVariable varKey : keys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            bloomFilterKeyFields[i] = i;
            i++;
        }
        fieldPermutation[numKeys] = propagatedSchema.findVariable(payload);

        Dataset dataset = findDataset(dataSource.getId().getDataverseName(), datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse "
                    + dataSource.getId().getDataverseName());
        }
        try {
            Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), dataset.getDatasetName());
            String indexName = primaryIndex.getIndexName();

            String itemTypeName = dataset.getItemTypeName();
            ARecordType itemType = (ARecordType) MetadataManager.INSTANCE.getDatatype(mdTxnCtx,
                    dataSource.getId().getDataverseName(), itemTypeName).getDatatype();

            ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType);

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                    itemType, context.getBinaryComparatorFactoryProvider());
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
                    dataSource.getId().getDataverseName(), datasetName, indexName);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            int[] primaryKeyFields = new int[numKeys];
            for (i = 0; i < numKeys; i++) {
                primaryKeyFields[i] = i;
            }
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            PrimaryIndexModificationOperationCallbackFactory modificationCallbackFactory = new PrimaryIndexModificationOperationCallbackFactory(
                    jobId, datasetId, primaryKeyFields, txnSubsystemProvider, indexOp, ResourceType.LSM_BTREE);

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            AsterixLSMTreeInsertDeleteOperatorDescriptor insertDeleteOp = new AsterixLSMTreeInsertDeleteOperatorDescriptor(
                    spec, recordDesc, appContext.getStorageManagerInterface(),
                    appContext.getIndexLifecycleManagerProvider(), splitsAndConstraint.first, typeTraits,
                    comparatorFactories, bloomFilterKeyFields, fieldPermutation, indexOp,
                    new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(datasetId),
                            compactionInfo.first, compactionInfo.second, new PrimaryIndexOperationTrackerProvider(
                                    dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE, storageProperties
                                    .getBloomFilterFalsePositiveRate()), null, modificationCallbackFactory, true);

            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(insertDeleteOp,
                    splitsAndConstraint.second);

        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payload, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        return getInsertOrDeleteRuntime(IndexOperation.INSERT, dataSource, propagatedSchema, typeEnv, keys, payload,
                recordDesc, context, spec);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payload, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        return getInsertOrDeleteRuntime(IndexOperation.DELETE, dataSource, propagatedSchema, typeEnv, keys, payload,
                recordDesc, context, spec);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertOrDeleteRuntime(
            IndexOperation indexOp, IDataSourceIndex<String, AqlSourceId> dataSourceIndex,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec) throws AlgebricksException {
        String indexName = dataSourceIndex.getId();
        String dataverseName = dataSourceIndex.getDataSource().getId().getDataverseName();
        String datasetName = dataSourceIndex.getDataSource().getId().getDatasetName();

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
                return getBTreeDmlRuntime(dataverseName, datasetName, indexName, propagatedSchema, typeEnv,
                        primaryKeys, secondaryKeys, filterFactory, recordDesc, context, spec, indexOp);
            }
            case RTREE: {
                return getRTreeDmlRuntime(dataverseName, datasetName, indexName, propagatedSchema, typeEnv,
                        primaryKeys, secondaryKeys, filterFactory, recordDesc, context, spec, indexOp);
            }
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX: {
                return getInvertedIndexDmlRuntime(dataverseName, datasetName, indexName, propagatedSchema, typeEnv,
                        primaryKeys, secondaryKeys, filterFactory, recordDesc, context, spec, indexOp,
                        secondaryIndex.getIndexType());
            }
            default: {
                throw new AlgebricksException("Insert and delete not implemented for index type: "
                        + secondaryIndex.getIndexType());
            }
        }
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<String, AqlSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec) throws AlgebricksException {
        return getIndexInsertOrDeleteRuntime(IndexOperation.INSERT, dataSourceIndex, propagatedSchema, inputSchemas,
                typeEnv, primaryKeys, secondaryKeys, filterExpr, recordDesc, context, spec);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<String, AqlSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec) throws AlgebricksException {
        return getIndexInsertOrDeleteRuntime(IndexOperation.DELETE, dataSourceIndex, propagatedSchema, inputSchemas,
                typeEnv, primaryKeys, secondaryKeys, filterExpr, recordDesc, context, spec);
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
            String datasetName, String indexName, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            AsterixTupleFilterFactory filterFactory, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp) throws AlgebricksException {

        int numKeys = primaryKeys.size() + secondaryKeys.size();
        // generate field permutations
        int[] fieldPermutation = new int[numKeys];
        int[] bloomFilterKeyFields = new int[secondaryKeys.size()];
        int i = 0;
        for (LogicalVariable varKey : secondaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            bloomFilterKeyFields[i] = i;
            i++;
        }
        for (LogicalVariable varKey : primaryKeys) {
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
            itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getDataverseName(), itemTypeName)
                    .getDatatype();

            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be indexed.");
            }

            ARecordType recType = (ARecordType) itemType;

            // Index parameters.
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);

            List<String> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();
            ITypeTraits[] typeTraits = new ITypeTraits[numKeys];
            IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numKeys];
            for (i = 0; i < secondaryKeys.size(); ++i) {
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(secondaryKeyExprs.get(i)
                        .toString(), recType);
                IAType keyType = keyPairType.first;
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                        keyType, true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            }
            List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            for (String partitioningKey : partitioningKeys) {
                IAType keyType = recType.getFieldType(partitioningKey);
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                        keyType, true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
                    dataverseName, datasetName, indexName);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            int[] primaryKeyFields = new int[primaryKeys.size()];
            i = 0;
            for (LogicalVariable varKey : primaryKeys) {
                int idx = propagatedSchema.findVariable(varKey);
                primaryKeyFields[i] = idx;
                i++;
            }
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            SecondaryIndexModificationOperationCallbackFactory modificationCallbackFactory = new SecondaryIndexModificationOperationCallbackFactory(
                    jobId, datasetId, primaryKeyFields, txnSubsystemProvider, indexOp, ResourceType.LSM_BTREE);

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            AsterixLSMTreeInsertDeleteOperatorDescriptor btreeBulkLoad = new AsterixLSMTreeInsertDeleteOperatorDescriptor(
                    spec, recordDesc, appContext.getStorageManagerInterface(),
                    appContext.getIndexLifecycleManagerProvider(), splitsAndConstraint.first, typeTraits,
                    comparatorFactories, bloomFilterKeyFields, fieldPermutation, indexOp,
                    new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(datasetId),
                            compactionInfo.first, compactionInfo.second, new SecondaryIndexOperationTrackerProvider(
                                    dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE, storageProperties
                                    .getBloomFilterFalsePositiveRate()), filterFactory, modificationCallbackFactory,
                    false);
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeBulkLoad,
                    splitsAndConstraint.second);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInvertedIndexDmlRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            AsterixTupleFilterFactory filterFactory, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp, IndexType indexType) throws AlgebricksException {

        // Sanity checks.
        if (primaryKeys.size() > 1) {
            throw new AlgebricksException("Cannot create inverted index on dataset with composite primary key.");
        }
        if (secondaryKeys.size() > 1) {
            throw new AlgebricksException("Cannot create composite inverted index on multiple fields.");
        }

        int numKeys = primaryKeys.size() + secondaryKeys.size();
        // generate field permutations
        int[] fieldPermutation = new int[numKeys];
        int i = 0;
        for (LogicalVariable varKey : secondaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            i++;
        }
        for (LogicalVariable varKey : primaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            i++;
        }

        boolean isPartitioned;
        if (indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
            isPartitioned = true;
        } else {
            isPartitioned = false;
        }

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
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

            List<String> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();

            int numTokenFields = (!isPartitioned) ? secondaryKeys.size() : secondaryKeys.size() + 1;
            ITypeTraits[] tokenTypeTraits = new ITypeTraits[numTokenFields];
            ITypeTraits[] invListsTypeTraits = new ITypeTraits[primaryKeys.size()];
            IBinaryComparatorFactory[] tokenComparatorFactories = new IBinaryComparatorFactory[numTokenFields];
            IBinaryComparatorFactory[] invListComparatorFactories = new IBinaryComparatorFactory[primaryKeys.size()];

            IAType secondaryKeyType = null;
            for (i = 0; i < secondaryKeys.size(); ++i) {
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(secondaryKeyExprs.get(i)
                        .toString(), recType);
                secondaryKeyType = keyPairType.first;
            }
            List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            i = 0;
            for (String partitioningKey : partitioningKeys) {
                IAType keyType = recType.getFieldType(partitioningKey);
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

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
                    dataverseName, datasetName, indexName);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            int[] primaryKeyFields = new int[primaryKeys.size()];
            i = 0;
            for (LogicalVariable varKey : primaryKeys) {
                int idx = propagatedSchema.findVariable(varKey);
                primaryKeyFields[i] = idx;
                i++;
            }
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            SecondaryIndexModificationOperationCallbackFactory modificationCallbackFactory = new SecondaryIndexModificationOperationCallbackFactory(
                    jobId, datasetId, primaryKeyFields, txnSubsystemProvider, indexOp, ResourceType.LSM_INVERTED_INDEX);

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor insertDeleteOp = new AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor(
                    spec, recordDesc, appContext.getStorageManagerInterface(), splitsAndConstraint.first,
                    appContext.getIndexLifecycleManagerProvider(), tokenTypeTraits, tokenComparatorFactories,
                    invListsTypeTraits, invListComparatorFactories, tokenizerFactory, fieldPermutation, indexOp,
                    new LSMInvertedIndexDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(datasetId),
                            compactionInfo.first, compactionInfo.second, new SecondaryIndexOperationTrackerProvider(
                                    dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMInvertedIndexIOOperationCallbackFactory.INSTANCE, storageProperties
                                    .getBloomFilterFalsePositiveRate()), filterFactory, modificationCallbackFactory);
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(insertDeleteOp,
                    splitsAndConstraint.second);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getRTreeDmlRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            AsterixTupleFilterFactory filterFactory, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp) throws AlgebricksException {
        try {
            Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            String itemTypeName = dataset.getItemTypeName();
            IAType itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, itemTypeName).getDatatype();
            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be indexed.");
            }
            ARecordType recType = (ARecordType) itemType;
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
            List<String> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(secondaryKeyExprs.get(0), recType);
            IAType spatialType = keyPairType.first;
            int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
            int numSecondaryKeys = dimension * 2;
            int numPrimaryKeys = primaryKeys.size();
            int numKeys = numSecondaryKeys + numPrimaryKeys;
            ITypeTraits[] typeTraits = new ITypeTraits[numKeys];
            IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numKeys];
            int[] fieldPermutation = new int[numKeys];
            int i = 0;

            for (LogicalVariable varKey : secondaryKeys) {
                int idx = propagatedSchema.findVariable(varKey);
                fieldPermutation[i] = idx;
                i++;
            }
            for (LogicalVariable varKey : primaryKeys) {
                int idx = propagatedSchema.findVariable(varKey);
                fieldPermutation[i] = idx;
                i++;
            }
            IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(spatialType.getTypeTag());
            IPrimitiveValueProviderFactory[] valueProviderFactories = new IPrimitiveValueProviderFactory[numSecondaryKeys];
            for (i = 0; i < numSecondaryKeys; i++) {
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                        nestedKeyType, true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(nestedKeyType);
                valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
            }
            List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            for (String partitioningKey : partitioningKeys) {
                IAType keyType = recType.getFieldType(partitioningKey);
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                        keyType, true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }

            IBinaryComparatorFactory[] primaryComparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(
                    dataset, recType, context.getBinaryComparatorFactoryProvider());
            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
                    dataverseName, datasetName, indexName);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            int[] primaryKeyFields = new int[numPrimaryKeys];
            i = 0;
            for (LogicalVariable varKey : primaryKeys) {
                int idx = propagatedSchema.findVariable(varKey);
                primaryKeyFields[i] = idx;
                i++;
            }
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            SecondaryIndexModificationOperationCallbackFactory modificationCallbackFactory = new SecondaryIndexModificationOperationCallbackFactory(
                    jobId, datasetId, primaryKeyFields, txnSubsystemProvider, indexOp, ResourceType.LSM_RTREE);

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            AsterixLSMTreeInsertDeleteOperatorDescriptor rtreeUpdate = new AsterixLSMTreeInsertDeleteOperatorDescriptor(
                    spec, recordDesc, appContext.getStorageManagerInterface(),
                    appContext.getIndexLifecycleManagerProvider(), splitsAndConstraint.first, typeTraits,
                    comparatorFactories, null, fieldPermutation, indexOp, new LSMRTreeDataflowHelperFactory(
                            valueProviderFactories, RTreePolicyType.RTREE, primaryComparatorFactories,
                            new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                            compactionInfo.second, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMRTreeIOOperationCallbackFactory.INSTANCE, proposeLinearizer(nestedKeyType.getTypeTag(),
                                    comparatorFactories.length), storageProperties.getBloomFilterFalsePositiveRate()),
                    filterFactory, modificationCallbackFactory, false);
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(rtreeUpdate, splitsAndConstraint.second);
        } catch (MetadataException | IOException e) {
            throw new AlgebricksException(e);
        }
    }

    public JobId getJobId() {
        return jobId;
    }

    public static ITreeIndexFrameFactory createBTreeNSMInteriorFrameFactory(ITypeTraits[] typeTraits) {
        return new BTreeNSMInteriorFrameFactory(new TypeAwareTupleWriterFactory(typeTraits));
    }

    public static ILinearizeComparatorFactory proposeLinearizer(ATypeTag keyType, int numKeyFields)
            throws AlgebricksException {
        if (numKeyFields / 2 == 2 && (keyType == ATypeTag.DOUBLE)) {
            return new HilbertDoubleComparatorFactory(2);
        } else if (keyType == ATypeTag.DOUBLE) {
            return new ZCurveDoubleComparatorFactory(numKeyFields / 2);
        } else if (keyType == ATypeTag.INT8 || keyType == ATypeTag.INT16 || keyType == ATypeTag.INT32
                || keyType == ATypeTag.INT64) {
            return new ZCurveIntComparatorFactory(numKeyFields / 2);
        } else {
            throw new AlgebricksException("Cannot propose linearizer for key with type " + keyType + ".");
        }
    }

    /**
     * Calculate an estimate size of the bloom filter. Note that this is an estimation which assumes that the data
     * is going to be uniformly distributed across all partitions.
     * 
     * @param dataset
     * @return Number of elements that will be used to create a bloom filter per dataset per partition
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
        InternalDatasetDetails datasetDetails = (InternalDatasetDetails) dataset.getDatasetDetails();
        List<String> nodeGroup = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, datasetDetails.getNodeGroupName())
                .getNodeNames();
        for (String nd : nodeGroup) {
            numPartitions += AsterixClusterProperties.INSTANCE.getNumberOfIODevices(nd);
        }
        return numElementsHint /= numPartitions;
    }

    @Override
    public IFunctionInfo lookupFunction(FunctionIdentifier fid) {
        return AsterixBuiltinFunctions.lookupFunction(fid);
    }

    public Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
            String dataverseName, String datasetName, String targetIdxName) throws AlgebricksException {
        FileSplit[] splits = splitsForInternalOrFeedDataset(mdTxnCtx, dataverseName, datasetName, targetIdxName);
        return splitProviderAndPartitionConstraints(splits);
    }

    public Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraintsForDataverse(
            String dataverse) {
        FileSplit[] splits = splitsForDataverse(mdTxnCtx, dataverse);
        return splitProviderAndPartitionConstraints(splits);
    }

    private Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraints(
            FileSplit[] splits) {
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(splits);
        String[] loc = new String[splits.length];
        for (int p = 0; p < splits.length; p++) {
            loc[p] = splits[p].getNodeName();
        }
        AlgebricksPartitionConstraint pc = new AlgebricksAbsolutePartitionConstraint(loc);
        return new Pair<IFileSplitProvider, AlgebricksPartitionConstraint>(splitProvider, pc);
    }

    private FileSplit[] splitsForDataverse(MetadataTransactionContext mdTxnCtx, String dataverseName) {
        File relPathFile = new File(dataverseName);
        List<FileSplit> splits = new ArrayList<FileSplit>();
        for (Map.Entry<String, String[]> entry : stores.entrySet()) {
            String node = entry.getKey();
            String[] nodeStores = entry.getValue();
            if (nodeStores == null) {
                continue;
            }
            for (int i = 0; i < nodeStores.length; i++) {
                int numIODevices = AsterixClusterProperties.INSTANCE.getNumberOfIODevices(node);
                String[] ioDevices = AsterixClusterProperties.INSTANCE.getIODevices(node);
                for (int j = 0; j < nodeStores.length; j++) {
                    for (int k = 0; k < numIODevices; k++) {
                        File f = new File(ioDevices[k] + File.separator + nodeStores[j] + File.separator + relPathFile);
                        splits.add(new FileSplit(node, new FileReference(f), k));
                    }
                }
            }
        }
        return splits.toArray(new FileSplit[] {});
    }

    private FileSplit[] splitsForInternalOrFeedDataset(MetadataTransactionContext mdTxnCtx, String dataverseName,
            String datasetName, String targetIdxName) throws AlgebricksException {

        try {
            File relPathFile = new File(getRelativePath(dataverseName, datasetName + "_idx_" + targetIdxName));
            Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            if (dataset.getDatasetType() != DatasetType.INTERNAL & dataset.getDatasetType() != DatasetType.FEED) {
                throw new AlgebricksException("Not an internal or feed dataset");
            }
            InternalDatasetDetails datasetDetails = (InternalDatasetDetails) dataset.getDatasetDetails();
            List<String> nodeGroup = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, datasetDetails.getNodeGroupName())
                    .getNodeNames();
            if (nodeGroup == null) {
                throw new AlgebricksException("Couldn't find node group " + datasetDetails.getNodeGroupName());
            }

            List<FileSplit> splitArray = new ArrayList<FileSplit>();
            for (String nd : nodeGroup) {
                String[] nodeStores = stores.get(nd);
                if (nodeStores == null) {
                    LOGGER.warning("Node " + nd + " has no stores.");
                    throw new AlgebricksException("Node " + nd + " has no stores.");
                } else {
                    int numIODevices;
                    if (datasetDetails.getNodeGroupName().compareTo(MetadataConstants.METADATA_NODEGROUP_NAME) == 0) {
                        numIODevices = 1;
                    } else {
                        numIODevices = AsterixClusterProperties.INSTANCE.getNumberOfIODevices(nd);
                    }
                    String[] ioDevices = AsterixClusterProperties.INSTANCE.getIODevices(nd);
                    for (int j = 0; j < nodeStores.length; j++) {
                        for (int k = 0; k < numIODevices; k++) {
                            File f = new File(ioDevices[k] + File.separator + nodeStores[j] + File.separator
                                    + relPathFile);
                            splitArray.add(new FileSplit(nd, new FileReference(f), k));
                        }
                    }
                }
            }
            FileSplit[] splits = new FileSplit[splitArray.size()];
            int i = 0;
            for (FileSplit fs : splitArray) {
                splits[i++] = fs;
            }
            return splits;
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    private static Map<String, String> initializeAdapterFactoryMapping() {
        Map<String, String> adapterFactoryMapping = new HashMap<String, String>();
        adapterFactoryMapping.put("edu.uci.ics.asterix.external.dataset.adapter.NCFileSystemAdapter",
                "edu.uci.ics.asterix.external.adapter.factory.NCFileSystemAdapterFactory");
        adapterFactoryMapping.put("edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter",
                "edu.uci.ics.asterix.external.adapter.factory.HDFSAdapterFactory");
        adapterFactoryMapping.put("edu.uci.ics.asterix.external.dataset.adapter.PullBasedTwitterAdapter",
                "edu.uci.ics.asterix.external.dataset.adapter.PullBasedTwitterAdapterFactory");
        adapterFactoryMapping.put("edu.uci.ics.asterix.external.dataset.adapter.RSSFeedAdapter",
                "edu.uci.ics.asterix.external.dataset.adapter..RSSFeedAdapterFactory");
        adapterFactoryMapping.put("edu.uci.ics.asterix.external.dataset.adapter.CNNFeedAdapter",
                "edu.uci.ics.asterix.external.dataset.adapter.CNNFeedAdapterFactory");
        return adapterFactoryMapping;
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

    private static String getRelativePath(String dataverseName, String fileName) {
        return dataverseName + File.separator + fileName;
    }

    public Dataset findDataset(String dataverse, String dataset) throws AlgebricksException {
        try {
            return MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverse, dataset);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public IAType findType(String dataverse, String typeName) {
        Datatype type;
        try {
            type = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverse, typeName);
        } catch (Exception e) {
            throw new IllegalStateException();
        }
        if (type == null) {
            throw new IllegalStateException();
        }
        return type.getDatatype();
    }

    public List<Index> getDatasetIndexes(String dataverseName, String datasetName) throws AlgebricksException {
        try {
            return MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public AlgebricksPartitionConstraint getClusterLocations() {
        ArrayList<String> locs = new ArrayList<String>();
        for (String i : stores.keySet()) {
            String[] nodeStores = stores.get(i);
            int numIODevices = AsterixClusterProperties.INSTANCE.getNumberOfIODevices(i);
            for (int j = 0; j < nodeStores.length; j++) {
                for (int k = 0; k < numIODevices; k++) {
                    locs.add(i);
                }
            }
        }
        String[] cluster = new String[locs.size()];
        cluster = locs.toArray(cluster);
        return new AlgebricksAbsolutePartitionConstraint(cluster);
    }

    public IDataFormat getFormat() {
        return FormatUtils.getDefaultFormat();
    }

    /**
     * Add HDFS scheduler and the cluster location constraint into the scheduler
     * 
     * @param properties
     *            the original dataset properties
     * @return a new map containing the original dataset properties and the scheduler/locations
     */
    private Map<String, Object> wrapProperties(Map<String, String> properties) {
        Map<String, Object> wrappedProperties = new HashMap<String, Object>();
        wrappedProperties.putAll(properties);
        wrappedProperties.put(HDFSAdapterFactory.SCHEDULER, hdfsScheduler);
        wrappedProperties.put(HDFSAdapterFactory.CLUSTER_LOCATIONS, getClusterLocations());
        return wrappedProperties;
    }

    /**
     * Adapt the original properties to a string-object map
     * 
     * @param properties
     *            the original properties
     * @return the new stirng-object map
     */
    private Map<String, Object> wrapPropertiesEmpty(Map<String, String> properties) {
        Map<String, Object> wrappedProperties = new HashMap<String, Object>();
        wrappedProperties.putAll(properties);
        return wrappedProperties;
    }

}
