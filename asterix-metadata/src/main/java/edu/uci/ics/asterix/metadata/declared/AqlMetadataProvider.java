/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.List;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.dataflow.IAsterixApplicationContextInfo;
import edu.uci.ics.asterix.common.parse.IParseFileSplitsDecl;
import edu.uci.ics.asterix.dataflow.data.nontagged.valueproviders.AqlPrimitiveValueProviderFactory;
import edu.uci.ics.asterix.external.data.adapter.api.IDatasourceAdapter;
import edu.uci.ics.asterix.external.data.adapter.api.IDatasourceReadAdapter;
import edu.uci.ics.asterix.external.data.operator.ExternalDataScanOperatorDescriptor;
import edu.uci.ics.asterix.feed.comm.IFeedMessage;
import edu.uci.ics.asterix.feed.mgmt.FeedId;
import edu.uci.ics.asterix.feed.operator.FeedIntakeOperatorDescriptor;
import edu.uci.ics.asterix.feed.operator.FeedMessageOperatorDescriptor;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.base.AsterixTupleFilterFactory;
import edu.uci.ics.asterix.runtime.transaction.TreeIndexInsertUpdateDeleteOperatorDescriptor;
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
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.SinkWriterRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeSearchOperatorDescriptor;

public class AqlMetadataProvider implements IMetadataProvider<AqlSourceId, String> {
    private final long txnId;
    private boolean isWriteTransaction;
    private final AqlCompiledMetadataDeclarations metadata;

    public AqlMetadataProvider(long txnId, boolean isWriteTransaction, AqlCompiledMetadataDeclarations metadata) {
        this.txnId = txnId;
        this.isWriteTransaction = isWriteTransaction;
        this.metadata = metadata;
    }

    @Override
    public AqlDataSource findDataSource(AqlSourceId id) throws AlgebricksException {
        AqlSourceId aqlId = (AqlSourceId) id;
        return lookupSourceInMetadata(metadata, aqlId);
    }

    public AqlCompiledMetadataDeclarations getMetadataDeclarations() {
        return metadata;
    }

    public boolean isWriteTransaction() {
        return isWriteTransaction;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(
            IDataSource<AqlSourceId> dataSource, List<LogicalVariable> scanVariables,
            List<LogicalVariable> projectVariables, boolean projectPushed, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec)
            throws AlgebricksException {
        Dataset dataset = metadata.findDataset(dataSource.getId().getDatasetName());
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + dataSource.getId().getDatasetName());
        }
        switch (dataset.getDatasetType()) {
            case FEED:
                if (dataSource instanceof ExternalFeedDataSource) {
                    return buildExternalDatasetScan(jobSpec, dataset, dataSource);
                } else {
                    return buildInternalDatasetScan(jobSpec, scanVariables, opSchema, typeEnv, dataset, dataSource,
                            context);
                }
            case INTERNAL: {
                return buildInternalDatasetScan(jobSpec, scanVariables, opSchema, typeEnv, dataset, dataSource, context);
            }
            case EXTERNAL: {
                return buildExternalDatasetScan(jobSpec, dataset, dataSource);
            }
            default: {
                throw new IllegalArgumentException();
            }
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildInternalDatasetScan(JobSpecification jobSpec,
            List<LogicalVariable> outputVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            Dataset dataset, IDataSource<AqlSourceId> dataSource, JobGenContext context) throws AlgebricksException {
        AqlSourceId asid = dataSource.getId();
        String dataverseName = asid.getDataverseName();
        String datasetName = asid.getDatasetName();
        Index primaryIndex = metadata.getDatasetPrimaryIndex(dataverseName, datasetName);
        return buildBtreeRuntime(jobSpec, outputVars, opSchema, typeEnv, metadata, context, false, datasetName,
                dataset, primaryIndex.getIndexName(), null, null, true, true);
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDatasetScan(JobSpecification jobSpec,
            Dataset dataset, IDataSource<AqlSourceId> dataSource) throws AlgebricksException {
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType = metadata.findType(itemTypeName);
        if (dataSource instanceof ExternalFeedDataSource) {
            FeedDatasetDetails datasetDetails = (FeedDatasetDetails) dataset.getDatasetDetails();
            return buildFeedIntakeRuntime(jobSpec, metadata.getDataverseName(), dataset.getDatasetName(), itemType,
                    datasetDetails, metadata.getFormat());
        } else {
            return buildExternalDataScannerRuntime(jobSpec, itemType,
                    (ExternalDatasetDetails) dataset.getDatasetDetails(), metadata.getFormat());
        }
    }

    @SuppressWarnings("rawtypes")
    public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDataScannerRuntime(
            JobSpecification jobSpec, IAType itemType, ExternalDatasetDetails datasetDetails, IDataFormat format)
            throws AlgebricksException {
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Can only scan datasets of records.");
        }

        IDatasourceReadAdapter adapter;
        try {
            adapter = (IDatasourceReadAdapter) Class.forName(datasetDetails.getAdapter()).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException("unable to load the adapter class " + e);
        }

        if (!(adapter.getAdapterType().equals(IDatasourceAdapter.AdapterType.READ) || adapter.getAdapterType().equals(
                IDatasourceAdapter.AdapterType.READ_WRITE))) {
            throw new AlgebricksException("external dataset adapter does not support read operation");
        }
        ARecordType rt = (ARecordType) itemType;

        try {
            adapter.configure(datasetDetails.getProperties(), itemType);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException("unable to configure the datasource adapter " + e);
        }

        ISerializerDeserializer payloadSerde = format.getSerdeProvider().getSerializerDeserializer(itemType);
        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] { payloadSerde });

        ExternalDataScanOperatorDescriptor dataScanner = new ExternalDataScanOperatorDescriptor(jobSpec,
                datasetDetails.getAdapter(), datasetDetails.getProperties(), rt, scannerDesc);
        dataScanner.setDatasourceAdapter(adapter);
        AlgebricksPartitionConstraint constraint = adapter.getPartitionConstraint();
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(dataScanner, constraint);
    }

    @SuppressWarnings("rawtypes")
    public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildScannerRuntime(
            JobSpecification jobSpec, IAType itemType, IParseFileSplitsDecl decl, IDataFormat format)
            throws AlgebricksException {
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
    public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildFeedIntakeRuntime(
            JobSpecification jobSpec, String dataverse, String dataset, IAType itemType,
            FeedDatasetDetails datasetDetails, IDataFormat format) throws AlgebricksException {
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Can only consume records.");
        }
        IDatasourceAdapter adapter;
        try {
            adapter = (IDatasourceAdapter) Class.forName(datasetDetails.getAdapter()).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException("unable to load the adapter class " + e);
        }

        ARecordType rt = (ARecordType) itemType;
        try {
            adapter.configure(datasetDetails.getProperties(), itemType);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException("unable to configure the datasource adapter " + e);
        }

        ISerializerDeserializer payloadSerde = format.getSerdeProvider().getSerializerDeserializer(itemType);
        RecordDescriptor feedDesc = new RecordDescriptor(new ISerializerDeserializer[] { payloadSerde });

        FeedIntakeOperatorDescriptor feedIngestor = new FeedIntakeOperatorDescriptor(jobSpec, new FeedId(dataverse,
                dataset), datasetDetails.getAdapter(), datasetDetails.getProperties(), rt, feedDesc);

        AlgebricksPartitionConstraint constraint = adapter.getPartitionConstraint();
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(feedIngestor, constraint);
    }

    public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildFeedMessengerRuntime(
            JobSpecification jobSpec, AqlCompiledMetadataDeclarations metadata, FeedDatasetDetails datasetDetails,
            String dataverse, String dataset, List<IFeedMessage> feedMessages) throws AlgebricksException {
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(dataset, dataset);
        FeedMessageOperatorDescriptor feedMessenger = new FeedMessageOperatorDescriptor(jobSpec, dataverse, dataset,
                feedMessages);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(feedMessenger, spPc.second);
    }

    public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildBtreeRuntime(JobSpecification jobSpec,
            List<LogicalVariable> outputVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            AqlCompiledMetadataDeclarations metadata, JobGenContext context, boolean retainInput, String datasetName,
            Dataset dataset, String indexName, int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive,
            boolean highKeyInclusive) throws AlgebricksException {
        boolean isSecondary = true;
        Index primaryIndex = metadata.getDatasetPrimaryIndex(dataset.getDataverseName(), dataset.getDatasetName());
        if (primaryIndex != null) {
            isSecondary = !indexName.equals(primaryIndex.getIndexName());
        }
        int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
        int numKeys = numPrimaryKeys;
        int keysStartIndex = outputRecDesc.getFieldCount() - numKeys - 1;
        if (isSecondary) {
            Index secondaryIndex = metadata.getIndex(dataset.getDataverseName(), dataset.getDatasetName(), indexName);
            int numSecondaryKeys = secondaryIndex.getKeyFieldNames().size();
            numKeys += numSecondaryKeys;
            keysStartIndex = outputRecDesc.getFieldCount() - numKeys;
        }
        IBinaryComparatorFactory[] comparatorFactories = JobGenHelper.variablesToAscBinaryComparatorFactories(
                outputVars, keysStartIndex, numKeys, typeEnv, context);
        ITypeTraits[] typeTraits = JobGenHelper.variablesToTypeTraits(outputVars, keysStartIndex, numKeys, typeEnv,
                context);

        IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc;
        try {
            spPc = metadata.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, indexName);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
        BTreeSearchOperatorDescriptor btreeSearchOp = new BTreeSearchOperatorDescriptor(jobSpec, outputRecDesc,
                appContext.getStorageManagerInterface(), appContext.getIndexRegistryProvider(), spPc.first, typeTraits,
                comparatorFactories, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive,
                new BTreeDataflowHelperFactory(), retainInput, NoOpOperationCallbackProvider.INSTANCE);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeSearchOp, spPc.second);
    }

    @SuppressWarnings("rawtypes")
    public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildRtreeRuntime(
            AqlCompiledMetadataDeclarations metadata, JobGenContext context, JobSpecification jobSpec,
            String datasetName, Dataset dataset, String indexName, int[] keyFields) throws AlgebricksException {
        ARecordType recType = (ARecordType) metadata.findType(dataset.getItemTypeName());
        boolean isSecondary = true;
        Index primaryIndex = metadata.getDatasetPrimaryIndex(dataset.getDataverseName(), dataset.getDatasetName());
        if (primaryIndex != null) {
            isSecondary = !indexName.equals(primaryIndex.getIndexName());
        }

        int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        ISerializerDeserializer[] recordFields;
        IBinaryComparatorFactory[] comparatorFactories;
        ITypeTraits[] typeTraits;
        IPrimitiveValueProviderFactory[] valueProviderFactories;
        int numSecondaryKeys = 0;
        int numNestedSecondaryKeyFields = 0;
        int i = 0;
        if (!isSecondary) {
            throw new AlgebricksException("R-tree can only be used as a secondary index");
        }
        Index secondaryIndex = metadata.getIndex(dataset.getDataverseName(), dataset.getDatasetName(), indexName);
        if (secondaryIndex == null) {
            throw new AlgebricksException("Code generation error: no index " + indexName + " for dataset "
                    + datasetName);
        }
        List<String> secondaryKeyFields = secondaryIndex.getKeyFieldNames();
        numSecondaryKeys = secondaryKeyFields.size();
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
        int dimension = NonTaggedFormatUtil.getNumDimensions(keyType.getTypeTag());
        numNestedSecondaryKeyFields = dimension * 2;

        int numFields = numNestedSecondaryKeyFields + numPrimaryKeys;
        recordFields = new ISerializerDeserializer[numFields];
        typeTraits = new ITypeTraits[numFields];
        comparatorFactories = new IBinaryComparatorFactory[numNestedSecondaryKeyFields];
        valueProviderFactories = new IPrimitiveValueProviderFactory[numNestedSecondaryKeyFields];

        IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());
        for (i = 0; i < numNestedSecondaryKeyFields; i++) {
            ISerializerDeserializer keySerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(nestedKeyType);
            recordFields[i] = keySerde;
            comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                    nestedKeyType, true);
            typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(nestedKeyType);
            valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
        }

        List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        for (String partitioningKey : partitioningKeys) {
            IAType type = recType.getFieldType(partitioningKey);
            ISerializerDeserializer keySerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(type);
            recordFields[i] = keySerde;
            typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(type);
            ++i;
        }
        IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
        RecordDescriptor recDesc = new RecordDescriptor(recordFields);
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, indexName);
        RTreeSearchOperatorDescriptor rtreeSearchOp = new RTreeSearchOperatorDescriptor(jobSpec, recDesc,
                appContext.getStorageManagerInterface(), appContext.getIndexRegistryProvider(), spPc.first, typeTraits,
                comparatorFactories, keyFields, new RTreeDataflowHelperFactory(valueProviderFactories), false,
                NoOpOperationCallbackProvider.INSTANCE);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(rtreeSearchOp, spPc.second);
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
                metadata.getWriterFactory(), inputDesc);
        AlgebricksPartitionConstraint apc = new AlgebricksAbsolutePartitionConstraint(new String[] { nodeId });
        return new Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint>(runtime, apc);
    }

    @Override
    public IDataSourceIndex<String, AqlSourceId> findDataSourceIndex(String indexId, AqlSourceId dataSourceId)
            throws AlgebricksException {
        AqlDataSource ads = findDataSource(dataSourceId);
        Dataset dataset = ads.getDataset();
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("No index for external dataset " + dataSourceId);
        }

        String indexName = (String) indexId;
        Index secondaryIndex = metadata.getIndex(dataset.getDataverseName(), dataset.getDatasetName(), indexName);
        if (secondaryIndex != null) {
            return new AqlIndex(secondaryIndex, metadata, dataset.getDatasetName());
        } else {
            Index primaryIndex = metadata.getDatasetPrimaryIndex(dataset.getDataverseName(), dataset.getDatasetName());
            if (primaryIndex.getIndexName().equals(indexId)) {
                return new AqlIndex(primaryIndex, metadata, dataset.getDatasetName());
            } else {
                return null;
            }
        }
    }

    public static AqlDataSource lookupSourceInMetadata(AqlCompiledMetadataDeclarations metadata, AqlSourceId aqlId)
            throws AlgebricksException {
        if (!aqlId.getDataverseName().equals(metadata.getDataverseName())) {
            return null;
        }
        Dataset dataset = metadata.findDataset(aqlId.getDatasetName());
        if (dataset == null) {
            throw new AlgebricksException("Datasource with id " + aqlId + " was not found.");
        }
        String tName = dataset.getItemTypeName();
        IAType itemType = metadata.findType(tName);
        return new AqlDataSource(aqlId, dataset, itemType);
    }

    @Override
    public boolean scannerOperatorIsLeaf(IDataSource<AqlSourceId> dataSource) {
        AqlSourceId asid = dataSource.getId();
        String datasetName = asid.getDatasetName();
        Dataset dataset = null;
        try {
            dataset = metadata.findDataset(datasetName);
        } catch (AlgebricksException e) {
            throw new IllegalStateException(e);
        }
        if (dataset == null) {
            throw new IllegalArgumentException("Unknown dataset " + datasetName);
        }
        return dataset.getDatasetType() == DatasetType.EXTERNAL;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payload, JobGenContext context, JobSpecification spec) throws AlgebricksException {
        String datasetName = dataSource.getId().getDatasetName();
        int numKeys = keys.size();
        // move key fields to front
        int[] fieldPermutation = new int[numKeys + 1];
        // System.arraycopy(keys, 0, fieldPermutation, 0, numKeys);
        int i = 0;
        for (LogicalVariable varKey : keys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            i++;
        }
        fieldPermutation[numKeys] = propagatedSchema.findVariable(payload);

        Dataset dataset = metadata.findDataset(datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        Index primaryIndex = metadata.getDatasetPrimaryIndex(dataset.getDataverseName(), dataset.getDatasetName());
        String indexName = primaryIndex.getIndexName();

        String itemTypeName = dataset.getItemTypeName();
        ARecordType itemType = (ARecordType) metadata.findType(itemTypeName);

        ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType);
        IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                itemType, context.getBinaryComparatorFactoryProvider());

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, indexName);
        IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
        TreeIndexBulkLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                appContext.getStorageManagerInterface(), appContext.getIndexRegistryProvider(),
                splitsAndConstraint.first, typeTraits, comparatorFactories, fieldPermutation,
                GlobalConfig.DEFAULT_BTREE_FILL_FACTOR, new BTreeDataflowHelperFactory(),
                NoOpOperationCallbackProvider.INSTANCE);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeBulkLoad, splitsAndConstraint.second);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertOrDeleteRuntime(IndexOp indexOp,
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payload, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec)
            throws AlgebricksException {
        String datasetName = dataSource.getId().getDatasetName();
        int numKeys = keys.size();
        // Move key fields to front.
        int[] fieldPermutation = new int[numKeys + 1];
        int i = 0;
        for (LogicalVariable varKey : keys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            i++;
        }
        fieldPermutation[numKeys] = propagatedSchema.findVariable(payload);

        Dataset dataset = metadata.findDataset(datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        Index primaryIndex = metadata.getDatasetPrimaryIndex(dataset.getDataverseName(), dataset.getDatasetName());
        String indexName = primaryIndex.getIndexName();

        String itemTypeName = dataset.getItemTypeName();
        ARecordType itemType = (ARecordType) metadata.findType(itemTypeName);

        ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType);

        IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
        IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                itemType, context.getBinaryComparatorFactoryProvider());
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, indexName);
        TreeIndexInsertUpdateDeleteOperatorDescriptor btreeBulkLoad = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, recordDesc, appContext.getStorageManagerInterface(), appContext.getIndexRegistryProvider(),
                splitsAndConstraint.first, typeTraits, comparatorFactories, fieldPermutation, indexOp,
                new BTreeDataflowHelperFactory(), null, NoOpOperationCallbackProvider.INSTANCE, txnId);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeBulkLoad, splitsAndConstraint.second);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payload, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec)
            throws AlgebricksException {
        return getInsertOrDeleteRuntime(IndexOp.INSERT, dataSource, propagatedSchema, keys, payload, recordDesc,
                context, spec);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payload, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec)
            throws AlgebricksException {
        return getInsertOrDeleteRuntime(IndexOp.DELETE, dataSource, propagatedSchema, keys, payload, recordDesc,
                context, spec);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertOrDeleteRuntime(IndexOp indexOp,
            IDataSourceIndex<String, AqlSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec) throws AlgebricksException {
        String indexName = dataSourceIndex.getId();
        String datasetName = dataSourceIndex.getDataSource().getId().getDatasetName();
        Dataset dataset = metadata.findDataset(datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        Index secondaryIndex = metadata.getIndex(dataset.getDataverseName(), dataset.getDatasetName(), indexName);
        AsterixTupleFilterFactory filterFactory = createTupleFilterFactory(inputSchemas, typeEnv, filterExpr, context);
        switch (secondaryIndex.getIndexType()) {
            case BTREE: {
                return getBTreeDmlRuntime(datasetName, indexName, propagatedSchema, primaryKeys, secondaryKeys,
                        filterFactory, recordDesc, context, spec, indexOp);
            }
            case RTREE: {
                return getRTreeDmlRuntime(datasetName, indexName, propagatedSchema, primaryKeys, secondaryKeys,
                        filterFactory, recordDesc, context, spec, indexOp);
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
        return getIndexInsertOrDeleteRuntime(IndexOp.INSERT, dataSourceIndex, propagatedSchema, inputSchemas, typeEnv,
                primaryKeys, secondaryKeys, filterExpr, recordDesc, context, spec);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<String, AqlSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec) throws AlgebricksException {
        return getIndexInsertOrDeleteRuntime(IndexOp.DELETE, dataSourceIndex, propagatedSchema, inputSchemas, typeEnv,
                primaryKeys, secondaryKeys, filterExpr, recordDesc, context, spec);
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

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getBTreeDmlRuntime(String datasetName,
            String indexName, IOperatorSchema propagatedSchema, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, AsterixTupleFilterFactory filterFactory, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec, IndexOp indexOp) throws AlgebricksException {
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

        Dataset dataset = metadata.findDataset(datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType = metadata.findType(itemTypeName);
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Only record types can be indexed.");
        }
        ARecordType recType = (ARecordType) itemType;

        // Index parameters.
        Index secondaryIndex = metadata.getIndex(dataset.getDataverseName(), dataset.getDatasetName(), indexName);
        List<String> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();
        ITypeTraits[] typeTraits = new ITypeTraits[numKeys];
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numKeys];
        for (i = 0; i < secondaryKeys.size(); ++i) {
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(secondaryKeyExprs.get(i).toString(),
                    recType);
            IAType keyType = keyPairType.first;
            comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType,
                    true);
            typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
        }
        List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        for (String partitioningKey : partitioningKeys) {
            IAType keyType = recType.getFieldType(partitioningKey);
            comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType,
                    true);
            typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            ++i;
        }

        IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, indexName);
        TreeIndexInsertUpdateDeleteOperatorDescriptor btreeBulkLoad = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, recordDesc, appContext.getStorageManagerInterface(), appContext.getIndexRegistryProvider(),
                splitsAndConstraint.first, typeTraits, comparatorFactories, fieldPermutation, indexOp,
                new BTreeDataflowHelperFactory(), filterFactory, NoOpOperationCallbackProvider.INSTANCE, txnId);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeBulkLoad, splitsAndConstraint.second);
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getRTreeDmlRuntime(String datasetName,
            String indexName, IOperatorSchema propagatedSchema, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, AsterixTupleFilterFactory filterFactory, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec, IndexOp indexOp) throws AlgebricksException {
        Dataset dataset = metadata.findDataset(datasetName);
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType = metadata.findType(itemTypeName);
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Only record types can be indexed.");
        }
        ARecordType recType = (ARecordType) itemType;
        Index secondaryIndex = metadata.getIndex(dataset.getDataverseName(), dataset.getDatasetName(), indexName);
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
            comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType,
                    true);
            typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            ++i;
        }

        IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, indexName);
        TreeIndexInsertUpdateDeleteOperatorDescriptor rtreeUpdate = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, recordDesc, appContext.getStorageManagerInterface(), appContext.getIndexRegistryProvider(),
                splitsAndConstraint.first, typeTraits, comparatorFactories, fieldPermutation, indexOp,
                new RTreeDataflowHelperFactory(valueProviderFactories), filterFactory,
                NoOpOperationCallbackProvider.INSTANCE, txnId);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(rtreeUpdate, splitsAndConstraint.second);
    }

    public long getTxnId() {
        return txnId;
    }

    public static ITreeIndexFrameFactory createBTreeNSMInteriorFrameFactory(ITypeTraits[] typeTraits) {
        return new BTreeNSMInteriorFrameFactory(new TypeAwareTupleWriterFactory(typeTraits));
    }

    @Override
    public IFunctionInfo lookupFunction(FunctionIdentifier fid) {
        return AsterixBuiltinFunctions.lookupFunction(fid);
    }
}
