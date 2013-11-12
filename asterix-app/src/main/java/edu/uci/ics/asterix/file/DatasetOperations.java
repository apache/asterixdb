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

package edu.uci.ics.asterix.file;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.uci.ics.asterix.api.common.Job;
import edu.uci.ics.asterix.common.api.ILocalResourceMetadata;
import edu.uci.ics.asterix.common.config.AsterixStorageProperties;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.config.OptimizationConfUtil;
import edu.uci.ics.asterix.common.context.AsterixVirtualBufferCacheProvider;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerProvider;
import edu.uci.ics.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import edu.uci.ics.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledDatasetDropStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledLoadFromFileStatement;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.LogicalExpressionJobGenToExpressionRuntimeProviderAdapter;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexCompactOperatorDescriptor;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;

public class DatasetOperations {

    private static final PhysicalOptimizationConfig physicalOptimizationConfig = OptimizationConfUtil
            .getPhysicalOptimizationConfig();

    private static Logger LOGGER = Logger.getLogger(DatasetOperations.class.getName());

    public static JobSpecification createDropDatasetJobSpec(CompiledDatasetDropStatement datasetDropStmt,
            AqlMetadataProvider metadataProvider) throws AlgebricksException, HyracksDataException, RemoteException,
            ACIDException, AsterixException {

        String dataverseName = null;
        if (datasetDropStmt.getDataverseName() != null) {
            dataverseName = datasetDropStmt.getDataverseName();
        } else if (metadataProvider.getDefaultDataverse() != null) {
            dataverseName = metadataProvider.getDefaultDataverse().getDataverseName();
        }

        String datasetName = datasetDropStmt.getDatasetName();
        String datasetPath = dataverseName + File.separator + datasetName;

        LOGGER.info("DROP DATASETPATH: " + datasetPath);

        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("DROP DATASET: No metadata for dataset " + datasetName);
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return JobSpecificationUtils.createJobSpecification();
        }

        JobSpecification specPrimary = JobSpecificationUtils.createJobSpecification();

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadataProvider
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(dataset.getDataverseName(), datasetName,
                        datasetName);
        AsterixStorageProperties storageProperties = AsterixAppContextInfo.getInstance().getStorageProperties();
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(dataset,
                metadataProvider.getMetadataTxnContext());
        IndexDropOperatorDescriptor primaryBtreeDrop = new IndexDropOperatorDescriptor(specPrimary,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                splitsAndConstraint.first, new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(
                        dataset.getDatasetId()), compactionInfo.first, compactionInfo.second,
                        new PrimaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate()));
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(specPrimary, primaryBtreeDrop,
                splitsAndConstraint.second);

        specPrimary.addRoot(primaryBtreeDrop);

        return specPrimary;
    }

    public static JobSpecification createDatasetJobSpec(Dataverse dataverse, String datasetName,
            AqlMetadataProvider metadata) throws AsterixException, AlgebricksException {
        String dataverseName = dataverse.getDataverseName();
        IDataFormat format;
        try {
            format = (IDataFormat) Class.forName(dataverse.getDataFormat()).newInstance();
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        Dataset dataset = metadata.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AsterixException("Could not find dataset " + datasetName + " in dataverse " + dataverseName);
        }
        ARecordType itemType = (ARecordType) metadata.findType(dataverseName, dataset.getItemTypeName());
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                itemType, format.getBinaryComparatorFactoryProvider());
        ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType);
        int[] blooFilterKeyFields = DatasetUtils.createBloomFilterKeyFields(dataset);

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(dataverseName, datasetName, datasetName);
        FileSplit[] fs = splitsAndConstraint.first.getFileSplits();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fs.length; i++) {
            sb.append(stringOf(fs[i]) + " ");
        }
        LOGGER.info("CREATING File Splits: " + sb.toString());

        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(dataset,
                metadata.getMetadataTxnContext());
        AsterixStorageProperties storageProperties = AsterixAppContextInfo.getInstance().getStorageProperties();
        //prepare a LocalResourceMetadata which will be stored in NC's local resource repository
        ILocalResourceMetadata localResourceMetadata = new LSMBTreeLocalResourceMetadata(typeTraits,
                comparatorFactories, blooFilterKeyFields, true, dataset.getDatasetId(), compactionInfo.first,
                compactionInfo.second);
        ILocalResourceFactoryProvider localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(
                localResourceMetadata, LocalResource.LSMBTreeResource);

        TreeIndexCreateOperatorDescriptor indexCreateOp = new TreeIndexCreateOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                splitsAndConstraint.first, typeTraits, comparatorFactories, blooFilterKeyFields,
                new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                        compactionInfo.first, compactionInfo.second, new PrimaryIndexOperationTrackerProvider(dataset
                                .getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                        LSMBTreeIOOperationCallbackFactory.INSTANCE, storageProperties
                                .getBloomFilterFalsePositiveRate()), localResourceFactoryProvider,
                NoOpOperationCallbackFactory.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, indexCreateOp,
                splitsAndConstraint.second);
        spec.addRoot(indexCreateOp);
        return spec;
    }

    @SuppressWarnings("rawtypes")
    public static Job createLoadDatasetJobSpec(AqlMetadataProvider metadataProvider,
            CompiledLoadFromFileStatement loadStmt, IDataFormat format) throws AsterixException, AlgebricksException {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        String dataverseName = loadStmt.getDataverseName();
        String datasetName = loadStmt.getDatasetName();
        Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
        if (dataset == null) {
            throw new AsterixException("Could not find dataset " + datasetName + " in dataverse " + dataverseName);
        }
        if (dataset.getDatasetType() != DatasetType.INTERNAL && dataset.getDatasetType() != DatasetType.FEED) {
            throw new AsterixException("Cannot load data into dataset  (" + datasetName + ")" + "of type "
                    + dataset.getDatasetType());
        }
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();

        ARecordType itemType = (ARecordType) MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName,
                dataset.getItemTypeName()).getDatatype();
        ISerializerDeserializer payloadSerde = format.getSerdeProvider().getSerializerDeserializer(itemType);

        IBinaryHashFunctionFactory[] hashFactories = DatasetUtils.computeKeysBinaryHashFunFactories(dataset, itemType,
                format.getBinaryHashFunctionFactoryProvider());
        IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                itemType, format.getBinaryComparatorFactoryProvider());
        ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType);
        int[] blooFilterKeyFields = DatasetUtils.createBloomFilterKeyFields(dataset);

        ExternalDatasetDetails externalDatasetDetails = new ExternalDatasetDetails(loadStmt.getAdapter(),
                loadStmt.getProperties());

        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = metadataProvider.buildExternalDataScannerRuntime(
                spec, itemType, externalDatasetDetails, format);
        IOperatorDescriptor scanner = p.first;
        AlgebricksPartitionConstraint scannerPc = p.second;
        RecordDescriptor recDesc = computePayloadKeyRecordDescriptor(dataset, itemType, payloadSerde, format);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, scanner, scannerPc);

        AssignRuntimeFactory assign = makeAssignRuntimeFactory(dataset, itemType, format);
        AlgebricksMetaOperatorDescriptor asterixOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { recDesc });

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixOp, scannerPc);

        int numKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        int[] keys = new int[numKeys];
        for (int i = 0; i < numKeys; i++) {
            keys[i] = i + 1;
        }
        // Move key fields to front.
        int[] fieldPermutation = new int[numKeys + 1];
        for (int i = 0; i < numKeys; i++) {
            fieldPermutation[i] = i + 1;
        }
        fieldPermutation[numKeys] = 0;

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadataProvider
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(dataverseName, datasetName, datasetName);

        FileSplit[] fs = splitsAndConstraint.first.getFileSplits();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fs.length; i++) {
            sb.append(stringOf(fs[i]) + " ");
        }
        LOGGER.info("LOAD into File Splits: " + sb.toString());

        long numElementsHint = metadataProvider.getCardinalityPerPartitionHint(dataset);
        AsterixStorageProperties storageProperties = AsterixAppContextInfo.getInstance().getStorageProperties();
        TreeIndexBulkLoadOperatorDescriptor btreeBulkLoad;
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(dataset,
                metadataProvider.getMetadataTxnContext());
        if (!loadStmt.alreadySorted()) {
            btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, splitsAndConstraint.first, typeTraits,
                    comparatorFactories, blooFilterKeyFields, fieldPermutation, GlobalConfig.DEFAULT_BTREE_FILL_FACTOR,
                    true, numElementsHint, true, new LSMBTreeDataflowHelperFactory(
                            new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                            compactionInfo.second, new PrimaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE,
                            storageProperties.getBloomFilterFalsePositiveRate()), NoOpOperationCallbackFactory.INSTANCE);
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, btreeBulkLoad,
                    splitsAndConstraint.second);

            spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, asterixOp, 0);

            int framesLimit = physicalOptimizationConfig.getMaxFramesExternalSort();
            ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, framesLimit, keys,
                    comparatorFactories, recDesc);
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sorter,
                    splitsAndConstraint.second);
            IConnectorDescriptor hashConn = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keys, hashFactories));
            spec.connect(hashConn, asterixOp, 0, sorter, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, btreeBulkLoad, 0);
        } else {
            btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, splitsAndConstraint.first, typeTraits,
                    comparatorFactories, blooFilterKeyFields, fieldPermutation, GlobalConfig.DEFAULT_BTREE_FILL_FACTOR,
                    true, numElementsHint, true, new LSMBTreeDataflowHelperFactory(
                            new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                            compactionInfo.second, new PrimaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE,
                            storageProperties.getBloomFilterFalsePositiveRate()), NoOpOperationCallbackFactory.INSTANCE);
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, btreeBulkLoad,
                    splitsAndConstraint.second);

            spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, asterixOp, 0);

            IConnectorDescriptor sortMergeConn = new MToNPartitioningMergingConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keys, hashFactories), keys, comparatorFactories, null);
            spec.connect(sortMergeConn, asterixOp, 0, btreeBulkLoad, 0);
        }
        spec.addRoot(btreeBulkLoad);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        return new Job(spec);
    }

    private static String stringOf(FileSplit fs) {
        return fs.getNodeName() + ":" + fs.getLocalFile().toString();
    }

    private static AssignRuntimeFactory makeAssignRuntimeFactory(Dataset dataset, ARecordType itemType,
            IDataFormat format) throws AlgebricksException {
        List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        int numKeys = partitioningKeys.size();
        ICopyEvaluatorFactory[] evalFactories = new ICopyEvaluatorFactory[numKeys];
        for (int i = 0; i < numKeys; i++) {
            Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType = format
                    .partitioningEvaluatorFactory(itemType, partitioningKeys.get(i));
            evalFactories[i] = evalFactoryAndType.first;
        }
        int[] outColumns = new int[numKeys];
        int[] projectionList = new int[numKeys + 1];
        projectionList[0] = 0;

        for (int i = 0; i < numKeys; i++) {
            outColumns[i] = i + 1;
            projectionList[i + 1] = i + 1;
        }
        IScalarEvaluatorFactory[] sefs = new IScalarEvaluatorFactory[evalFactories.length];
        for (int i = 0; i < evalFactories.length; ++i) {
            sefs[i] = new LogicalExpressionJobGenToExpressionRuntimeProviderAdapter.ScalarEvaluatorFactoryAdapter(
                    evalFactories[i]);
        }
        return new AssignRuntimeFactory(outColumns, sefs, projectionList);
    }

    @SuppressWarnings("rawtypes")
    private static RecordDescriptor computePayloadKeyRecordDescriptor(Dataset dataset, ARecordType itemType,
            ISerializerDeserializer payloadSerde, IDataFormat dataFormat) throws AlgebricksException {
        List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        int numKeys = partitioningKeys.size();
        ISerializerDeserializer[] recordFields = new ISerializerDeserializer[1 + numKeys];
        recordFields[0] = payloadSerde;
        for (int i = 0; i < numKeys; i++) {
            IAType keyType;
            try {
                keyType = itemType.getFieldType(partitioningKeys.get(i));
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
            ISerializerDeserializer keySerde = dataFormat.getSerdeProvider().getSerializerDeserializer(keyType);
            recordFields[i + 1] = keySerde;
        }
        return new RecordDescriptor(recordFields);
    }

    public static JobSpecification compactDatasetJobSpec(Dataverse dataverse, String datasetName,
            AqlMetadataProvider metadata) throws AsterixException, AlgebricksException {
        String dataverseName = dataverse.getDataverseName();
        IDataFormat format;
        try {
            format = (IDataFormat) Class.forName(dataverse.getDataFormat()).newInstance();
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        Dataset dataset = metadata.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AsterixException("Could not find dataset " + datasetName + " in dataverse " + dataverseName);
        }
        ARecordType itemType = (ARecordType) metadata.findType(dataverseName, dataset.getItemTypeName());
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                itemType, format.getBinaryComparatorFactoryProvider());
        ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType);
        int[] blooFilterKeyFields = DatasetUtils.createBloomFilterKeyFields(dataset);

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(dataverseName, datasetName, datasetName);

        AsterixStorageProperties storageProperties = AsterixAppContextInfo.getInstance().getStorageProperties();

        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(dataset,
                metadata.getMetadataTxnContext());
        LSMTreeIndexCompactOperatorDescriptor compactOp = new LSMTreeIndexCompactOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                splitsAndConstraint.first, typeTraits, comparatorFactories, blooFilterKeyFields,
                new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                        compactionInfo.first, compactionInfo.second, new PrimaryIndexOperationTrackerProvider(
                                dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                        LSMBTreeIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate()), NoOpOperationCallbackFactory.INSTANCE);
        AlgebricksPartitionConstraintHelper
                .setPartitionConstraintInJobSpec(spec, compactOp, splitsAndConstraint.second);

        AlgebricksPartitionConstraintHelper
                .setPartitionConstraintInJobSpec(spec, compactOp, splitsAndConstraint.second);
        spec.addRoot(compactOp);
        return spec;
    }
}
