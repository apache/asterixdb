/*
 * Copyright 2009-2011 by The Regents of the University of California
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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.asterix.api.common.Job;
import edu.uci.ics.asterix.aql.translator.DdlTranslator.CompiledDatasetDropStatement;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.config.OptimizationConfUtil;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.context.AsterixTreeRegistryProvider;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.std.NoTupleSourceRuntimeFactory;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledLoadFromFileStatement;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.std.AssignRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;
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
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDropOperatorDescriptor;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class DatasetOperations {

    private static final PhysicalOptimizationConfig physicalOptimizationConfig = OptimizationConfUtil
            .getPhysicalOptimizationConfig();

    private static Logger LOGGER = Logger.getLogger(DatasetOperations.class.getName());

    public static JobSpecification[] createDropDatasetJobSpec(CompiledDatasetDropStatement deleteStmt,
            AqlCompiledMetadataDeclarations metadata) throws AlgebricksException, HyracksDataException,
            RemoteException, ACIDException, AsterixException {

        String datasetName = deleteStmt.getDatasetName();
        String datasetPath = metadata.getRelativePath(datasetName);

        LOGGER.info("DROP DATASETPATH: " + datasetPath);

        IIndexRegistryProvider<IIndex> btreeRegistryProvider = AsterixTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;

        AqlCompiledDatasetDecl adecl = metadata.findDataset(datasetName);
        if (adecl == null) {
            throw new AlgebricksException("DROP DATASET: No metadata for dataset " + datasetName);
        }
        if (adecl.getDatasetType() == DatasetType.EXTERNAL) {
            return new JobSpecification[0];
        }

        List<AqlCompiledIndexDecl> secondaryIndexes = DatasetUtils.getSecondaryIndexes(adecl);

        JobSpecification[] specs;

        if (secondaryIndexes != null && !secondaryIndexes.isEmpty()) {
            int n = secondaryIndexes.size();
            specs = new JobSpecification[n + 1];
            int i = 0;
            // first, drop indexes
            for (AqlCompiledIndexDecl acid : secondaryIndexes) {
                specs[i] = new JobSpecification();
                Pair<IFileSplitProvider, AlgebricksPartitionConstraint> idxSplitsAndConstraint = metadata
                        .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, acid.getIndexName());
                TreeIndexDropOperatorDescriptor secondaryBtreeDrop = new TreeIndexDropOperatorDescriptor(specs[i],
                        storageManager, btreeRegistryProvider, idxSplitsAndConstraint.first);
                AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(specs[i], secondaryBtreeDrop,
                        idxSplitsAndConstraint.second);
                i++;
            }
        } else {
            specs = new JobSpecification[1];
        }
        JobSpecification specPrimary = new JobSpecification();
        specs[specs.length - 1] = specPrimary;

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, datasetName);
        TreeIndexDropOperatorDescriptor primaryBtreeDrop = new TreeIndexDropOperatorDescriptor(specPrimary,
                storageManager, btreeRegistryProvider, splitsAndConstraint.first);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(specPrimary, primaryBtreeDrop,
                splitsAndConstraint.second);

        specPrimary.addRoot(primaryBtreeDrop);

        return specs;
    }

    public static JobSpecification[] createInitializeDatasetJobSpec(long txnId, String datasetName,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException {

        AqlCompiledDatasetDecl compiledDatasetDecl = metadata.findDataset(datasetName);
        if (compiledDatasetDecl == null) {
            throw new AsterixException("Could not find dataset " + datasetName);
        }
        if (compiledDatasetDecl.getDatasetType() != DatasetType.INTERNAL
                && compiledDatasetDecl.getDatasetType() != DatasetType.FEED) {
            throw new AsterixException("Cannot initialize  dataset  (" + datasetName + ")" + "of type "
                    + compiledDatasetDecl.getDatasetType());
        }

        ARecordType itemType = (ARecordType) metadata.findType(compiledDatasetDecl.getItemTypeName());
        IDataFormat format;
        ISerializerDeserializer payloadSerde;
        IBinaryComparatorFactory[] comparatorFactories;
        ITypeTraits[] typeTraits;
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint;

        try {
            format = metadata.getFormat();
            payloadSerde = format.getSerdeProvider().getSerializerDeserializer(itemType);
            comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(compiledDatasetDecl, metadata
                    .getFormat().getBinaryComparatorFactoryProvider());
            typeTraits = DatasetUtils.computeTupleTypeTraits(compiledDatasetDecl, metadata);
            splitsAndConstraint = metadata.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName,
                    datasetName);

        } catch (AlgebricksException e1) {
            throw new AsterixException(e1);
        }

        ITreeIndexFrameFactory interiorFrameFactory = AqlMetadataProvider.createBTreeNSMInteriorFrameFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = AqlMetadataProvider.createBTreeNSMLeafFrameFactory(typeTraits);

        IIndexRegistryProvider<IIndex> btreeRegistryProvider = AsterixTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;

        JobSpecification spec = new JobSpecification();
        RecordDescriptor recDesc;
        try {
            recDesc = computePayloadKeyRecordDescriptor(compiledDatasetDecl, payloadSerde, metadata.getFormat());
            NoTupleSourceRuntimeFactory factory = new NoTupleSourceRuntimeFactory();
            AlgebricksMetaOperatorDescriptor asterixOp = new AlgebricksMetaOperatorDescriptor(spec, 0, 1,
                    new IPushRuntimeFactory[] { factory }, new RecordDescriptor[] { recDesc });

            // move key fieldsx to front
            List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                    .getPartitioningFunctions(compiledDatasetDecl);
            int numKeys = partitioningFunctions.size();
            int[] keys = new int[numKeys];
            for (int i = 0; i < numKeys; i++) {
                keys[i] = i + 1;
            }

            int[] fieldPermutation = new int[numKeys + 1];
            System.arraycopy(keys, 0, fieldPermutation, 0, numKeys);
            fieldPermutation[numKeys] = 0;

            TreeIndexBulkLoadOperatorDescriptor bulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                    storageManager, btreeRegistryProvider, splitsAndConstraint.first, interiorFrameFactory,
                    leafFrameFactory, typeTraits, comparatorFactories, fieldPermutation,
                    GlobalConfig.DEFAULT_BTREE_FILL_FACTOR, new BTreeDataflowHelperFactory());

            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixOp,
                    splitsAndConstraint.second);
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, bulkLoad,
                    splitsAndConstraint.second);

            spec.connect(new OneToOneConnectorDescriptor(spec), asterixOp, 0, bulkLoad, 0);

            spec.addRoot(bulkLoad);
        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }

        return new JobSpecification[] { spec };
    }

    @SuppressWarnings("unchecked")
    public static List<Job> createLoadDatasetJobSpec(CompiledLoadFromFileStatement loadStmt,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException {

        String datasetName = loadStmt.getDatasetName();

        AqlCompiledDatasetDecl compiledDatasetDecl = metadata.findDataset(datasetName);
        if (compiledDatasetDecl == null) {
            throw new AsterixException("Could not find dataset " + datasetName);
        }
        if (compiledDatasetDecl.getDatasetType() != DatasetType.INTERNAL
                && compiledDatasetDecl.getDatasetType() != DatasetType.FEED) {
            throw new AsterixException("Cannot load data into dataset  (" + datasetName + ")" + "of type "
                    + compiledDatasetDecl.getDatasetType());
        }

        List<Job> jobSpecs = new ArrayList<Job>();
        try {
            jobSpecs.addAll(dropDatasetIndexes(datasetName, metadata));
        } catch (AlgebricksException ae) {
            throw new AsterixException(ae);
        }

        ARecordType itemType = (ARecordType) metadata.findType(compiledDatasetDecl.getItemTypeName());
        IDataFormat format;
        try {
            format = metadata.getFormat();
        } catch (AlgebricksException e1) {
            throw new AsterixException(e1);
        }
        ISerializerDeserializer payloadSerde;
        try {
            payloadSerde = format.getSerdeProvider().getSerializerDeserializer(itemType);
        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }

        IBinaryHashFunctionFactory[] hashFactories;
        IBinaryComparatorFactory[] comparatorFactories;
        ITypeTraits[] typeTraits;
        try {
            hashFactories = DatasetUtils.computeKeysBinaryHashFunFactories(compiledDatasetDecl, metadata.getFormat()
                    .getBinaryHashFunctionFactoryProvider());
            comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(compiledDatasetDecl, metadata
                    .getFormat().getBinaryComparatorFactoryProvider());
            typeTraits = DatasetUtils.computeTupleTypeTraits(compiledDatasetDecl, metadata);
        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }

        JobSpecification spec = new JobSpecification();
        IOperatorDescriptor scanner;
        AlgebricksPartitionConstraint scannerPc;
        RecordDescriptor recDesc;
        try {
            AqlCompiledExternalDatasetDetails add = new AqlCompiledExternalDatasetDetails(loadStmt.getAdapter(),
                    loadStmt.getProperties());
            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = AqlMetadataProvider
                    .buildExternalDataScannerRuntime(spec, itemType, add, format);
            scanner = p.first;
            scannerPc = p.second;
            recDesc = computePayloadKeyRecordDescriptor(compiledDatasetDecl, payloadSerde, metadata.getFormat());
        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, scanner, scannerPc);

        AssignRuntimeFactory assign = makeAssignRuntimeFactory(compiledDatasetDecl);
        AlgebricksMetaOperatorDescriptor asterixOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { recDesc });

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixOp, scannerPc);

        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(compiledDatasetDecl);
        int numKeys = partitioningFunctions.size();
        int[] keys = new int[numKeys];
        for (int i = 0; i < numKeys; i++) {
            keys[i] = i + 1;
        }
        int framesLimit = physicalOptimizationConfig.getMaxFramesExternalSort();

        ITreeIndexFrameFactory interiorFrameFactory = AqlMetadataProvider.createBTreeNSMInteriorFrameFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = AqlMetadataProvider.createBTreeNSMLeafFrameFactory(typeTraits);

        IIndexRegistryProvider<IIndex> btreeRegistryProvider = AsterixTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;

        // move key fields to front
        int[] fieldPermutation = new int[numKeys + 1];
        System.arraycopy(keys, 0, fieldPermutation, 0, numKeys);
        fieldPermutation[numKeys] = 0;

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint;
        try {
            splitsAndConstraint = metadata.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName,
                    datasetName);
        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }

        FileSplit[] fs = splitsAndConstraint.first.getFileSplits();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fs.length; i++) {
            sb.append(stringOf(fs[i]) + " ");
        }
        LOGGER.info("LOAD into File Splits: " + sb.toString());

        TreeIndexBulkLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManager, btreeRegistryProvider, splitsAndConstraint.first, interiorFrameFactory,
                leafFrameFactory, typeTraits, comparatorFactories, fieldPermutation,
                GlobalConfig.DEFAULT_BTREE_FILL_FACTOR, new BTreeDataflowHelperFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, btreeBulkLoad,
                splitsAndConstraint.second);

        spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, asterixOp, 0);

        if (!loadStmt.alreadySorted()) {
            ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, framesLimit, keys,
                    comparatorFactories, recDesc);
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sorter,
                    splitsAndConstraint.second);

            IConnectorDescriptor hashConn = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keys, hashFactories));

            spec.connect(hashConn, asterixOp, 0, sorter, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, btreeBulkLoad, 0);
        } else {
            IConnectorDescriptor sortMergeConn = new MToNPartitioningMergingConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keys, hashFactories), keys, comparatorFactories);
            spec.connect(sortMergeConn, asterixOp, 0, btreeBulkLoad, 0);
        }

        spec.addRoot(btreeBulkLoad);

        jobSpecs.add(new Job(spec));
        return jobSpecs;
    }

    private static List<Job> dropDatasetIndexes(String datasetName, AqlCompiledMetadataDeclarations metadata)
            throws AlgebricksException, MetadataException {

        AqlCompiledDatasetDecl adecl = metadata.findDataset(datasetName);
        if (adecl == null) {
            throw new AlgebricksException("DROP DATASET INDEXES: No metadata for dataset " + datasetName);
        }

        List<AqlCompiledIndexDecl> indexes = DatasetUtils.getSecondaryIndexes(adecl);
        indexes.add(DatasetUtils.getPrimaryIndex(adecl));

        List<Job> specs = new ArrayList<Job>();
        IIndexRegistryProvider<IIndex> btreeRegistryProvider = AsterixTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;

        if (indexes != null && !indexes.isEmpty()) {
            // first, drop indexes
            for (AqlCompiledIndexDecl acid : indexes) {
                JobSpecification spec = new JobSpecification();
                Pair<IFileSplitProvider, AlgebricksPartitionConstraint> idxSplitsAndConstraint = metadata
                        .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, acid.getIndexName());
                TreeIndexDropOperatorDescriptor secondaryBtreeDrop = new TreeIndexDropOperatorDescriptor(spec,
                        storageManager, btreeRegistryProvider, idxSplitsAndConstraint.first);
                AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, secondaryBtreeDrop,
                        idxSplitsAndConstraint.second);
                specs.add(new Job(spec));
            }
        }
        return specs;
    }

    private static String stringOf(FileSplit fs) {
        return fs.getNodeName() + ":" + fs.getLocalFile().toString();
    }

    private static AssignRuntimeFactory makeAssignRuntimeFactory(AqlCompiledDatasetDecl compiledDatasetDecl) {
        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(compiledDatasetDecl);
        int numKeys = partitioningFunctions.size();
        IEvaluatorFactory[] evalFactories = new IEvaluatorFactory[numKeys];
        for (int i = 0; i < numKeys; i++) {
            Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType = partitioningFunctions
                    .get(i);
            evalFactories[i] = evalFactoryAndType.first;
        }
        int[] outColumns = new int[numKeys];
        int[] projectionList = new int[numKeys + 1];
        projectionList[0] = 0;

        for (int i = 0; i < numKeys; i++) {
            outColumns[i] = i + 1;
            projectionList[i + 1] = i + 1;
        }
        return new AssignRuntimeFactory(outColumns, evalFactories, projectionList);
    }

    @SuppressWarnings("unchecked")
    private static RecordDescriptor computePayloadKeyRecordDescriptor(AqlCompiledDatasetDecl compiledDatasetDecl,
            ISerializerDeserializer payloadSerde, IDataFormat dataFormat) throws AlgebricksException {
        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(compiledDatasetDecl);
        int numKeys = partitioningFunctions.size();
        ISerializerDeserializer[] recordFields = new ISerializerDeserializer[1 + numKeys];
        recordFields[0] = payloadSerde;
        for (int i = 0; i < numKeys; i++) {
            Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType = partitioningFunctions
                    .get(i);
            IAType keyType = evalFactoryAndType.third;
            ISerializerDeserializer keySerde = dataFormat.getSerdeProvider().getSerializerDeserializer(keyType);
            recordFields[i + 1] = keySerde;
        }
        return new RecordDescriptor(recordFields);
    }

}
