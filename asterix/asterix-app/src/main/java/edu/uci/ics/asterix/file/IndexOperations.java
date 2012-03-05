package edu.uci.ics.asterix.file;

import java.io.DataOutput;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.asterix.aql.translator.DdlTranslator.CompiledIndexDropStatement;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.OptimizationConfUtil;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.context.AsterixTreeRegistryProvider;
import edu.uci.ics.asterix.dataflow.data.nontagged.valueproviders.AqlPrimitiveValueProviderFactory;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryTokenizerFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledCreateIndexStatement;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
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
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDropOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class IndexOperations {

    private static final PhysicalOptimizationConfig physicalOptimizationConfig = OptimizationConfUtil
            .getPhysicalOptimizationConfig();

    private static final Logger LOGGER = Logger.getLogger(IndexOperations.class.getName());

    public static JobSpecification buildCreateIndexJobSpec(CompiledCreateIndexStatement createIndexStmt,
            AqlCompiledMetadataDeclarations datasetDecls) throws AsterixException, AlgebricksException {

        switch (createIndexStmt.getIndexType()) {
            case BTREE: {
                return createBtreeIndexJobSpec(createIndexStmt, datasetDecls);
            }

            case RTREE: {
                return createRtreeIndexJobSpec(createIndexStmt, datasetDecls);
            }

            case KEYWORD: {
                return createKeywordIndexJobSpec(createIndexStmt, datasetDecls);
            }

            case QGRAM: {
                // return createQgramIndexJobSpec(createIndexStmt,
                // datasetDecls);
            }

            default: {
                throw new AsterixException("Unknown Index Type: " + createIndexStmt.getIndexType());
            }

        }
    }

    public static JobSpecification createSecondaryIndexDropJobSpec(CompiledIndexDropStatement deleteStmt,
            AqlCompiledMetadataDeclarations datasetDecls) throws AlgebricksException, MetadataException {
        String datasetName = deleteStmt.getDatasetName();
        String indexName = deleteStmt.getIndexName();

        JobSpecification spec = new JobSpecification();
        IIndexRegistryProvider<IIndex> btreeRegistryProvider = AsterixTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = datasetDecls
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, indexName);
        TreeIndexDropOperatorDescriptor btreeDrop = new TreeIndexDropOperatorDescriptor(spec, storageManager,
                btreeRegistryProvider, splitsAndConstraint.first);
        AlgebricksPartitionConstraintHelper
                .setPartitionConstraintInJobSpec(spec, btreeDrop, splitsAndConstraint.second);
        spec.addRoot(btreeDrop);

        return spec;
    }

    @SuppressWarnings("unchecked")
    public static JobSpecification createBtreeIndexJobSpec(CompiledCreateIndexStatement createIndexStmt,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException, AlgebricksException {

        JobSpecification spec = new JobSpecification();

        String datasetName = createIndexStmt.getDatasetName();
        String secondaryIndexName = createIndexStmt.getIndexName();

        AqlCompiledDatasetDecl compiledDatasetDecl = metadata.findDataset(datasetName);
        if (compiledDatasetDecl == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        ARecordType itemType = (ARecordType) metadata.findType(compiledDatasetDecl.getItemTypeName());
        ISerializerDeserializer payloadSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(itemType);

        if (compiledDatasetDecl.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AsterixException("Cannot index an external dataset (" + datasetName + ").");
        }

        AqlCompiledDatasetDecl srcCompiledDatasetDecl = compiledDatasetDecl;
        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(compiledDatasetDecl).size();

        // ---------- START GENERAL BTREE STUFF
        IIndexRegistryProvider<IIndex> treeRegistryProvider = AsterixTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;

        // ---------- END GENERAL BTREE STUFF

        // ---------- START KEY PROVIDER OP

        // TODO: should actually be empty tuple source
        // build tuple containing low and high search keys
        ArrayTupleBuilder tb = new ArrayTupleBuilder(1); // just one dummy field
        DataOutput dos = tb.getDataOutput();

        tb.reset();
        try {
            IntegerSerializerDeserializer.INSTANCE.serialize(0, dos);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        } // dummy field
        tb.addFieldEndOffset();

        ISerializerDeserializer[] keyRecDescSers = { IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> keyProviderSplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, datasetName);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, keyProviderOp,
                keyProviderSplitsAndConstraint.second);

        // ---------- END KEY PROVIDER OP

        // ---------- START PRIMARY INDEX SCAN

        ISerializerDeserializer[] primaryRecFields = new ISerializerDeserializer[numPrimaryKeys + 1];
        IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[numPrimaryKeys];
        ITypeTraits[] primaryTypeTraits = new ITypeTraits[numPrimaryKeys + 1];
        int i = 0;
        ISerializerDeserializerProvider serdeProvider = metadata.getFormat().getSerdeProvider();
        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(srcCompiledDatasetDecl);
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType : partitioningFunctions) {
            IAType keyType = evalFactoryAndType.third;
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(keyType);
            primaryRecFields[i] = keySerde;
            primaryComparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                    keyType, OrderKind.ASC);
            primaryTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            ++i;
        }
        primaryRecFields[numPrimaryKeys] = payloadSerde;
        primaryTypeTraits[numPrimaryKeys] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(itemType);

        ITreeIndexFrameFactory primaryInteriorFrameFactory = AqlMetadataProvider
                .createBTreeNSMInteriorFrameFactory(primaryTypeTraits);
        ITreeIndexFrameFactory primaryLeafFrameFactory = AqlMetadataProvider
                .createBTreeNSMLeafFrameFactory(primaryTypeTraits);

        int[] lowKeyFields = null; // -infinity
        int[] highKeyFields = null; // +infinity
        RecordDescriptor primaryRecDesc = new RecordDescriptor(primaryRecFields);

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> primarySplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, datasetName);

        BTreeSearchOperatorDescriptor primarySearchOp = new BTreeSearchOperatorDescriptor(spec, primaryRecDesc,
                storageManager, treeRegistryProvider, primarySplitsAndConstraint.first, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits, primaryComparatorFactories, true, lowKeyFields,
                highKeyFields, true, true, new BTreeDataflowHelperFactory());

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, primarySearchOp,
                primarySplitsAndConstraint.second);

        // ---------- END PRIMARY INDEX SCAN

        // ---------- START ASSIGN OP

        List<String> secondaryKeyFields = createIndexStmt.getKeyFields();
        int numSecondaryKeys = secondaryKeyFields.size();
        ISerializerDeserializer[] secondaryRecFields = new ISerializerDeserializer[numPrimaryKeys + numSecondaryKeys];
        IEvaluatorFactory[] evalFactories = new IEvaluatorFactory[numSecondaryKeys];
        IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys
                + numPrimaryKeys];
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        for (i = 0; i < numSecondaryKeys; i++) {
            evalFactories[i] = metadata.getFormat().getFieldAccessEvaluatorFactory(itemType, secondaryKeyFields.get(i),
                    numPrimaryKeys);
            IAType keyType = AqlCompiledIndexDecl.keyFieldType(secondaryKeyFields.get(i), itemType);
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(keyType);
            secondaryRecFields[i] = keySerde;
            secondaryComparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                    keyType, OrderKind.ASC);
            secondaryTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
        }
        // fill in serializers and comparators for primary index fields
        for (i = 0; i < numPrimaryKeys; i++) {
            secondaryRecFields[numSecondaryKeys + i] = primaryRecFields[i];
            secondaryComparatorFactories[numSecondaryKeys + i] = primaryComparatorFactories[i];
            secondaryTypeTraits[numSecondaryKeys + i] = primaryTypeTraits[i];
        }
        RecordDescriptor secondaryRecDesc = new RecordDescriptor(secondaryRecFields);

        int[] outColumns = new int[numSecondaryKeys];
        int[] projectionList = new int[numSecondaryKeys + numPrimaryKeys];
        for (i = 0; i < numSecondaryKeys; i++) {
            outColumns[i] = numPrimaryKeys + i + 1;
        }
        int projCount = 0;
        for (i = 0; i < numSecondaryKeys; i++) {
            projectionList[projCount++] = numPrimaryKeys + i + 1;
        }
        for (i = 0; i < numPrimaryKeys; i++) {
            projectionList[projCount++] = i;
        }

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> assignSplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, datasetName);

        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, evalFactories, projectionList);
        AlgebricksMetaOperatorDescriptor asterixAssignOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { secondaryRecDesc });

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixAssignOp,
                assignSplitsAndConstraint.second);

        // ---------- END ASSIGN OP

        // ---------- START EXTERNAL SORT OP

        int[] sortFields = new int[numSecondaryKeys + numPrimaryKeys];
        for (i = 0; i < numSecondaryKeys + numPrimaryKeys; i++)
            sortFields[i] = i;

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> sorterSplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, datasetName);

        ExternalSortOperatorDescriptor sortOp = new ExternalSortOperatorDescriptor(spec,
                physicalOptimizationConfig.getMaxFramesExternalSort(), sortFields, secondaryComparatorFactories,
                secondaryRecDesc);

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp,
                sorterSplitsAndConstraint.second);

        // ---------- END EXTERNAL SORT OP

        // ---------- START SECONDARY INDEX BULK LOAD

        ITreeIndexFrameFactory secondaryInteriorFrameFactory = AqlMetadataProvider
                .createBTreeNSMInteriorFrameFactory(secondaryTypeTraits);
        ITreeIndexFrameFactory secondaryLeafFrameFactory = AqlMetadataProvider
                .createBTreeNSMLeafFrameFactory(secondaryTypeTraits);

        int[] fieldPermutation = new int[numSecondaryKeys + numPrimaryKeys];
        for (i = 0; i < numSecondaryKeys + numPrimaryKeys; i++)
            fieldPermutation[i] = i;

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, secondaryIndexName);

        // GlobalConfig.DEFAULT_BTREE_FILL_FACTOR
        TreeIndexBulkLoadOperatorDescriptor secondaryBulkLoadOp = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManager, treeRegistryProvider, secondarySplitsAndConstraint.first,
                secondaryInteriorFrameFactory, secondaryLeafFrameFactory, secondaryTypeTraits,
                secondaryComparatorFactories, fieldPermutation, 0.7f, new BTreeDataflowHelperFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, secondaryBulkLoadOp,
                secondarySplitsAndConstraint.second);

        // ---------- END SECONDARY INDEX BULK LOAD

        // ---------- START CONNECT THE OPERATORS

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primarySearchOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), primarySearchOp, 0, asterixAssignOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, sortOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);

        spec.addRoot(secondaryBulkLoadOp);

        // ---------- END CONNECT THE OPERATORS

        return spec;

    }

    @SuppressWarnings("unchecked")
    public static JobSpecification createRtreeIndexJobSpec(CompiledCreateIndexStatement createIndexStmt,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException, AlgebricksException {

        JobSpecification spec = new JobSpecification();

        String primaryIndexName = createIndexStmt.getDatasetName();
        String secondaryIndexName = createIndexStmt.getIndexName();

        AqlCompiledDatasetDecl compiledDatasetDecl = metadata.findDataset(primaryIndexName);
        if (compiledDatasetDecl == null) {
            throw new AsterixException("Could not find dataset " + primaryIndexName);
        }
        ARecordType itemType = (ARecordType) metadata.findType(compiledDatasetDecl.getItemTypeName());
        ISerializerDeserializerProvider serdeProvider = metadata.getFormat().getSerdeProvider();
        ISerializerDeserializer payloadSerde = serdeProvider.getSerializerDeserializer(itemType);

        if (compiledDatasetDecl.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AsterixException("Cannot index an external dataset (" + primaryIndexName + ").");
        }
        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(compiledDatasetDecl).size();

        // ---------- START GENERAL BTREE STUFF

        IIndexRegistryProvider<IIndex> treeRegistryProvider = AsterixTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;

        // ---------- END GENERAL BTREE STUFF

        // ---------- START KEY PROVIDER OP

        // TODO: should actually be empty tuple source
        // build tuple containing low and high search keys
        ArrayTupleBuilder tb = new ArrayTupleBuilder(1); // just one dummy field
        DataOutput dos = tb.getDataOutput();

        tb.reset();
        try {
            IntegerSerializerDeserializer.INSTANCE.serialize(0, dos);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        } // dummy field
        tb.addFieldEndOffset();

        ISerializerDeserializer[] keyRecDescSers = { IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> keyProviderSplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(primaryIndexName, primaryIndexName);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, keyProviderOp,
                keyProviderSplitsAndConstraint.second);

        // ---------- END KEY PROVIDER OP

        // ---------- START PRIMARY INDEX SCAN

        ISerializerDeserializer[] primaryRecFields = new ISerializerDeserializer[numPrimaryKeys + 1];
        IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[numPrimaryKeys];
        ITypeTraits[] primaryTypeTraits = new ITypeTraits[numPrimaryKeys + 1];
        int i = 0;
        serdeProvider = metadata.getFormat().getSerdeProvider();
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType : DatasetUtils
                .getPartitioningFunctions(compiledDatasetDecl)) {
            IAType keyType = evalFactoryAndType.third;
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(keyType);
            primaryRecFields[i] = keySerde;
            primaryComparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                    keyType, OrderKind.ASC);
            primaryTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            ++i;
        }
        primaryRecFields[numPrimaryKeys] = payloadSerde;
        primaryTypeTraits[numPrimaryKeys] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(itemType);

        ITreeIndexFrameFactory primaryInteriorFrameFactory = AqlMetadataProvider
                .createBTreeNSMInteriorFrameFactory(primaryTypeTraits);
        ITreeIndexFrameFactory primaryLeafFrameFactory = AqlMetadataProvider
                .createBTreeNSMLeafFrameFactory(primaryTypeTraits);

        int[] lowKeyFields = null; // -infinity
        int[] highKeyFields = null; // +infinity
        RecordDescriptor primaryRecDesc = new RecordDescriptor(primaryRecFields);

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> primarySplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(primaryIndexName, primaryIndexName);

        BTreeSearchOperatorDescriptor primarySearchOp = new BTreeSearchOperatorDescriptor(spec, primaryRecDesc,
                storageManager, treeRegistryProvider, primarySplitsAndConstraint.first, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits, primaryComparatorFactories, true, lowKeyFields,
                highKeyFields, true, true, new BTreeDataflowHelperFactory());

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, primarySearchOp,
                primarySplitsAndConstraint.second);

        // ---------- END PRIMARY INDEX SCAN

        // ---------- START ASSIGN OP

        List<String> secondaryKeyFields = createIndexStmt.getKeyFields();
        int numSecondaryKeys = secondaryKeyFields.size();

        if (numSecondaryKeys != 1) {
            throw new AsterixException(
                    "Cannot use "
                            + numSecondaryKeys
                            + " fields as a key for the R-tree index. There can be only one field as a key for the R-tree index.");
        }

        IAType spatialType = AqlCompiledIndexDecl.keyFieldType(secondaryKeyFields.get(0), itemType);
        if (spatialType == null) {
            throw new AsterixException("Could not find field " + secondaryKeyFields.get(0) + " in the schema.");
        }

        int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numNestedSecondaryKeyFields = dimension * 2;

        IEvaluatorFactory[] evalFactories = metadata.getFormat().createMBRFactory(itemType, secondaryKeyFields.get(0),
                numPrimaryKeys, dimension);

        ISerializerDeserializer[] secondaryRecFields = new ISerializerDeserializer[numPrimaryKeys
                + numNestedSecondaryKeyFields];
        IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[numNestedSecondaryKeyFields];
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numNestedSecondaryKeyFields + numPrimaryKeys];
        IPrimitiveValueProviderFactory[] valueProviderFactories = new IPrimitiveValueProviderFactory[numNestedSecondaryKeyFields];

        IAType keyType = AqlCompiledIndexDecl.keyFieldType(secondaryKeyFields.get(0), itemType);
        IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());
        for (i = 0; i < numNestedSecondaryKeyFields; i++) {
            ISerializerDeserializer keySerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(nestedKeyType);
            secondaryRecFields[i] = keySerde;
            secondaryComparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                    nestedKeyType, OrderKind.ASC);
            secondaryTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(nestedKeyType);
            valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
        }

        // fill in serializers and comparators for primary index fields
        for (i = 0; i < numPrimaryKeys; i++) {
            secondaryRecFields[numNestedSecondaryKeyFields + i] = primaryRecFields[i];
            secondaryTypeTraits[numNestedSecondaryKeyFields + i] = primaryTypeTraits[i];
        }
        RecordDescriptor secondaryRecDesc = new RecordDescriptor(secondaryRecFields);

        int[] outColumns = new int[numNestedSecondaryKeyFields];
        int[] projectionList = new int[numNestedSecondaryKeyFields + numPrimaryKeys];
        for (i = 0; i < numNestedSecondaryKeyFields; i++) {
            outColumns[i] = numPrimaryKeys + i + 1;
        }
        int projCount = 0;
        for (i = 0; i < numNestedSecondaryKeyFields; i++) {
            projectionList[projCount++] = numPrimaryKeys + i + 1;
        }
        for (i = 0; i < numPrimaryKeys; i++) {
            projectionList[projCount++] = i;
        }

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> assignSplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(primaryIndexName, primaryIndexName);

        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, evalFactories, projectionList);
        AlgebricksMetaOperatorDescriptor asterixAssignOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { secondaryRecDesc });

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixAssignOp,
                assignSplitsAndConstraint.second);

        // ---------- END ASSIGN OP

        // ---------- START SECONDARY INDEX BULK LOAD

        /*
        ITreeIndexFrameFactory secondaryInteriorFrameFactory = JobGenHelper.createRTreeNSMInteriorFrameFactory(
                secondaryTypeTraits, numNestedSecondaryKeyFields);
        ITreeIndexFrameFactory secondaryLeafFrameFactory = JobGenHelper.createRTreeNSMLeafFrameFactory(
                secondaryTypeTraits, numNestedSecondaryKeyFields);
        */

        ITreeIndexFrameFactory secondaryInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(
                new RTreeTypeAwareTupleWriterFactory(secondaryTypeTraits), valueProviderFactories);
        ITreeIndexFrameFactory secondaryLeafFrameFactory = new RTreeNSMLeafFrameFactory(
                new RTreeTypeAwareTupleWriterFactory(secondaryTypeTraits), valueProviderFactories);

        int[] fieldPermutation = new int[numNestedSecondaryKeyFields + numPrimaryKeys];
        for (i = 0; i < numNestedSecondaryKeyFields + numPrimaryKeys; i++)
            fieldPermutation[i] = i;

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(primaryIndexName, secondaryIndexName);

        // GlobalConfig.DEFAULT_BTREE_FILL_FACTOR
        TreeIndexBulkLoadOperatorDescriptor secondaryBulkLoadOp = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManager, treeRegistryProvider, secondarySplitsAndConstraint.first,
                secondaryInteriorFrameFactory, secondaryLeafFrameFactory, secondaryTypeTraits,
                secondaryComparatorFactories, fieldPermutation, 0.7f, new RTreeDataflowHelperFactory());

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, secondaryBulkLoadOp,
                secondarySplitsAndConstraint.second);

        // ---------- END SECONDARY INDEX BULK LOAD

        // ---------- START CONNECT THE OPERATORS

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primarySearchOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), primarySearchOp, 0, asterixAssignOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, secondaryBulkLoadOp, 0);

        spec.addRoot(secondaryBulkLoadOp);

        // ---------- END CONNECT THE OPERATORS

        return spec;

    }

    @SuppressWarnings("unchecked")
    public static JobSpecification createKeywordIndexJobSpec(CompiledCreateIndexStatement createIndexStmt,
            AqlCompiledMetadataDeclarations datasetDecls) throws AsterixException, AlgebricksException {

        JobSpecification spec = new JobSpecification();

        String primaryIndexName = createIndexStmt.getDatasetName();
        String secondaryIndexName = createIndexStmt.getIndexName();

        AqlCompiledDatasetDecl compiledDatasetDecl = datasetDecls.findDataset(primaryIndexName);
        if (compiledDatasetDecl == null) {
            throw new AsterixException("Could not find dataset " + primaryIndexName);
        }

        if (compiledDatasetDecl.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AsterixException("Cannot index an external dataset (" + primaryIndexName + ").");
        }
        ARecordType itemType = (ARecordType) datasetDecls.findType(compiledDatasetDecl.getItemTypeName());
        ISerializerDeserializerProvider serdeProvider = datasetDecls.getFormat().getSerdeProvider();
        ISerializerDeserializer payloadSerde = serdeProvider.getSerializerDeserializer(itemType);

        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(compiledDatasetDecl).size();

        // sanity
        if (numPrimaryKeys > 1)
            throw new AsterixException("Cannot create inverted keyword index on dataset with composite primary key.");

        // sanity
        IAType fieldsToTokenizeType = AqlCompiledIndexDecl
                .keyFieldType(createIndexStmt.getKeyFields().get(0), itemType);
        for (String fieldName : createIndexStmt.getKeyFields()) {
            IAType nextFieldToTokenizeType = AqlCompiledIndexDecl.keyFieldType(fieldName, itemType);
            if (nextFieldToTokenizeType.getTypeTag() != fieldsToTokenizeType.getTypeTag()) {
                throw new AsterixException(
                        "Cannot create inverted keyword index. Fields to tokenize must be of the same type.");
            }
        }

        // ---------- START GENERAL BTREE STUFF

        IIndexRegistryProvider<IIndex> treeRegistryProvider = AsterixTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;

        // ---------- END GENERAL BTREE STUFF

        // ---------- START KEY PROVIDER OP

        // TODO: should actually be empty tuple source
        // build tuple containing low and high search keys
        ArrayTupleBuilder tb = new ArrayTupleBuilder(1); // just one dummy field
        DataOutput dos = tb.getDataOutput();

        try {
            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(0, dos); // dummy
            // field
            tb.addFieldEndOffset();
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }

        ISerializerDeserializer[] keyRecDescSers = { IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> keyProviderSplitsAndConstraint = datasetDecls
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(primaryIndexName, primaryIndexName);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, keyProviderOp,
                keyProviderSplitsAndConstraint.second);

        // ---------- END KEY PROVIDER OP

        // ---------- START PRIMARY INDEX SCAN

        ISerializerDeserializer[] primaryRecFields = new ISerializerDeserializer[numPrimaryKeys + 1];
        IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[numPrimaryKeys];
        ITypeTraits[] primaryTypeTraits = new ITypeTraits[numPrimaryKeys + 1];
        int i = 0;
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType : DatasetUtils
                .getPartitioningFunctions(compiledDatasetDecl)) {
            IAType keyType = evalFactoryAndType.third;
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(keyType);
            primaryRecFields[i] = keySerde;
            primaryComparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                    keyType, OrderKind.ASC);
            primaryTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            ++i;
        }
        primaryRecFields[numPrimaryKeys] = payloadSerde;
        primaryTypeTraits[numPrimaryKeys] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(itemType);

        ITreeIndexFrameFactory primaryInteriorFrameFactory = AqlMetadataProvider
                .createBTreeNSMInteriorFrameFactory(primaryTypeTraits);
        ITreeIndexFrameFactory primaryLeafFrameFactory = AqlMetadataProvider
                .createBTreeNSMLeafFrameFactory(primaryTypeTraits);

        int[] lowKeyFields = null; // -infinity
        int[] highKeyFields = null; // +infinity
        RecordDescriptor primaryRecDesc = new RecordDescriptor(primaryRecFields);

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> primarySplitsAndConstraint = datasetDecls
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(primaryIndexName, primaryIndexName);

        BTreeSearchOperatorDescriptor primarySearchOp = new BTreeSearchOperatorDescriptor(spec, primaryRecDesc,
                storageManager, treeRegistryProvider, primarySplitsAndConstraint.first, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits, primaryComparatorFactories, true, lowKeyFields,
                highKeyFields, true, true, new BTreeDataflowHelperFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, primarySearchOp,
                primarySplitsAndConstraint.second);

        // ---------- END PRIMARY INDEX SCAN

        // ---------- START ASSIGN OP

        List<String> secondaryKeyFields = createIndexStmt.getKeyFields();
        int numSecondaryKeys = secondaryKeyFields.size();
        ISerializerDeserializer[] secondaryRecFields = new ISerializerDeserializer[numPrimaryKeys + numSecondaryKeys];
        IEvaluatorFactory[] evalFactories = new IEvaluatorFactory[numSecondaryKeys];
        for (i = 0; i < numSecondaryKeys; i++) {
            evalFactories[i] = datasetDecls.getFormat().getFieldAccessEvaluatorFactory(itemType,
                    secondaryKeyFields.get(i), numPrimaryKeys);
            IAType keyType = AqlCompiledIndexDecl.keyFieldType(secondaryKeyFields.get(i), itemType);
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(keyType);
            secondaryRecFields[i] = keySerde;
        }
        // fill in serializers and comparators for primary index fields
        for (i = 0; i < numPrimaryKeys; i++) {
            secondaryRecFields[numSecondaryKeys + i] = primaryRecFields[i];
        }
        RecordDescriptor secondaryRecDesc = new RecordDescriptor(secondaryRecFields);

        int[] outColumns = new int[numSecondaryKeys];
        int[] projectionList = new int[numSecondaryKeys + numPrimaryKeys];
        for (i = 0; i < numSecondaryKeys; i++) {
            outColumns[i] = numPrimaryKeys + i + 1;
        }
        int projCount = 0;
        for (i = 0; i < numSecondaryKeys; i++) {
            projectionList[projCount++] = numPrimaryKeys + i + 1;
        }
        for (i = 0; i < numPrimaryKeys; i++) {
            projectionList[projCount++] = i;
        }

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> assignSplitsAndConstraint = datasetDecls
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(primaryIndexName, primaryIndexName);

        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, evalFactories, projectionList);
        AlgebricksMetaOperatorDescriptor asterixAssignOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { secondaryRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixAssignOp,
                assignSplitsAndConstraint.second);

        // ---------- END ASSIGN OP

        // ---------- START TOKENIZER OP

        int numTokenKeyPairFields = numPrimaryKeys + 1;

        ISerializerDeserializer[] tokenKeyPairFields = new ISerializerDeserializer[numTokenKeyPairFields];
        tokenKeyPairFields[0] = serdeProvider.getSerializerDeserializer(fieldsToTokenizeType);
        for (i = 0; i < numPrimaryKeys; i++)
            tokenKeyPairFields[i + 1] = secondaryRecFields[numSecondaryKeys + i];
        RecordDescriptor tokenKeyPairRecDesc = new RecordDescriptor(tokenKeyPairFields);

        int[] fieldsToTokenize = new int[numSecondaryKeys];
        for (i = 0; i < numSecondaryKeys; i++)
            fieldsToTokenize[i] = i;

        int[] primaryKeyFields = new int[numPrimaryKeys];
        for (i = 0; i < numPrimaryKeys; i++)
            primaryKeyFields[i] = numSecondaryKeys + i;

        IBinaryTokenizerFactory tokenizerFactory = AqlBinaryTokenizerFactoryProvider.INSTANCE
                .getTokenizerFactory(fieldsToTokenizeType);
        BinaryTokenizerOperatorDescriptor tokenizerOp = new BinaryTokenizerOperatorDescriptor(spec,
                tokenKeyPairRecDesc, tokenizerFactory, fieldsToTokenize, primaryKeyFields);
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint = datasetDecls
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(primaryIndexName, secondaryIndexName);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, tokenizerOp,
                secondarySplitsAndConstraint.second);

        // ---------- END TOKENIZER OP

        // ---------- START EXTERNAL SORT OP

        IBinaryComparatorFactory[] tokenKeyPairComparatorFactories = new IBinaryComparatorFactory[numTokenKeyPairFields];
        tokenKeyPairComparatorFactories[0] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                fieldsToTokenizeType, OrderKind.ASC);
        for (i = 0; i < numPrimaryKeys; i++)
            tokenKeyPairComparatorFactories[i + 1] = primaryComparatorFactories[i];

        int[] sortFields = new int[numTokenKeyPairFields]; // <token, primary
        // key a, primary
        // key b, etc.>
        for (i = 0; i < numTokenKeyPairFields; i++)
            sortFields[i] = i;

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> sorterSplitsAndConstraint = datasetDecls
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(primaryIndexName, primaryIndexName);

        ExternalSortOperatorDescriptor sortOp = new ExternalSortOperatorDescriptor(spec,
                physicalOptimizationConfig.getMaxFramesExternalSort(), sortFields, tokenKeyPairComparatorFactories,
                secondaryRecDesc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp,
                sorterSplitsAndConstraint.second);

        // ---------- END EXTERNAL SORT OP

        // ---------- START SECONDARY INDEX BULK LOAD

        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numTokenKeyPairFields];
        secondaryTypeTraits[0] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(fieldsToTokenizeType);
        for (i = 0; i < numPrimaryKeys; i++)
            secondaryTypeTraits[i + 1] = primaryTypeTraits[i];

        ITreeIndexFrameFactory secondaryInteriorFrameFactory = AqlMetadataProvider
                .createBTreeNSMInteriorFrameFactory(secondaryTypeTraits);
        ITreeIndexFrameFactory secondaryLeafFrameFactory = AqlMetadataProvider
                .createBTreeNSMLeafFrameFactory(secondaryTypeTraits);

        int[] fieldPermutation = new int[numSecondaryKeys + numPrimaryKeys];
        for (i = 0; i < numTokenKeyPairFields; i++)
            fieldPermutation[i] = i;

        TreeIndexBulkLoadOperatorDescriptor secondaryBulkLoadOp = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManager, treeRegistryProvider, secondarySplitsAndConstraint.first,
                secondaryInteriorFrameFactory, secondaryLeafFrameFactory, secondaryTypeTraits,
                tokenKeyPairComparatorFactories, fieldPermutation, 0.7f, new BTreeDataflowHelperFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, secondaryBulkLoadOp,
                secondarySplitsAndConstraint.second);

        // ---------- END SECONDARY INDEX BULK LOAD

        // ---------- START CONNECT THE OPERATORS

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primarySearchOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), primarySearchOp, 0, asterixAssignOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, tokenizerOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), tokenizerOp, 0, sortOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);

        spec.addRoot(secondaryBulkLoadOp);

        // ---------- END CONNECT THE OPERATORS

        return spec;
    }

    public static void main(String[] args) throws Exception {
        String host;
        String appName;
        String ddlFile;

        switch (args.length) {
            case 0: {
                host = "127.0.0.1";
                appName = "asterix";
                ddlFile = "/home/abehm/workspace/asterix/asterix-app/src/test/resources/demo0927/local/create-index.aql";
                System.out.println("No arguments specified, using defauls:");
                System.out.println("HYRACKS HOST: " + host);
                System.out.println("APPNAME:      " + appName);
                System.out.println("DDLFILE:      " + ddlFile);
            }
                break;

            case 3: {
                host = args[0];
                appName = args[1];
                ddlFile = args[2];
            }
                break;

            default: {
                System.out.println("USAGE:");
                System.out.println("ARG 1: Hyracks Host (IP or Hostname)");
                System.out.println("ARG 2: Application Name (e.g., asterix)");
                System.out.println("ARG 3: DDL File");
                host = null;
                appName = null;
                ddlFile = null;
                System.exit(0);
            }
                break;

        }

        // int port = HyracksIntegrationUtil.DEFAULT_HYRACKS_CC_PORT;

        // AsterixJavaClient q = compileQuery(ddlFile, true, false, true);

        // long start = System.currentTimeMillis();
        // q.execute(port);
        // long end = System.currentTimeMillis();
        // System.err.println(start + " " + end + " " + (end - start));
    }
}
