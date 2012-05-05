package edu.uci.ics.asterix.file;

import java.io.DataOutput;
import java.util.List;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.common.context.AsterixTreeRegistryProvider;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledCreateIndexStatement;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IPushRuntimeFactory;
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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackProvider;

@SuppressWarnings("rawtypes")
public abstract class SecondaryIndexCreator {
    protected final PhysicalOptimizationConfig physOptConf;
    
    protected int numPrimaryKeys;
    protected int numSecondaryKeys;
    protected AqlCompiledMetadataDeclarations metadata;
    protected String datasetName;
    protected AqlCompiledDatasetDecl datasetDecl;
    protected ARecordType itemType;    
    protected ISerializerDeserializer payloadSerde;
    protected IFileSplitProvider primaryFileSplitProvider;
    protected AlgebricksPartitionConstraint primaryPartitionConstraint;
    protected String secondaryIndexName;

    protected IBinaryComparatorFactory[] primaryComparatorFactories;
    protected RecordDescriptor primaryRecDesc;
    protected IBinaryComparatorFactory[] secondaryComparatorFactories;
    protected RecordDescriptor secondaryRecDesc;
    protected IEvaluatorFactory[] evalFactories;
    
    // Prevent public construction.
    protected SecondaryIndexCreator(PhysicalOptimizationConfig physOptConf) {
        this.physOptConf = physOptConf;
    }

    public static SecondaryIndexCreator createIndexCreator(IndexType indexType, PhysicalOptimizationConfig physOptConf) throws AsterixException {
        switch (indexType) {
            case BTREE: {
                return new SecondaryBTreeCreator(physOptConf);
            }
            case RTREE: {
                return new SecondaryRTreeCreator(physOptConf);
            }
            case KEYWORD: {
                return new SecondaryInvertedIndexCreator(physOptConf);
            }
            default: {
                throw new AsterixException("Unknown Index Type: " + indexType);
            }
        }
    }
    
    public abstract JobSpecification createJobSpec(CompiledCreateIndexStatement createIndexStmt,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException, AlgebricksException;
    
    protected void init(CompiledCreateIndexStatement createIndexStmt, AqlCompiledMetadataDeclarations metadata) throws AsterixException, AlgebricksException {
        this.metadata = metadata;
        datasetName = createIndexStmt.getDatasetName();
        secondaryIndexName = createIndexStmt.getIndexName();
        datasetDecl = metadata.findDataset(datasetName);
        if (datasetDecl == null) {
            throw new AsterixException("Unknown dataset " + datasetName);
        }
        if (datasetDecl.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AsterixException("Cannot index an external dataset (" + datasetName + ").");
        }
        itemType = (ARecordType) metadata.findType(datasetDecl.getItemTypeName());
        payloadSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(itemType);
        numPrimaryKeys = DatasetUtils.getPartitioningFunctions(datasetDecl).size();
        numSecondaryKeys = createIndexStmt.getKeyFields().size();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, datasetName);
        primaryFileSplitProvider = splitProviderAndConstraint.first;
        primaryPartitionConstraint = splitProviderAndConstraint.second;
        // Must be called in this order.
        setPrimaryRecDescAndComparators();
        setSecondaryRecDescAndComparators(createIndexStmt.getKeyFields());
    }
    
    protected void setPrimaryRecDescAndComparators() throws AlgebricksException {
        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(datasetDecl).size();
        ISerializerDeserializer[] primaryRecFields = new ISerializerDeserializer[numPrimaryKeys + 1];
        ITypeTraits[] primaryTypeTraits = new ITypeTraits[numPrimaryKeys + 1];
        primaryComparatorFactories = new IBinaryComparatorFactory[numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = metadata.getFormat().getSerdeProvider();
        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(datasetDecl);
        int i = 0;
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
        primaryRecDesc = new RecordDescriptor(primaryRecFields, primaryTypeTraits);
    }
    
    protected void setSecondaryRecDescAndComparators(List<String> secondaryKeyFields) throws AlgebricksException, AsterixException {
        evalFactories = new IEvaluatorFactory[numSecondaryKeys];
        secondaryComparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys + numPrimaryKeys];
        ISerializerDeserializer[] secondaryRecFields = new ISerializerDeserializer[numPrimaryKeys + numSecondaryKeys];
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = metadata.getFormat().getSerdeProvider();
        for (int i = 0; i < numSecondaryKeys; i++) {
            evalFactories[i] = metadata.getFormat().getFieldAccessEvaluatorFactory(itemType, secondaryKeyFields.get(i),
                    numPrimaryKeys);
            IAType keyType = AqlCompiledIndexDecl.keyFieldType(secondaryKeyFields.get(i), itemType);
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(keyType);
            secondaryRecFields[i] = keySerde;
            secondaryComparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                    keyType, OrderKind.ASC);
            secondaryTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
        }
        // Add serializers and comparators for primary index fields.
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryRecFields[numSecondaryKeys + i] = primaryRecDesc.getFields()[i];
            secondaryTypeTraits[numSecondaryKeys + i] = primaryRecDesc.getTypeTraits()[i];
            secondaryComparatorFactories[numSecondaryKeys + i] = primaryComparatorFactories[i];
        }
        secondaryRecDesc = new RecordDescriptor(secondaryRecFields, secondaryTypeTraits);
    }
    
    protected AbstractOperatorDescriptor createDummyKeyProviderOp(
            JobSpecification spec) throws AsterixException, AlgebricksException {
        // Build dummy tuple containing one field with a dummy value inside.
        ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        DataOutput dos = tb.getDataOutput();
        tb.reset();
        try {
            // Serialize dummy value into a field.
            IntegerSerializerDeserializer.INSTANCE.serialize(0, dos);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
        // Add dummy field.
        tb.addFieldEndOffset();
        ISerializerDeserializer[] keyRecDescSers = { IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);
        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(
                spec, keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(),
                tb.getSize());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(
                spec, keyProviderOp, primaryPartitionConstraint);
        return keyProviderOp;
    }
    
    protected BTreeSearchOperatorDescriptor createPrimaryIndexScanOp(JobSpecification spec) throws AlgebricksException {
        // -Infinity
        int[] lowKeyFields = null;
        // +Infinity
        int[] highKeyFields = null;
        BTreeSearchOperatorDescriptor primarySearchOp = new BTreeSearchOperatorDescriptor(spec, primaryRecDesc,
                AsterixStorageManagerInterface.INSTANCE, AsterixTreeRegistryProvider.INSTANCE,
                primaryFileSplitProvider, primaryRecDesc.getTypeTraits(), primaryComparatorFactories, lowKeyFields,
                highKeyFields, true, true, new BTreeDataflowHelperFactory(), NoOpOperationCallbackProvider.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, primarySearchOp,
                primaryPartitionConstraint);
        return primarySearchOp;
    }
    
    protected AlgebricksMetaOperatorDescriptor createAssignOp(JobSpecification spec,
            BTreeSearchOperatorDescriptor primaryScanOp, int numSecondaryKeyFields) throws AlgebricksException {
        int[] outColumns = new int[numSecondaryKeyFields];
        int[] projectionList = new int[numSecondaryKeyFields + numPrimaryKeys];
        for (int i = 0; i < numSecondaryKeyFields; i++) {
            outColumns[i] = numPrimaryKeys + i + 1;
        }
        int projCount = 0;
        for (int i = 0; i < numSecondaryKeyFields; i++) {
            projectionList[projCount++] = numPrimaryKeys + i + 1;
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            projectionList[projCount++] = i;
        }

        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, evalFactories, projectionList);
        AlgebricksMetaOperatorDescriptor asterixAssignOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { secondaryRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixAssignOp,
                primaryPartitionConstraint);
        return asterixAssignOp;
    }
    
    protected ExternalSortOperatorDescriptor createSortOp(JobSpecification spec,
            IBinaryComparatorFactory[] secondaryComparatorFactories, RecordDescriptor secondaryRecDesc) {
        int[] sortFields = new int[secondaryComparatorFactories.length];
        for (int i = 0; i < secondaryComparatorFactories.length; i++) {
            sortFields[i] = i;
        }
        ExternalSortOperatorDescriptor sortOp = new ExternalSortOperatorDescriptor(spec,
                physOptConf.getMaxFramesExternalSort(), sortFields, secondaryComparatorFactories, secondaryRecDesc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp,
                primaryPartitionConstraint);
        return sortOp;
    }
    
    protected TreeIndexBulkLoadOperatorDescriptor createTreeIndexBulkLoadOp(JobSpecification spec,
            int numSecondaryKeyFields, IIndexDataflowHelperFactory dataflowHelperFactory, float fillFactor) throws MetadataException,
            AlgebricksException {
        int[] fieldPermutation = new int[numSecondaryKeyFields + numPrimaryKeys];
        for (int i = 0; i < numSecondaryKeyFields + numPrimaryKeys; i++) {
            fieldPermutation[i] = i;
        }
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, secondaryIndexName);
        TreeIndexBulkLoadOperatorDescriptor treeIndexBulkLoadOp = new TreeIndexBulkLoadOperatorDescriptor(spec,
                AsterixStorageManagerInterface.INSTANCE, AsterixTreeRegistryProvider.INSTANCE,
                secondarySplitsAndConstraint.first, secondaryRecDesc.getTypeTraits(), secondaryComparatorFactories,
                fieldPermutation, fillFactor, dataflowHelperFactory, NoOpOperationCallbackProvider.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, treeIndexBulkLoadOp,
                secondarySplitsAndConstraint.second);
        return treeIndexBulkLoadOp;
    }
}
