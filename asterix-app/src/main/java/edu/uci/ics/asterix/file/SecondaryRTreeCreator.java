package edu.uci.ics.asterix.file;

import java.util.List;

import edu.uci.ics.asterix.common.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.common.context.AsterixTreeRegistryProvider;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.valueproviders.AqlPrimitiveValueProviderFactory;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledCreateIndexStatement;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.std.AssignRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeDataflowHelperFactory;

@SuppressWarnings("rawtypes")
public class SecondaryRTreeCreator extends SecondaryIndexCreator {

    protected SecondaryRTreeCreator(PhysicalOptimizationConfig physOptConf) {
        super(physOptConf);
    }

    @Override
    public JobSpecification createJobSpec(CompiledCreateIndexStatement createIndexStmt,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException, AlgebricksException {
        init(createIndexStmt, metadata);
        JobSpecification spec = new JobSpecification();
        
        // Create dummy key provider for feeding the primary index scan. 
        AbstractOperatorDescriptor keyProviderOp = createDummyKeyProviderOp(spec);
        
        // Create primary index scan op.
        BTreeSearchOperatorDescriptor primaryScanOp = createPrimaryIndexScanOp(spec);

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
        for (int i = 0; i < numNestedSecondaryKeyFields; i++) {
            ISerializerDeserializer keySerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(nestedKeyType);
            secondaryRecFields[i] = keySerde;
            secondaryComparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                    nestedKeyType, OrderKind.ASC);
            secondaryTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(nestedKeyType);
            valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
        }

        // Fill in serializers and comparators for primary index fields.
        RecordDescriptor primaryRecDesc = primaryScanOp.getRecordDescriptor();
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryRecFields[numNestedSecondaryKeyFields + i] = primaryRecDesc.getFields()[i];
            secondaryTypeTraits[numNestedSecondaryKeyFields + i] = primaryRecDesc.getTypeTraits()[i];
        }
        RecordDescriptor secondaryRecDesc = new RecordDescriptor(secondaryRecFields);

        int[] outColumns = new int[numNestedSecondaryKeyFields];
        int[] projectionList = new int[numNestedSecondaryKeyFields + numPrimaryKeys];
        for (int i = 0; i < numNestedSecondaryKeyFields; i++) {
            outColumns[i] = numPrimaryKeys + i + 1;
        }
        int projCount = 0;
        for (int i = 0; i < numNestedSecondaryKeyFields; i++) {
            projectionList[projCount++] = numPrimaryKeys + i + 1;
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            projectionList[projCount++] = i;
        }

        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, evalFactories, projectionList);
        AlgebricksMetaOperatorDescriptor asterixAssignOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { secondaryRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixAssignOp,
                primaryPartitionConstrant);

        // ---------- END ASSIGN OP

        // ---------- START SECONDARY INDEX BULK LOAD

        int[] fieldPermutation = new int[numNestedSecondaryKeyFields + numPrimaryKeys];
        for (int i = 0; i < numNestedSecondaryKeyFields + numPrimaryKeys; i++) {
            fieldPermutation[i] = i;
        }

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, secondaryIndexName);

        TreeIndexBulkLoadOperatorDescriptor secondaryBulkLoadOp = new TreeIndexBulkLoadOperatorDescriptor(
                spec, AsterixStorageManagerInterface.INSTANCE, AsterixTreeRegistryProvider.INSTANCE,
                secondarySplitsAndConstraint.first, secondaryTypeTraits,
                secondaryComparatorFactories, fieldPermutation, 0.7f,
                new RTreeDataflowHelperFactory(valueProviderFactories),
                NoOpOperationCallbackProvider.INSTANCE);

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, secondaryBulkLoadOp,
                secondarySplitsAndConstraint.second);

        // ---------- END SECONDARY INDEX BULK LOAD

        // Connect the operators.
        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryScanOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, asterixAssignOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, secondaryBulkLoadOp, 0);
        spec.addRoot(secondaryBulkLoadOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

}
