package edu.uci.ics.asterix.file;

import java.util.List;

import edu.uci.ics.asterix.common.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.common.context.AsterixTreeRegistryProvider;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledCreateIndexStatement;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.ISerializerDeserializerProvider;
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
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackProvider;

@SuppressWarnings("rawtypes")
public class SecondaryBTreeCreator extends SecondaryIndexCreator {
    
    protected SecondaryBTreeCreator(PhysicalOptimizationConfig physOptConf) {
        super(physOptConf);
    }

    @Override
    public JobSpecification createJobSpec(CompiledCreateIndexStatement createIndexStmt,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException, AlgebricksException {
        init(createIndexStmt, metadata);
        JobSpecification spec = new JobSpecification();
        
        // Create dummy key provider for feeding the primary index scan. 
        AbstractOperatorDescriptor keyProviderOp = createDummyKeyProviderOp(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(
                spec, keyProviderOp, primaryPartitionConstrant);
        
        // Create primary index scan op.
        BTreeSearchOperatorDescriptor primaryScanOp = createPrimaryIndexScanOp(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(
                spec, keyProviderOp, primaryPartitionConstrant);
        
        // ---------- START ASSIGN OP

        List<String> secondaryKeyFields = createIndexStmt.getKeyFields();
        int numSecondaryKeys = secondaryKeyFields.size();
        ISerializerDeserializer[] secondaryRecFields = new ISerializerDeserializer[numPrimaryKeys + numSecondaryKeys];
        IEvaluatorFactory[] evalFactories = new IEvaluatorFactory[numSecondaryKeys];
        IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys
                + numPrimaryKeys];
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = metadata.getFormat()
                .getSerdeProvider();
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
        // Fill in serializers and comparators for primary index fields.
        RecordDescriptor primaryRecDesc = primaryScanOp.getRecordDescriptor();
        IBinaryComparatorFactory[] primaryComparatorFactories = primaryScanOp.getTreeIndexComparatorFactories();
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryRecFields[numSecondaryKeys + i] = primaryRecDesc.getFields()[i];
            secondaryTypeTraits[numSecondaryKeys + i] = primaryRecDesc.getTypeTraits()[i];
            secondaryComparatorFactories[numSecondaryKeys + i] = primaryComparatorFactories[i];
        }
        RecordDescriptor secondaryRecDesc = new RecordDescriptor(secondaryRecFields);

        int[] outColumns = new int[numSecondaryKeys];
        int[] projectionList = new int[numSecondaryKeys + numPrimaryKeys];
        for (int i = 0; i < numSecondaryKeys; i++) {
            outColumns[i] = numPrimaryKeys + i + 1;
        }
        int projCount = 0;
        for (int i = 0; i < numSecondaryKeys; i++) {
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

        // Sort by secondary keys.
        ExternalSortOperatorDescriptor sortOp = createSortOp(spec, secondaryComparatorFactories, secondaryRecDesc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp, primaryPartitionConstrant);

        // ---------- START SECONDARY INDEX BULK LOAD

        int[] fieldPermutation = new int[numSecondaryKeys + numPrimaryKeys];
        for (int i = 0; i < numSecondaryKeys + numPrimaryKeys; i++) {
            fieldPermutation[i] = i;
        }
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, secondaryIndexName);
        TreeIndexBulkLoadOperatorDescriptor secondaryBulkLoadOp = new TreeIndexBulkLoadOperatorDescriptor(
                spec, AsterixStorageManagerInterface.INSTANCE, AsterixTreeRegistryProvider.INSTANCE,
                secondarySplitsAndConstraint.first, secondaryTypeTraits,
                secondaryComparatorFactories, fieldPermutation, 0.7f,
                new BTreeDataflowHelperFactory(),
                NoOpOperationCallbackProvider.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, secondaryBulkLoadOp,
                secondarySplitsAndConstraint.second);

        // ---------- END SECONDARY INDEX BULK LOAD

        // Connect the operators.
        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryScanOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, asterixAssignOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, sortOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);
        spec.addRoot(secondaryBulkLoadOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;

    }
}
