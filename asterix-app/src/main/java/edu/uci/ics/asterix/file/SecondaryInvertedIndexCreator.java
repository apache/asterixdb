package edu.uci.ics.asterix.file;

import java.util.List;

import edu.uci.ics.asterix.common.context.AsterixIndexRegistryProvider;
import edu.uci.ics.asterix.common.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.optimizer.rules.am.InvertedIndexAccessMethod;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledCreateIndexStatement;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.algebricks.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
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
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.dataflow.InvertedIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.dataflow.InvertedIndexCreateOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;

public class SecondaryInvertedIndexCreator extends SecondaryIndexCreator {

    private IAType secondaryKeyType;
    private ITypeTraits[] invListsTypeTraits;
    private IBinaryComparatorFactory[] tokenComparatorFactories;
    private ITypeTraits[] tokenTypeTraits;
    private IBinaryTokenizerFactory tokenizerFactory;
    private Pair<IFileSplitProvider, IFileSplitProvider> fileSplitProviders;
    // For tokenization, sorting and loading. Represents <token, primary keys>.
    private int numTokenKeyPairFields;
    private IBinaryComparatorFactory[] tokenKeyPairComparatorFactories;
    private RecordDescriptor tokenKeyPairRecDesc;

    protected SecondaryInvertedIndexCreator(PhysicalOptimizationConfig physOptConf) {
        super(physOptConf);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected void setSecondaryRecDescAndComparators(CompiledCreateIndexStatement createIndexStmt)
            throws AlgebricksException, AsterixException {
        // Sanity checks.
        if (numPrimaryKeys > 1) {
            throw new AsterixException("Cannot create inverted index on dataset with composite primary key.");
        }
        if (numSecondaryKeys > 1) {
            throw new AsterixException("Cannot create composite inverted index on multiple fields.");
        }
        // Prepare record descriptor used in the assign op, and the optional select op.
        List<String> secondaryKeyFields = createIndexStmt.getKeyFields();
        secondaryFieldAccessEvalFactories = new ICopyEvaluatorFactory[numSecondaryKeys];
        ISerializerDeserializer[] secondaryRecFields = new ISerializerDeserializer[numPrimaryKeys + numSecondaryKeys];
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = metadata.getFormat().getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = metadata.getFormat().getTypeTraitProvider();
        for (int i = 0; i < numSecondaryKeys; i++) {
            secondaryFieldAccessEvalFactories[i] = metadata.getFormat().getFieldAccessEvaluatorFactory(itemType,
                    secondaryKeyFields.get(i), numPrimaryKeys);
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableKeyFieldType(secondaryKeyFields.get(i), itemType);
            secondaryKeyType = keyTypePair.first;
            anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(secondaryKeyType);
            secondaryRecFields[i] = keySerde;
            secondaryTypeTraits[i] = typeTraitProvider.getTypeTrait(secondaryKeyType);
        }
        secondaryRecDesc = new RecordDescriptor(secondaryRecFields, secondaryTypeTraits);
        // Comparators and type traits for tokens.
        tokenComparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys];
        tokenTypeTraits = new ITypeTraits[numSecondaryKeys];
        tokenComparatorFactories[0] = InvertedIndexAccessMethod.getTokenBinaryComparatorFactory(secondaryKeyType);
        tokenTypeTraits[0] = InvertedIndexAccessMethod.getTokenTypeTrait(secondaryKeyType);
        // Set tokenizer factory.
        // TODO: We might want to expose the hashing option at the AQL level, 
        // and add the choice to the index metadata.
        tokenizerFactory = InvertedIndexAccessMethod.getBinaryTokenizerFactory(secondaryKeyType.getTypeTag(),
                createIndexStmt.getIndexType(), createIndexStmt.getGramLength());
        // Type traits for inverted-list elements. Inverted lists contain primary keys.
        invListsTypeTraits = new ITypeTraits[numPrimaryKeys];
        for (int i = 0; i < numPrimaryKeys; i++) {
            invListsTypeTraits[i] = primaryRecDesc.getTypeTraits()[i];
        }
        // Get file split providers for the BTree and inverted-list files.
        fileSplitProviders = metadata.getInvertedIndexFileSplitProviders(secondaryFileSplitProvider);
        // For tokenization, sorting and loading.
        // One token + primary keys.
        numTokenKeyPairFields = 1 + numPrimaryKeys;
        ISerializerDeserializer[] tokenKeyPairFields = new ISerializerDeserializer[numTokenKeyPairFields];
        ITypeTraits[] tokenKeyPairTypeTraits = new ITypeTraits[numTokenKeyPairFields];
        tokenKeyPairComparatorFactories = new IBinaryComparatorFactory[numTokenKeyPairFields];
        tokenKeyPairFields[0] = serdeProvider.getSerializerDeserializer(secondaryKeyType);
        tokenKeyPairTypeTraits[0] = tokenTypeTraits[0];
        tokenKeyPairComparatorFactories[0] = InvertedIndexAccessMethod
                .getTokenBinaryComparatorFactory(secondaryKeyType);
        for (int i = 0; i < numPrimaryKeys; i++) {
            tokenKeyPairFields[i + 1] = primaryRecDesc.getFields()[i];
            tokenKeyPairTypeTraits[i + 1] = primaryRecDesc.getTypeTraits()[i];
            tokenKeyPairComparatorFactories[i + 1] = primaryComparatorFactories[i];
        }
        tokenKeyPairRecDesc = new RecordDescriptor(tokenKeyPairFields, tokenKeyPairTypeTraits);
    }

    @Override
    public JobSpecification buildCreationJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = new JobSpecification();
        InvertedIndexCreateOperatorDescriptor invIndexCreateOp = new InvertedIndexCreateOperatorDescriptor(spec,
                AsterixStorageManagerInterface.INSTANCE, fileSplitProviders.first, fileSplitProviders.second,
                AsterixIndexRegistryProvider.INSTANCE, tokenTypeTraits, tokenComparatorFactories, invListsTypeTraits,
                primaryComparatorFactories, tokenizerFactory, new BTreeDataflowHelperFactory(),
                NoOpOperationCallbackProvider.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, invIndexCreateOp,
                secondaryPartitionConstraint);
        spec.addRoot(invIndexCreateOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = new JobSpecification();

        // Create dummy key provider for feeding the primary index scan. 
        AbstractOperatorDescriptor keyProviderOp = createDummyKeyProviderOp(spec);

        // Create primary index scan op.
        BTreeSearchOperatorDescriptor primaryScanOp = createPrimaryIndexScanOp(spec);

        // Assign op.
        AlgebricksMetaOperatorDescriptor asterixAssignOp = createAssignOp(spec, primaryScanOp, numSecondaryKeys);

        // If any of the secondary fields are nullable, then add a select op that filters nulls.
        AlgebricksMetaOperatorDescriptor selectOp = null;
        if (anySecondaryKeyIsNullable) {
            selectOp = createFilterNullsSelectOp(spec, numSecondaryKeys);
        }

        // Create a tokenizer op.
        AbstractOperatorDescriptor tokenizerOp = createTokenizerOp(spec);

        // Sort by token + primary keys.
        ExternalSortOperatorDescriptor sortOp = createSortOp(spec, tokenKeyPairComparatorFactories, tokenKeyPairRecDesc);

        // Create secondary inverted index bulk load op.
        InvertedIndexBulkLoadOperatorDescriptor invIndexBulkLoadOp = createInvertedIndexBulkLoadOp(spec);

        // Connect the operators.
        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryScanOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, asterixAssignOp, 0);
        if (anySecondaryKeyIsNullable) {
            spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, tokenizerOp, 0);
        } else {
            spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, tokenizerOp, 0);
        }
        spec.connect(new OneToOneConnectorDescriptor(spec), tokenizerOp, 0, sortOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, invIndexBulkLoadOp, 0);
        spec.addRoot(invIndexBulkLoadOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    private AbstractOperatorDescriptor createTokenizerOp(JobSpecification spec) throws AlgebricksException {
        int[] fieldsToTokenize = new int[numSecondaryKeys];
        for (int i = 0; i < numSecondaryKeys; i++) {
            fieldsToTokenize[i] = i;
        }
        int[] primaryKeyFields = new int[numPrimaryKeys];
        for (int i = 0; i < numPrimaryKeys; i++) {
            primaryKeyFields[i] = numSecondaryKeys + i;
        }
        BinaryTokenizerOperatorDescriptor tokenizerOp = new BinaryTokenizerOperatorDescriptor(spec,
                tokenKeyPairRecDesc, tokenizerFactory, fieldsToTokenize, primaryKeyFields);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, tokenizerOp,
                primaryPartitionConstraint);
        return tokenizerOp;
    }

    @Override
    protected ExternalSortOperatorDescriptor createSortOp(JobSpecification spec,
            IBinaryComparatorFactory[] secondaryComparatorFactories, RecordDescriptor secondaryRecDesc) {
        // Sort on token and primary keys.
        int[] sortFields = new int[numTokenKeyPairFields];
        for (int i = 0; i < numTokenKeyPairFields; i++) {
            sortFields[i] = i;
        }
        ExternalSortOperatorDescriptor sortOp = new ExternalSortOperatorDescriptor(spec,
                physOptConf.getMaxFramesExternalSort(), sortFields, tokenKeyPairComparatorFactories, secondaryRecDesc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp, primaryPartitionConstraint);
        return sortOp;
    }

    private InvertedIndexBulkLoadOperatorDescriptor createInvertedIndexBulkLoadOp(JobSpecification spec) {
        int[] fieldPermutation = new int[numSecondaryKeys + numPrimaryKeys];
        for (int i = 0; i < numTokenKeyPairFields; i++) {
            fieldPermutation[i] = i;
        }
        InvertedIndexBulkLoadOperatorDescriptor invIndexBulkLoadOp = new InvertedIndexBulkLoadOperatorDescriptor(spec,
                fieldPermutation, AsterixStorageManagerInterface.INSTANCE, fileSplitProviders.first,
                fileSplitProviders.second, AsterixIndexRegistryProvider.INSTANCE, tokenTypeTraits,
                tokenComparatorFactories, invListsTypeTraits, primaryComparatorFactories, tokenizerFactory,
                new BTreeDataflowHelperFactory(), NoOpOperationCallbackProvider.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, invIndexBulkLoadOp,
                secondaryPartitionConstraint);
        return invIndexBulkLoadOp;
    }
}
