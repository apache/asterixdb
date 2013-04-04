package edu.uci.ics.asterix.file;

import java.util.List;

import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.context.AsterixRuntimeComponentsProvider;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.formats.FormatUtils;
import edu.uci.ics.asterix.transaction.management.resource.ILocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.LSMInvertedIndexLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledCreateIndexStatement;
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
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexCreateOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.PartitionedLSMInvertedIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;

public class SecondaryInvertedIndexCreator extends SecondaryIndexCreator {

    private IAType secondaryKeyType;
    private ITypeTraits[] invListsTypeTraits;
    private IBinaryComparatorFactory[] tokenComparatorFactories;
    private ITypeTraits[] tokenTypeTraits;
    private IBinaryTokenizerFactory tokenizerFactory;
    // For tokenization, sorting and loading. Represents <token, primary keys>.
    private int numTokenKeyPairFields;
    private IBinaryComparatorFactory[] tokenKeyPairComparatorFactories;
    private RecordDescriptor tokenKeyPairRecDesc;
    private boolean isPartitioned;

    protected SecondaryInvertedIndexCreator(PhysicalOptimizationConfig physOptConf) {
        super(physOptConf);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected void setSecondaryRecDescAndComparators(CompiledCreateIndexStatement createIndexStmt,
            AqlMetadataProvider metadata) throws AlgebricksException, AsterixException {
        // Sanity checks.
        if (numPrimaryKeys > 1) {
            throw new AsterixException("Cannot create inverted index on dataset with composite primary key.");
        }
        if (numSecondaryKeys > 1) {
            throw new AsterixException("Cannot create composite inverted index on multiple fields.");
        }
        if (createIndexStmt.getIndexType() == IndexType.FUZZY_WORD_INVIX
                || createIndexStmt.getIndexType() == IndexType.FUZZY_NGRAM_INVIX) {
            isPartitioned = true;
        } else {
            isPartitioned = false;
        }
        // Prepare record descriptor used in the assign op, and the optional
        // select op.
        List<String> secondaryKeyFields = createIndexStmt.getKeyFields();
        secondaryFieldAccessEvalFactories = new ICopyEvaluatorFactory[numSecondaryKeys];
        ISerializerDeserializer[] secondaryRecFields = new ISerializerDeserializer[numPrimaryKeys + numSecondaryKeys];
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = FormatUtils.getDefaultFormat().getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = FormatUtils.getDefaultFormat().getTypeTraitProvider();
        for (int i = 0; i < numSecondaryKeys; i++) {
            secondaryFieldAccessEvalFactories[i] = FormatUtils.getDefaultFormat().getFieldAccessEvaluatorFactory(
                    itemType, secondaryKeyFields.get(i), numPrimaryKeys);
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableKeyFieldType(secondaryKeyFields.get(i), itemType);
            secondaryKeyType = keyTypePair.first;
            anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(secondaryKeyType);
            secondaryRecFields[i] = keySerde;
            secondaryTypeTraits[i] = typeTraitProvider.getTypeTrait(secondaryKeyType);
        }
        secondaryRecDesc = new RecordDescriptor(secondaryRecFields, secondaryTypeTraits);
        // Comparators and type traits for tokens.
        int numTokenFields = (!isPartitioned) ? numSecondaryKeys : numSecondaryKeys + 1;
        tokenComparatorFactories = new IBinaryComparatorFactory[numTokenFields];
        tokenTypeTraits = new ITypeTraits[numTokenFields];
        tokenComparatorFactories[0] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
        tokenTypeTraits[0] = NonTaggedFormatUtil.getTokenTypeTrait(secondaryKeyType);
        if (isPartitioned) {
            // The partitioning field is hardcoded to be a short *without* an Asterix type tag.
            tokenComparatorFactories[1] = PointableBinaryComparatorFactory.of(ShortPointable.FACTORY);
            tokenTypeTraits[1] = ShortPointable.TYPE_TRAITS;
        }
        // Set tokenizer factory.
        // TODO: We might want to expose the hashing option at the AQL level,
        // and add the choice to the index metadata.
        tokenizerFactory = NonTaggedFormatUtil.getBinaryTokenizerFactory(secondaryKeyType.getTypeTag(),
                createIndexStmt.getIndexType(), createIndexStmt.getGramLength());
        // Type traits for inverted-list elements. Inverted lists contain
        // primary keys.
        invListsTypeTraits = new ITypeTraits[numPrimaryKeys];
        for (int i = 0; i < numPrimaryKeys; i++) {
            invListsTypeTraits[i] = primaryRecDesc.getTypeTraits()[i];
        }
        // For tokenization, sorting and loading.
        // One token (+ optional partitioning field) + primary keys.
        numTokenKeyPairFields = (!isPartitioned) ? 1 + numPrimaryKeys : 2 + numPrimaryKeys;
        ISerializerDeserializer[] tokenKeyPairFields = new ISerializerDeserializer[numTokenKeyPairFields];
        ITypeTraits[] tokenKeyPairTypeTraits = new ITypeTraits[numTokenKeyPairFields];
        tokenKeyPairComparatorFactories = new IBinaryComparatorFactory[numTokenKeyPairFields];
        tokenKeyPairFields[0] = serdeProvider.getSerializerDeserializer(secondaryKeyType);
        tokenKeyPairTypeTraits[0] = tokenTypeTraits[0];
        tokenKeyPairComparatorFactories[0] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
        int pkOff = 1;
        if (isPartitioned) {
            tokenKeyPairFields[1] = ShortSerializerDeserializer.INSTANCE;
            tokenKeyPairTypeTraits[1] = tokenTypeTraits[1];
            tokenKeyPairComparatorFactories[1] = PointableBinaryComparatorFactory.of(ShortPointable.FACTORY);
            pkOff = 2;
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            tokenKeyPairFields[i + pkOff] = primaryRecDesc.getFields()[i];
            tokenKeyPairTypeTraits[i + pkOff] = primaryRecDesc.getTypeTraits()[i];
            tokenKeyPairComparatorFactories[i + pkOff] = primaryComparatorFactories[i];
        }
        tokenKeyPairRecDesc = new RecordDescriptor(tokenKeyPairFields, tokenKeyPairTypeTraits);
    }

    @Override
    public JobSpecification buildCreationJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = new JobSpecification();

        //prepare a LocalResourceMetadata which will be stored in NC's local resource repository
        ILocalResourceMetadata localResourceMetadata = new LSMInvertedIndexLocalResourceMetadata(invListsTypeTraits,
                primaryComparatorFactories, tokenTypeTraits, tokenComparatorFactories, tokenizerFactory,
                GlobalConfig.DEFAULT_INDEX_MEM_PAGE_SIZE, GlobalConfig.DEFAULT_INDEX_MEM_NUM_PAGES, isPartitioned);
        ILocalResourceFactoryProvider localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(
                localResourceMetadata, LocalResource.LSMInvertedIndexResource);

        IIndexDataflowHelperFactory dataflowHelperFactory = createDataflowHelperFactory();
        LSMInvertedIndexCreateOperatorDescriptor invIndexCreateOp = new LSMInvertedIndexCreateOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER, secondaryFileSplitProvider,
                AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER, tokenTypeTraits, tokenComparatorFactories,
                invListsTypeTraits, primaryComparatorFactories, tokenizerFactory, dataflowHelperFactory,
                localResourceFactoryProvider, NoOpOperationCallbackFactory.INSTANCE);
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

        // If any of the secondary fields are nullable, then add a select op
        // that filters nulls.
        AlgebricksMetaOperatorDescriptor selectOp = null;
        if (anySecondaryKeyIsNullable) {
            selectOp = createFilterNullsSelectOp(spec, numSecondaryKeys);
        }

        // Create a tokenizer op.
        AbstractOperatorDescriptor tokenizerOp = createTokenizerOp(spec);

        // Sort by token + primary keys.
        ExternalSortOperatorDescriptor sortOp = createSortOp(spec, tokenKeyPairComparatorFactories, tokenKeyPairRecDesc);

        // Create secondary inverted index bulk load op.
        LSMInvertedIndexBulkLoadOperatorDescriptor invIndexBulkLoadOp = createInvertedIndexBulkLoadOp(spec);

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
        int docField = 0;
        int[] primaryKeyFields = new int[numPrimaryKeys];
        for (int i = 0; i < numPrimaryKeys; i++) {
            primaryKeyFields[i] = numSecondaryKeys + i;
        }
        BinaryTokenizerOperatorDescriptor tokenizerOp = new BinaryTokenizerOperatorDescriptor(spec,
                tokenKeyPairRecDesc, tokenizerFactory, docField, primaryKeyFields, isPartitioned);
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

    private LSMInvertedIndexBulkLoadOperatorDescriptor createInvertedIndexBulkLoadOp(JobSpecification spec) {
        int[] fieldPermutation = new int[numTokenKeyPairFields];
        for (int i = 0; i < numTokenKeyPairFields; i++) {
            fieldPermutation[i] = i;
        }
        IIndexDataflowHelperFactory dataflowHelperFactory = createDataflowHelperFactory();
        LSMInvertedIndexBulkLoadOperatorDescriptor invIndexBulkLoadOp = new LSMInvertedIndexBulkLoadOperatorDescriptor(
                spec, fieldPermutation, false, numElementsHint, AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER,
                secondaryFileSplitProvider, AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER, tokenTypeTraits,
                tokenComparatorFactories, invListsTypeTraits, primaryComparatorFactories, tokenizerFactory,
                dataflowHelperFactory, NoOpOperationCallbackFactory.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, invIndexBulkLoadOp,
                secondaryPartitionConstraint);
        return invIndexBulkLoadOp;
    }

    private IIndexDataflowHelperFactory createDataflowHelperFactory() {
        if (!isPartitioned) {
            return new LSMInvertedIndexDataflowHelperFactory(
                    AsterixRuntimeComponentsProvider.LSMINVERTEDINDEX_PROVIDER,
                    AsterixRuntimeComponentsProvider.LSMINVERTEDINDEX_PROVIDER,
                    AsterixRuntimeComponentsProvider.LSMINVERTEDINDEX_PROVIDER,
                    AsterixRuntimeComponentsProvider.LSMINVERTEDINDEX_PROVIDER,
                    GlobalConfig.DEFAULT_INDEX_MEM_PAGE_SIZE, GlobalConfig.DEFAULT_INDEX_MEM_NUM_PAGES);
        } else {
            return new PartitionedLSMInvertedIndexDataflowHelperFactory(
                    AsterixRuntimeComponentsProvider.LSMINVERTEDINDEX_PROVIDER,
                    AsterixRuntimeComponentsProvider.LSMINVERTEDINDEX_PROVIDER,
                    AsterixRuntimeComponentsProvider.LSMINVERTEDINDEX_PROVIDER,
                    AsterixRuntimeComponentsProvider.LSMINVERTEDINDEX_PROVIDER,
                    GlobalConfig.DEFAULT_INDEX_MEM_PAGE_SIZE, GlobalConfig.DEFAULT_INDEX_MEM_NUM_PAGES);
        }
    }
}
