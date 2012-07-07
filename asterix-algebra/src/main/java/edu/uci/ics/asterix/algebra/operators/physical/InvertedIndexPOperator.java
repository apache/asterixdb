package edu.uci.ics.asterix.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.dataflow.IAsterixApplicationContextInfo;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.optimizer.rules.am.InvertedIndexAccessMethod;
import edu.uci.ics.asterix.optimizer.rules.am.InvertedIndexAccessMethod.SearchModifierType;
import edu.uci.ics.asterix.optimizer.rules.am.InvertedIndexJobGenParams;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.dataflow.InvertedIndexSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;

/**
 * Contributes the runtime operator for an unnest-map representing an inverted-index search.
 */
public class InvertedIndexPOperator extends IndexSearchPOperator {
    public InvertedIndexPOperator(IDataSourceIndex<String, AqlSourceId> idx, boolean requiresBroadcast) {
        super(idx, requiresBroadcast);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.INVERTED_INDEX_SEARCH;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        UnnestMapOperator unnestMapOp = (UnnestMapOperator) op;
        ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException();
        }
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
        if (unnestFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.INDEX_SEARCH) {
            return;
        }
        InvertedIndexJobGenParams jobGenParams = new InvertedIndexJobGenParams();
        jobGenParams.readFromFuncArgs(unnestFuncExpr.getArguments());

        AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = metadataProvider.getMetadataDeclarations();
        Dataset dataset = metadata.findDataset(jobGenParams.getDatasetName());
        int[] keyIndexes = getKeyIndexes(jobGenParams.getKeyVarList(), inputSchemas);

        // Build runtime.
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> invIndexSearch = buildInvertedIndexRuntime(metadata,
                context, builder.getJobSpec(), unnestMapOp, opSchema, jobGenParams.getRetainInput(),
                jobGenParams.getDatasetName(), dataset, jobGenParams.getIndexName(), jobGenParams.getSearchKeyType(),
                keyIndexes, jobGenParams.getSearchModifierType(), jobGenParams.getSimilarityThreshold());
        // Contribute operator in hyracks job.
        builder.contributeHyracksOperator(unnestMapOp, invIndexSearch.first);
        builder.contributeAlgebricksPartitionConstraint(invIndexSearch.first, invIndexSearch.second);
        ILogicalOperator srcExchange = unnestMapOp.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMapOp, 0);
    }

    public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildInvertedIndexRuntime(
            AqlCompiledMetadataDeclarations metadata, JobGenContext context, JobSpecification jobSpec,
            UnnestMapOperator unnestMap, IOperatorSchema opSchema, boolean retainInput, String datasetName,
            Dataset dataset, String indexName, ATypeTag searchKeyType, int[] keyFields,
            SearchModifierType searchModifierType, IAlgebricksConstantValue similarityThreshold)
            throws AlgebricksException {
        IAObject simThresh = ((AsterixConstantValue) similarityThreshold).getObject();
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType = metadata.findType(itemTypeName);
        int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        Index secondaryIndex = metadata.getIndex(dataset.getDataverseName(), dataset.getDatasetName(), indexName);
        if (secondaryIndex == null) {
            throw new AlgebricksException("Code generation error: no index " + indexName + " for dataset "
                    + datasetName);
        }
        List<String> secondaryKeyFields = secondaryIndex.getKeyFieldNames();
        int numSecondaryKeys = secondaryKeyFields.size();
        if (numSecondaryKeys != 1) {
            throw new AlgebricksException(
                    "Cannot use "
                            + numSecondaryKeys
                            + " fields as a key for an inverted index. There can be only one field as a key for the inverted index index.");
        }
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Only record types can be indexed.");
        }
        ARecordType recordType = (ARecordType) itemType;
        Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(secondaryKeyFields.get(0), recordType);
        IAType secondaryKeyType = keyPairType.first;
        if (secondaryKeyType == null) {
            throw new AlgebricksException("Could not find field " + secondaryKeyFields.get(0) + " in the schema.");
        }

        // TODO: For now we assume the type of the generated tokens is the same as the indexed field.
        // We need a better way of expressing this because tokens may be hashed, or an inverted-index may index a list type, etc.
        ITypeTraits[] tokenTypeTraits = new ITypeTraits[numSecondaryKeys];
        IBinaryComparatorFactory[] tokenComparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys];
        for (int i = 0; i < numSecondaryKeys; i++) {
            tokenComparatorFactories[i] = InvertedIndexAccessMethod.getTokenBinaryComparatorFactory(secondaryKeyType);
            tokenTypeTraits[i] = InvertedIndexAccessMethod.getTokenTypeTrait(secondaryKeyType);
        }

        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(unnestMap);
        List<LogicalVariable> outputVars = unnestMap.getVariables();
        if (retainInput) {
            outputVars = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(unnestMap, outputVars);
        }
        RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);

        int start = outputRecDesc.getFieldCount() - numPrimaryKeys;
        IBinaryComparatorFactory[] invListsComparatorFactories = JobGenHelper.variablesToAscBinaryComparatorFactories(
                outputVars, start, numPrimaryKeys, typeEnv, context);
        ITypeTraits[] invListsTypeTraits = JobGenHelper.variablesToTypeTraits(outputVars, start, numPrimaryKeys,
                typeEnv, context);

        IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, indexName);
        Pair<IFileSplitProvider, IFileSplitProvider> fileSplitProviders = metadata
                .getInvertedIndexFileSplitProviders(secondarySplitsAndConstraint.first);

        // TODO: Here we assume there is only one search key field.
        int queryField = keyFields[0];
        // Get tokenizer and search modifier factories.
        IInvertedIndexSearchModifierFactory searchModifierFactory = InvertedIndexAccessMethod.getSearchModifierFactory(
                searchModifierType, simThresh, secondaryIndex);
        IBinaryTokenizerFactory queryTokenizerFactory = InvertedIndexAccessMethod.getBinaryTokenizerFactory(
                searchModifierType, searchKeyType, secondaryIndex);
        InvertedIndexSearchOperatorDescriptor invIndexSearchOp = new InvertedIndexSearchOperatorDescriptor(jobSpec,
                queryField, appContext.getStorageManagerInterface(), fileSplitProviders.first,
                fileSplitProviders.second, appContext.getIndexRegistryProvider(), tokenTypeTraits,
                tokenComparatorFactories, invListsTypeTraits, invListsComparatorFactories,
                new BTreeDataflowHelperFactory(), queryTokenizerFactory, searchModifierFactory, outputRecDesc,
                retainInput, NoOpOperationCallbackProvider.INSTANCE);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(invIndexSearchOp,
                secondarySplitsAndConstraint.second);
    }
}
