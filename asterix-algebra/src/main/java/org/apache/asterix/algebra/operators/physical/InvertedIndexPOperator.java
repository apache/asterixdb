/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.AsterixStorageProperties;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.dataflow.IAsterixApplicationContextInfo;
import org.apache.asterix.common.ioopcallbacks.LSMInvertedIndexIOOperationCallbackFactory;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.AqlSourceId;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.optimizer.rules.am.InvertedIndexAccessMethod;
import org.apache.asterix.optimizer.rules.am.InvertedIndexAccessMethod.SearchModifierType;
import org.apache.asterix.optimizer.rules.am.InvertedIndexJobGenParams;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import org.apache.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifierFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.PartitionedLSMInvertedIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;

/**
 * Contributes the runtime operator for an unnest-map representing an
 * inverted-index search.
 */
public class InvertedIndexPOperator extends IndexSearchPOperator {
    private final boolean isPartitioned;

    public InvertedIndexPOperator(IDataSourceIndex<String, AqlSourceId> idx, boolean requiresBroadcast,
            boolean isPartitioned) {
        super(idx, requiresBroadcast);
        this.isPartitioned = isPartitioned;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        if (isPartitioned) {
            return PhysicalOperatorTag.LENGTH_PARTITIONED_INVERTED_INDEX_SEARCH;
        } else {
            return PhysicalOperatorTag.SINGLE_PARTITION_INVERTED_INDEX_SEARCH;
        }
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
        Dataset dataset;
        try {
            dataset = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(),
                    jobGenParams.getDataverseName(), jobGenParams.getDatasetName());
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
        int[] keyIndexes = getKeyIndexes(jobGenParams.getKeyVarList(), inputSchemas);

        int[] minFilterFieldIndexes = getKeyIndexes(unnestMapOp.getMinFilterVars(), inputSchemas);
        int[] maxFilterFieldIndexes = getKeyIndexes(unnestMapOp.getMaxFilterVars(), inputSchemas);
        // Build runtime.
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> invIndexSearch = buildInvertedIndexRuntime(
                metadataProvider, context, builder.getJobSpec(), unnestMapOp, opSchema, jobGenParams.getRetainInput(),
                jobGenParams.getRetainNull(), jobGenParams.getDatasetName(), dataset, jobGenParams.getIndexName(),
                jobGenParams.getSearchKeyType(), keyIndexes, jobGenParams.getSearchModifierType(),
                jobGenParams.getSimilarityThreshold(), minFilterFieldIndexes, maxFilterFieldIndexes);

        // Contribute operator in hyracks job.
        builder.contributeHyracksOperator(unnestMapOp, invIndexSearch.first);
        builder.contributeAlgebricksPartitionConstraint(invIndexSearch.first, invIndexSearch.second);
        ILogicalOperator srcExchange = unnestMapOp.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMapOp, 0);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildInvertedIndexRuntime(
            AqlMetadataProvider metadataProvider, JobGenContext context, JobSpecification jobSpec,
            UnnestMapOperator unnestMap, IOperatorSchema opSchema, boolean retainInput, boolean retainNull,
            String datasetName, Dataset dataset, String indexName, ATypeTag searchKeyType, int[] keyFields,
            SearchModifierType searchModifierType, IAlgebricksConstantValue similarityThreshold,
            int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes) throws AlgebricksException {

        try {
            IAObject simThresh = ((AsterixConstantValue) similarityThreshold).getObject();
            IAType itemType = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                    dataset.getItemTypeDataverseName(), dataset.getItemTypeName()).getDatatype();
            int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(),
                    dataset.getDataverseName(), dataset.getDatasetName(), indexName);
            if (secondaryIndex == null) {
                throw new AlgebricksException(
                        "Code generation error: no index " + indexName + " for dataset " + datasetName);
            }
            List<List<String>> secondaryKeyFieldEntries = secondaryIndex.getKeyFieldNames();
            List<IAType> secondaryKeyTypeEntries = secondaryIndex.getKeyFieldTypes();
            int numSecondaryKeys = secondaryKeyFieldEntries.size();
            if (numSecondaryKeys != 1) {
                throw new AlgebricksException("Cannot use " + numSecondaryKeys
                        + " fields as a key for an inverted index. There can be only one field as a key for the inverted index index.");
            }
            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be indexed.");
            }
            ARecordType recordType = (ARecordType) itemType;
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryKeyTypeEntries.get(0),
                    secondaryKeyFieldEntries.get(0), recordType);
            IAType secondaryKeyType = keyPairType.first;
            if (secondaryKeyType == null) {
                throw new AlgebricksException(
                        "Could not find field " + secondaryKeyFieldEntries.get(0) + " in the schema.");
            }

            // TODO: For now we assume the type of the generated tokens is the
            // same as the indexed field.
            // We need a better way of expressing this because tokens may be
            // hashed, or an inverted-index may index a list type, etc.
            int numTokenKeys = (!isPartitioned) ? numSecondaryKeys : numSecondaryKeys + 1;
            ITypeTraits[] tokenTypeTraits = new ITypeTraits[numTokenKeys];
            IBinaryComparatorFactory[] tokenComparatorFactories = new IBinaryComparatorFactory[numTokenKeys];
            for (int i = 0; i < numSecondaryKeys; i++) {
                tokenComparatorFactories[i] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
                tokenTypeTraits[i] = NonTaggedFormatUtil.getTokenTypeTrait(secondaryKeyType);
            }
            if (isPartitioned) {
                // The partitioning field is hardcoded to be a short *without* an Asterix type tag.
                tokenComparatorFactories[numSecondaryKeys] = PointableBinaryComparatorFactory
                        .of(ShortPointable.FACTORY);
                tokenTypeTraits[numSecondaryKeys] = ShortPointable.TYPE_TRAITS;
            }

            IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(unnestMap);
            List<LogicalVariable> outputVars = unnestMap.getVariables();
            if (retainInput) {
                outputVars = new ArrayList<LogicalVariable>();
                VariableUtilities.getLiveVariables(unnestMap, outputVars);
            }
            RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);

            int start = outputRecDesc.getFieldCount() - numPrimaryKeys;
            IBinaryComparatorFactory[] invListsComparatorFactories = JobGenHelper
                    .variablesToAscBinaryComparatorFactories(outputVars, start, numPrimaryKeys, typeEnv, context);
            ITypeTraits[] invListsTypeTraits = JobGenHelper.variablesToTypeTraits(outputVars, start, numPrimaryKeys,
                    typeEnv, context);

            ITypeTraits[] filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, recordType);
            IBinaryComparatorFactory[] filterCmpFactories = DatasetUtils.computeFilterBinaryComparatorFactories(dataset,
                    recordType, context.getBinaryComparatorFactoryProvider());

            int[] filterFields = null;
            int[] invertedIndexFields = null;
            int[] filterFieldsForNonBulkLoadOps = null;
            int[] invertedIndexFieldsForNonBulkLoadOps = null;
            if (filterTypeTraits != null) {
                filterFields = new int[1];
                filterFields[0] = numTokenKeys + numPrimaryKeys;
                invertedIndexFields = new int[numTokenKeys + numPrimaryKeys];
                for (int k = 0; k < invertedIndexFields.length; k++) {
                    invertedIndexFields[k] = k;
                }

                filterFieldsForNonBulkLoadOps = new int[1];
                filterFieldsForNonBulkLoadOps[0] = numPrimaryKeys + numSecondaryKeys;
                invertedIndexFieldsForNonBulkLoadOps = new int[numPrimaryKeys + numSecondaryKeys];
                for (int k = 0; k < invertedIndexFieldsForNonBulkLoadOps.length; k++) {
                    invertedIndexFieldsForNonBulkLoadOps[k] = k;
                }
            }

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint = metadataProvider
                    .splitProviderAndPartitionConstraintsForDataset(dataset.getDataverseName(), datasetName, indexName,
                            dataset.getDatasetDetails().isTemp());
            // TODO: Here we assume there is only one search key field.
            int queryField = keyFields[0];
            // Get tokenizer and search modifier factories.
            IInvertedIndexSearchModifierFactory searchModifierFactory = InvertedIndexAccessMethod
                    .getSearchModifierFactory(searchModifierType, simThresh, secondaryIndex);
            IBinaryTokenizerFactory queryTokenizerFactory = InvertedIndexAccessMethod
                    .getBinaryTokenizerFactory(searchModifierType, searchKeyType, secondaryIndex);
            IIndexDataflowHelperFactory dataflowHelperFactory;

            AsterixStorageProperties storageProperties = AsterixAppContextInfo.getInstance().getStorageProperties();
            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils
                    .getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
            boolean temp = dataset.getDatasetDetails().isTemp();
            if (!isPartitioned) {
                dataflowHelperFactory = new LSMInvertedIndexDataflowHelperFactory(
                        new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                        compactionInfo.second, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                        LSMInvertedIndexIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate(), invertedIndexFields, filterTypeTraits,
                        filterCmpFactories, filterFields, filterFieldsForNonBulkLoadOps,
                        invertedIndexFieldsForNonBulkLoadOps, !temp);
            } else {
                dataflowHelperFactory = new PartitionedLSMInvertedIndexDataflowHelperFactory(
                        new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                        compactionInfo.second, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                        LSMInvertedIndexIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate(), invertedIndexFields, filterTypeTraits,
                        filterCmpFactories, filterFields, filterFieldsForNonBulkLoadOps,
                        invertedIndexFieldsForNonBulkLoadOps, !temp);
            }
            LSMInvertedIndexSearchOperatorDescriptor invIndexSearchOp = new LSMInvertedIndexSearchOperatorDescriptor(
                    jobSpec, queryField, appContext.getStorageManagerInterface(), secondarySplitsAndConstraint.first,
                    appContext.getIndexLifecycleManagerProvider(), tokenTypeTraits, tokenComparatorFactories,
                    invListsTypeTraits, invListsComparatorFactories, dataflowHelperFactory, queryTokenizerFactory,
                    searchModifierFactory, outputRecDesc, retainInput, retainNull, context.getNullWriterFactory(),
                    NoOpOperationCallbackFactory.INSTANCE, minFilterFieldIndexes, maxFilterFieldIndexes);

            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(invIndexSearchOp,
                    secondarySplitsAndConstraint.second);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }
}
