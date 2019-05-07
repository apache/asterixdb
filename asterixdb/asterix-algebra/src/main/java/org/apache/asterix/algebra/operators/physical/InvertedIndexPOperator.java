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

import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.optimizer.rules.am.InvertedIndexAccessMethod;
import org.apache.asterix.optimizer.rules.am.InvertedIndexAccessMethod.SearchModifierType;
import org.apache.asterix.optimizer.rules.am.InvertedIndexJobGenParams;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifierFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;

/**
 * Contributes the runtime operator for an unnest-map representing an
 * inverted-index search.
 */
public class InvertedIndexPOperator extends IndexSearchPOperator {

    // variable memory, min 5 frames
    // 1 for query + 2 for intermediate results + 1 for final result + 1 for reading an inverted list
    public static final int MIN_FRAME_LIMIT_FOR_TEXT_SEARCH = OptimizationConfUtil.MIN_FRAME_LIMIT_FOR_TEXT_SEARCH;

    private final boolean isPartitioned;

    public InvertedIndexPOperator(IDataSourceIndex<String, DataSourceId> idx, INodeDomain domain,
            boolean requiresBroadcast, boolean isPartitioned) {
        super(idx, domain, requiresBroadcast);
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
    public void createLocalMemoryRequirements(ILogicalOperator op) {
        localMemoryRequirements = LocalMemoryRequirements.variableMemoryBudget(MIN_FRAME_LIMIT_FOR_TEXT_SEARCH);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        AbstractUnnestMapOperator unnestMapOp = (AbstractUnnestMapOperator) op;
        ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException();
        }
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
        if (unnestFuncExpr.getFunctionIdentifier() != BuiltinFunctions.INDEX_SEARCH) {
            return;
        }
        InvertedIndexJobGenParams jobGenParams = new InvertedIndexJobGenParams();
        jobGenParams.readFromFuncArgs(unnestFuncExpr.getArguments());

        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        Dataset dataset = metadataProvider.findDataset(jobGenParams.getDataverseName(), jobGenParams.getDatasetName());
        int[] keyIndexes = getKeyIndexes(jobGenParams.getKeyVarList(), inputSchemas);

        int[] minFilterFieldIndexes = getKeyIndexes(unnestMapOp.getMinFilterVars(), inputSchemas);
        int[] maxFilterFieldIndexes = getKeyIndexes(unnestMapOp.getMaxFilterVars(), inputSchemas);
        boolean retainNull = false;
        if (op.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP) {
            // By nature, LEFT_OUTER_UNNEST_MAP should generate null values for non-matching
            // tuples.
            retainNull = true;
        }
        // In-memory budget (frame limit) for inverted-index search operations
        int frameLimit = localMemoryRequirements.getMemoryBudgetInFrames();

        // Build runtime.
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> invIndexSearch =
                buildInvertedIndexRuntime(metadataProvider, context, builder.getJobSpec(), unnestMapOp, opSchema,
                        jobGenParams.getRetainInput(), retainNull, jobGenParams.getDatasetName(), dataset,
                        jobGenParams.getIndexName(), jobGenParams.getSearchKeyType(), keyIndexes,
                        jobGenParams.getSearchModifierType(), jobGenParams.getSimilarityThreshold(),
                        minFilterFieldIndexes, maxFilterFieldIndexes, jobGenParams.getIsFullTextSearch(), frameLimit);
        IOperatorDescriptor opDesc = invIndexSearch.first;
        opDesc.setSourceLocation(unnestMapOp.getSourceLocation());

        // Contribute operator in hyracks job.
        builder.contributeHyracksOperator(unnestMapOp, opDesc);
        builder.contributeAlgebricksPartitionConstraint(opDesc, invIndexSearch.second);
        ILogicalOperator srcExchange = unnestMapOp.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMapOp, 0);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildInvertedIndexRuntime(
            MetadataProvider metadataProvider, JobGenContext context, JobSpecification jobSpec,
            AbstractUnnestMapOperator unnestMap, IOperatorSchema opSchema, boolean retainInput, boolean retainMissing,
            String datasetName, Dataset dataset, String indexName, ATypeTag searchKeyType, int[] keyFields,
            SearchModifierType searchModifierType, IAlgebricksConstantValue similarityThreshold,
            int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes, boolean isFullTextSearchQuery, int frameLimit)
            throws AlgebricksException {
        boolean propagateIndexFilter = unnestMap.propagateIndexFilter();
        IAObject simThresh = ((AsterixConstantValue) similarityThreshold).getObject();
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        Index secondaryIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(),
                dataset.getDataverseName(), dataset.getDatasetName(), indexName);
        if (secondaryIndex == null) {
            throw new AlgebricksException(
                    "Code generation error: no index " + indexName + " for dataset " + datasetName);
        }
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(unnestMap);
        RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset, indexName);
        // TODO: Here we assume there is only one search key field.
        int queryField = keyFields[0];
        // Get tokenizer and search modifier factories.
        IInvertedIndexSearchModifierFactory searchModifierFactory =
                InvertedIndexAccessMethod.getSearchModifierFactory(searchModifierType, simThresh, secondaryIndex);
        IBinaryTokenizerFactory queryTokenizerFactory =
                InvertedIndexAccessMethod.getBinaryTokenizerFactory(searchModifierType, searchKeyType, secondaryIndex);
        IIndexDataflowHelperFactory dataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), secondarySplitsAndConstraint.first);
        LSMInvertedIndexSearchOperatorDescriptor invIndexSearchOp = new LSMInvertedIndexSearchOperatorDescriptor(
                jobSpec, outputRecDesc, queryField, dataflowHelperFactory, queryTokenizerFactory, searchModifierFactory,
                retainInput, retainMissing, context.getMissingWriterFactory(),
                dataset.getSearchCallbackFactory(metadataProvider.getStorageComponentProvider(), secondaryIndex,
                        IndexOperation.SEARCH, null),
                minFilterFieldIndexes, maxFilterFieldIndexes, isFullTextSearchQuery, numPrimaryKeys,
                propagateIndexFilter, frameLimit);
        return new Pair<>(invIndexSearchOp, secondarySplitsAndConstraint.second);
    }
}
