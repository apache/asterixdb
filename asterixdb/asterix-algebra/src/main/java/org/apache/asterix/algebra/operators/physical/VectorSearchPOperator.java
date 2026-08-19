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

import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.DataSourceIndex;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.PushFilterIntoVectorSearchRule;
import org.apache.asterix.optimizer.rules.am.VectorJobGenParams;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;

/**
 * Contributes the runtime operator for an unnest-map representing a vector index search.
 */
public class VectorSearchPOperator extends IndexSearchPOperator {

    public VectorSearchPOperator(IDataSourceIndex<String, DataSourceId> idx, INodeDomain domain,
            boolean requiresBroadcast) {
        super(idx, domain, requiresBroadcast);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.VECTOR_SEARCH;
    }

    /**
     * The vector index unnest-map emits its scan variables as
     * {@code [pk_0..pk_{N-1}, (distance?), pushedIncludeFilterVars...]} -- the primary keys come
     * FIRST, unlike a BTree/RTree secondary search where they come last.
     * {@link IndexSearchPOperator#computeDeliveredProperties} assumes the primary keys are the LAST
     * {@code numPrimaryKeys} scan variables, so an appended index-only distance field or a pushed
     * INCLUDE-filter variable would be mislabeled as a primary key and yield a wrong delivered
     * partitioning property on a partitioned cluster. Take the FIRST {@code numPrimaryKeys} variables
     * instead. When nothing extra is appended this is identical to the inherited behavior.
     */
    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        IDataSource<?> ds = idx.getDataSource();
        List<LogicalVariable> scanVariables = new ArrayList<>();
        if (idx instanceof DataSourceIndex) {
            Index index = ((DataSourceIndex) idx).getIndex();
            if (index.isSecondaryIndex() && ds instanceof DatasetDataSource) {
                Dataset dataset = ((DatasetDataSource) ds).getDataset();
                int numOfPrimaryKeys = dataset.getPrimaryKeys().size();
                if (op.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP
                        || op.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP) {
                    List<LogicalVariable> opVars = ((AbstractUnnestMapOperator) op).getScanVariables();
                    // Vector search: primary keys are at the FRONT of the scan-variable list.
                    scanVariables.addAll(opVars.subList(0, numOfPrimaryKeys));
                    scanVariables.add(new LogicalVariable(-1));
                    if (dataset.hasMetaPart()) {
                        scanVariables.add(new LogicalVariable(-1));
                    }
                }
            }
        }
        if (scanVariables.isEmpty()) {
            AbstractScanOperator as = (AbstractScanOperator) op;
            scanVariables.addAll(as.getScanVariables());
        }
        IDataSourcePropertiesProvider dspp = ds.getPropertiesProvider();
        deliveredProperties = dspp.computeDeliveredProperties(scanVariables, context);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        AbstractUnnestMapOperator unnestMap = (AbstractUnnestMapOperator) op;
        ILogicalExpression unnestExpr = unnestMap.getExpressionRef().getValue();
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new AlgebricksException("Vector search unnest expression must be a function call");
        }
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
        FunctionIdentifier funcIdent = unnestFuncExpr.getFunctionIdentifier();
        if (!funcIdent.equals(BuiltinFunctions.INDEX_SEARCH)) {
            return;
        }

        VectorJobGenParams jobGenParams = new VectorJobGenParams();
        jobGenParams.readFromFuncArgs(unnestFuncExpr.getArguments());

        // queryVarList layout: [query_vector, k, metric, min_probe_fraction, k_multiplier]
        int[] queryIndexes = getKeyIndexes(jobGenParams.getQueryVarList(), inputSchemas);

        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
        Dataset dataset = mp.findDataset(jobGenParams.getDatabaseName(), jobGenParams.getDataverseName(),
                jobGenParams.getDatasetName());
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(unnestMap);

        List<LogicalVariable> outputVars = unnestMap.getVariables();
        if (jobGenParams.getRetainInput()) {
            outputVars = new ArrayList<>();
            VariableUtilities.getLiveVariables(unnestMap, outputVars);
        }

        // Determine quantization from vector index metadata
        Index vectorIndex = mp.getIndex(jobGenParams.getDatabaseName(), jobGenParams.getDataverseName(),
                jobGenParams.getDatasetName(), jobGenParams.getIndexName());
        Index.VectorIndexDetails vectorDetails = (Index.VectorIndexDetails) vectorIndex.getIndexDetails();
        boolean isQuantized = vectorDetails.getVectorParameters().isQuantized();
        int numSecondaryKeys = isQuantized ? 4 : 2;

        // Create tuple filter factory if selectCondition is present (for INCLUDE field filtering)
        // The opSchema only has [pk] because INCLUDE fields are only used for filtering.
        // Filter variables are mapped directly to physical field indexes via annotation.
        ITupleFilterFactory tupleFilterFactory = null;
        if (unnestMap instanceof UnnestMapOperator) {
            UnnestMapOperator unnestMapOp = (UnnestMapOperator) unnestMap;
            if (unnestMapOp.getSelectCondition() != null) {
                // Get filter variable to physical field index mapping from annotation
                @SuppressWarnings("unchecked")
                Map<LogicalVariable, Integer> filterVarToFieldIndex = (Map<LogicalVariable, Integer>) unnestMapOp
                        .getAnnotations().get(PushFilterIntoVectorSearchRule.VECTOR_FILTER_VAR_MAPPING);

                // Get filter variable types from annotation
                @SuppressWarnings("unchecked")
                Map<LogicalVariable, IAType> filterVarTypes = (Map<LogicalVariable, IAType>) unnestMapOp
                        .getAnnotations().get(PushFilterIntoVectorSearchRule.VECTOR_FILTER_VAR_TYPES);

                // Create filter schema with direct mapping for filter-only variables
                // numSecondaryKeys: offset from physical tuple start to PK field
                IOperatorSchema filterSchema =
                        new VectorIndexFilterSchema(opSchema, filterVarToFieldIndex, numSecondaryKeys);

                // Create type environment with filter variable types
                // Pass context so function expressions can use this wrapper for recursive type lookups
                IVariableTypeEnvironment filterTypeEnv =
                        new VectorIndexFilterTypeEnvironment(typeEnv, filterVarTypes, context);

                tupleFilterFactory = mp.createTupleFilterFactory(new IOperatorSchema[] { filterSchema }, filterTypeEnv,
                        unnestMapOp.getSelectCondition().getValue(), context);
            }
        }

        // jobGenParams.isIndexOnly() (set by IntroduceTopKAccessMethodRule when the projection above
        // LIMIT references only PK columns) tells the runtime to emit [pk..., D(q,x)] per candidate so
        // the downstream sort can rank without the primary BTree lookup.
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> vectorSearch = mp.getVectorSearchRuntime(
                builder.getJobSpec(), outputVars, opSchema, typeEnv, context, jobGenParams.getRetainInput(), dataset,
                jobGenParams.getIndexName(), queryIndexes, tupleFilterFactory, jobGenParams.isIndexOnly());

        IOperatorDescriptor opDesc = vectorSearch.first;
        opDesc.setSourceLocation(unnestMap.getSourceLocation());

        builder.contributeHyracksOperator(unnestMap, opDesc);
        builder.contributeAlgebricksPartitionConstraint(opDesc, vectorSearch.second);

        ILogicalOperator srcExchange = unnestMap.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMap, 0);
    }
}
