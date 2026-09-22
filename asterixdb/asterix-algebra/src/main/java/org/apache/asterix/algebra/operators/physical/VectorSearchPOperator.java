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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.DataSourceIndex;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.VectorIncludeFilterPushdown;
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
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;

/**
 * Contributes the runtime operator for an unnest-map representing a vector index search.
 */
public class VectorSearchPOperator extends IndexSearchPOperator {

    private static final int[] NO_INCLUDE_FIELDS = new int[0];

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
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "the vector search unnest expression is not a function call");
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
        int numSecondaryKeys = VTreeDataTupleAccessor.getNumSecondaryFields(isQuantized);

        // The INCLUDE columns are declared on the search whether this query reads them or not, so one
        // mapping serves the pushed predicate, the projection, and the runtime's output tuple alike.
        ITupleFilterFactory tupleFilterFactory = null;
        int[] includeFields = NO_INCLUDE_FIELDS;
        if (unnestMap instanceof UnnestMapOperator) {
            UnnestMapOperator unnestMapOp = (UnnestMapOperator) unnestMap;
            VectorIncludeFilterPushdown.IncludeColumns columns =
                    VectorIncludeFilterPushdown.getIncludeColumns(unnestMapOp);

            if (unnestMapOp.getSelectCondition() != null) {
                if (columns == null) {
                    // A condition reaches the search only through the binding, which has nothing to bind a
                    // field access to without INCLUDE columns. One here without them was installed by some
                    // other hand, and dropping it would return the rows it was meant to remove.
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, unnestMap.getSourceLocation(),
                            "the vector index search of index " + jobGenParams.getIndexName()
                                    + " carries a filter condition but the index declares no INCLUDE columns");
                }
                // The operator schema carries the INCLUDE columns at their output positions, but the filter
                // runs against the SECONDARY tuple, so its variables resolve through the mapping instead.
                IOperatorSchema filterSchema =
                        new VectorIndexFilterSchema(opSchema, columns.varToFieldIndex(), numSecondaryKeys);
                IVariableTypeEnvironment filterTypeEnv =
                        new VectorIndexFilterTypeEnvironment(typeEnv, columns.varTypes(), context);
                tupleFilterFactory = mp.createTupleFilterFactory(new IOperatorSchema[] { filterSchema }, filterTypeEnv,
                        unnestMapOp.getSelectCondition().getValue(), context);
            }

            if (columns != null) {
                includeFields = includeFieldIndexes(unnestMapOp, columns.varToFieldIndex());
            }
        }

        // jobGenParams.isIndexOnly() (set by IntroduceTopKAccessMethodRule when the projection above
        // LIMIT references only PK columns) tells the runtime to emit [pk..., D(q,x)] per candidate so
        // the downstream sort can rank without the primary BTree lookup.
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> vectorSearch =
                mp.getVectorSearchRuntime(builder.getJobSpec(), outputVars, opSchema, typeEnv, context,
                        jobGenParams.getRetainInput(), dataset, jobGenParams.getIndexName(), queryIndexes,
                        tupleFilterFactory, includeFields, jobGenParams.isIndexOnly());

        IOperatorDescriptor opDesc = vectorSearch.first;
        opDesc.setSourceLocation(unnestMap.getSourceLocation());

        builder.contributeHyracksOperator(unnestMap, opDesc);
        builder.contributeAlgebricksPartitionConstraint(opDesc, vectorSearch.second);

        ILogicalOperator srcExchange = unnestMap.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMap, 0);
    }

    /**
     * The physical field indexes of the INCLUDE columns this search emits, in output order.
     * <p>
     * Those columns are the LAST variables of the unnest-map:
     * {@link VectorIncludeFilterPushdown#declareIncludeColumns} appends them after the primary keys and, on
     * the index-only plan, after the distance. The operator schema, and hence the output record descriptor,
     * is built from that same list, and the runtime writes the primary keys, then the distance, then these
     * columns -- so their
     * being trailing and in this order is what makes the two agree. Checked rather than assumed: a tuple
     * narrower than the descriptor is emitted with the previous tuple's offsets in its trailing slots, not
     * with an error.
     */
    private static int[] includeFieldIndexes(UnnestMapOperator unnestMap,
            Map<LogicalVariable, Integer> filterVarToFieldIndex) throws CompilationException {
        if (filterVarToFieldIndex == null || filterVarToFieldIndex.isEmpty()) {
            return NO_INCLUDE_FIELDS;
        }
        List<LogicalVariable> vars = unnestMap.getVariables();
        int numFilterVars = filterVarToFieldIndex.size();
        int firstFilterVar = vars.size() - numFilterVars;
        int[] fields = new int[numFilterVars];
        for (int i = 0; i < numFilterVars; i++) {
            Integer field = firstFilterVar >= 0 ? filterVarToFieldIndex.get(vars.get(firstFilterVar + i)) : null;
            if (field == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, unnestMap.getSourceLocation(),
                        "the vector index search does not declare its " + numFilterVars
                                + " INCLUDE filter columns as its last output variables: " + vars);
            }
            fields[i] = field;
        }
        return fields;
    }
}
