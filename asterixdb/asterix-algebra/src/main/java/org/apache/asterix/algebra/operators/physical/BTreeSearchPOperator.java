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

import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.DataSourceIndex;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.am.BTreeJobGenParams;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
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
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;

/**
 * Contributes the runtime operator for an unnest-map representing a BTree search.
 */
public class BTreeSearchPOperator extends IndexSearchPOperator {

    private final List<LogicalVariable> lowKeyVarList;
    private final List<LogicalVariable> highKeyVarList;
    private final boolean isPrimaryIndex;
    private final boolean isEqCondition;
    private Object implConfig;

    public BTreeSearchPOperator(IDataSourceIndex<String, DataSourceId> idx, INodeDomain domain,
            boolean requiresBroadcast, boolean isPrimaryIndex, boolean isEqCondition,
            List<LogicalVariable> lowKeyVarList, List<LogicalVariable> highKeyVarList) {
        super(idx, domain, requiresBroadcast);
        this.isPrimaryIndex = isPrimaryIndex;
        this.isEqCondition = isEqCondition;
        this.lowKeyVarList = lowKeyVarList;
        this.highKeyVarList = highKeyVarList;
    }

    public void setImplConfig(Object implConfig) {
        this.implConfig = implConfig;
    }

    public Object getImplConfig() {
        return implConfig;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.BTREE_SEARCH;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        AbstractUnnestMapOperator unnestMap = (AbstractUnnestMapOperator) op;
        ILogicalExpression unnestExpr = unnestMap.getExpressionRef().getValue();
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException();
        }
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
        FunctionIdentifier funcIdent = unnestFuncExpr.getFunctionIdentifier();
        if (!funcIdent.equals(BuiltinFunctions.INDEX_SEARCH)) {
            return;
        }
        BTreeJobGenParams jobGenParams = new BTreeJobGenParams();
        jobGenParams.readFromFuncArgs(unnestFuncExpr.getArguments());
        int[] lowKeyIndexes = getKeyIndexes(jobGenParams.getLowKeyVarList(), inputSchemas);
        int[] highKeyIndexes = getKeyIndexes(jobGenParams.getHighKeyVarList(), inputSchemas);

        int[] minFilterFieldIndexes = getKeyIndexes(unnestMap.getMinFilterVars(), inputSchemas);
        int[] maxFilterFieldIndexes = getKeyIndexes(unnestMap.getMaxFilterVars(), inputSchemas);
        boolean propagateFilter = unnestMap.propagateIndexFilter();

        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        Dataset dataset = metadataProvider.findDataset(jobGenParams.getDataverseName(), jobGenParams.getDatasetName());
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(op);
        ITupleFilterFactory tupleFilterFactory = null;
        long outputLimit = -1;
        if (unnestMap.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            UnnestMapOperator unnestMapOp = (UnnestMapOperator) unnestMap;
            outputLimit = unnestMapOp.getOutputLimit();
            if (unnestMapOp.getSelectCondition() != null) {
                tupleFilterFactory = metadataProvider.createTupleFilterFactory(new IOperatorSchema[] { opSchema },
                        typeEnv, unnestMapOp.getSelectCondition().getValue(), context);
            }
        }
        // By nature, LEFT_OUTER_UNNEST_MAP should generate null values for non-matching tuples.
        boolean retainMissing = op.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP;
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> btreeSearch = metadataProvider.buildBtreeRuntime(
                builder.getJobSpec(), opSchema, typeEnv, context, jobGenParams.getRetainInput(), retainMissing, dataset,
                jobGenParams.getIndexName(), lowKeyIndexes, highKeyIndexes, jobGenParams.isLowKeyInclusive(),
                jobGenParams.isHighKeyInclusive(), propagateFilter, minFilterFieldIndexes, maxFilterFieldIndexes,
                tupleFilterFactory, outputLimit, unnestMap.getGenerateCallBackProceedResultVar());
        IOperatorDescriptor opDesc = btreeSearch.first;
        opDesc.setSourceLocation(unnestMap.getSourceLocation());

        builder.contributeHyracksOperator(unnestMap, opDesc);
        builder.contributeAlgebricksPartitionConstraint(opDesc, btreeSearch.second);

        ILogicalOperator srcExchange = unnestMap.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMap, 0);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        if (requiresBroadcast) {
            // For primary indexes optimizing an equality condition we can reduce the broadcast requirement to hash partitioning.
            if (isPrimaryIndex && isEqCondition) {

                // If this is a composite primary index, then all of the keys should be provided.
                Index searchIndex = ((DataSourceIndex) idx).getIndex();
                int numberOfKeyFields = searchIndex.getKeyFieldNames().size();

                if (numberOfKeyFields < 2
                        || (lowKeyVarList.size() == numberOfKeyFields && highKeyVarList.size() == numberOfKeyFields)) {
                    StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
                    ListSet<LogicalVariable> searchKeyVars = new ListSet<>();
                    searchKeyVars.addAll(lowKeyVarList);
                    searchKeyVars.addAll(highKeyVarList);
                    // Also, add a local sorting property to enforce a sort before the primary-index operator.
                    List<ILocalStructuralProperty> propsLocal = new ArrayList<>();
                    List<OrderColumn> orderColumns = new ArrayList<>();
                    for (LogicalVariable orderVar : searchKeyVars) {
                        orderColumns.add(new OrderColumn(orderVar, OrderKind.ASC));
                    }
                    propsLocal.add(new LocalOrderProperty(orderColumns));
                    pv[0] = new StructuralPropertiesVector(new UnorderedPartitionedProperty(searchKeyVars, domain),
                            propsLocal);
                    return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
                }
            }
            StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
            pv[0] = new StructuralPropertiesVector(new BroadcastPartitioningProperty(domain), null);
            return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
        } else {
            return super.getRequiredPropertiesForChildren(op, reqdByParent, context);
        }
    }
}
