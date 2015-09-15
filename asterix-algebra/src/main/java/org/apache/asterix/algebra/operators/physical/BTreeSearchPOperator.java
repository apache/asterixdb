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

import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.AqlSourceId;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.optimizer.rules.am.BTreeJobGenParams;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;

/**
 * Contributes the runtime operator for an unnest-map representing a BTree search.
 */
public class BTreeSearchPOperator extends IndexSearchPOperator {

    private final List<LogicalVariable> lowKeyVarList;
    private final List<LogicalVariable> highKeyVarList;
    private final boolean isPrimaryIndex;
    private final boolean isEqCondition;
    private Object implConfig;

    public BTreeSearchPOperator(IDataSourceIndex<String, AqlSourceId> idx, boolean requiresBroadcast,
            boolean isPrimaryIndex, boolean isEqCondition, List<LogicalVariable> lowKeyVarList,
            List<LogicalVariable> highKeyVarList) {
        super(idx, requiresBroadcast);
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
        UnnestMapOperator unnestMap = (UnnestMapOperator) op;
        ILogicalExpression unnestExpr = unnestMap.getExpressionRef().getValue();
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException();
        }
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
        FunctionIdentifier funcIdent = unnestFuncExpr.getFunctionIdentifier();
        if (!funcIdent.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
            return;
        }
        BTreeJobGenParams jobGenParams = new BTreeJobGenParams();
        jobGenParams.readFromFuncArgs(unnestFuncExpr.getArguments());
        int[] lowKeyIndexes = getKeyIndexes(jobGenParams.getLowKeyVarList(), inputSchemas);
        int[] highKeyIndexes = getKeyIndexes(jobGenParams.getHighKeyVarList(), inputSchemas);

        int[] minFilterFieldIndexes = getKeyIndexes(unnestMap.getMinFilterVars(), inputSchemas);
        int[] maxFilterFieldIndexes = getKeyIndexes(unnestMap.getMaxFilterVars(), inputSchemas);

        AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
        Dataset dataset = metadataProvider.findDataset(jobGenParams.getDataverseName(), jobGenParams.getDatasetName());
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(op);
        List<LogicalVariable> outputVars = unnestMap.getVariables();
        if (jobGenParams.getRetainInput()) {
            outputVars = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(unnestMap, outputVars);
        }
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> btreeSearch = metadataProvider.buildBtreeRuntime(
                builder.getJobSpec(), outputVars, opSchema, typeEnv, context, jobGenParams.getRetainInput(),
                jobGenParams.getRetainNull(), dataset, jobGenParams.getIndexName(), lowKeyIndexes, highKeyIndexes,
                jobGenParams.isLowKeyInclusive(), jobGenParams.isHighKeyInclusive(), implConfig, minFilterFieldIndexes,
                maxFilterFieldIndexes);

        builder.contributeHyracksOperator(unnestMap, btreeSearch.first);
        builder.contributeAlgebricksPartitionConstraint(btreeSearch.first, btreeSearch.second);

        ILogicalOperator srcExchange = unnestMap.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMap, 0);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        if (requiresBroadcast) {
            // For primary indexes optimizing an equality condition we can reduce the broadcast requirement to hash partitioning.
            if (isPrimaryIndex && isEqCondition) {
                StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
                ListSet<LogicalVariable> searchKeyVars = new ListSet<LogicalVariable>();
                searchKeyVars.addAll(lowKeyVarList);
                searchKeyVars.addAll(highKeyVarList);
                // Also, add a local sorting property to enforce a sort before the primary-index operator.
                List<ILocalStructuralProperty> propsLocal = new ArrayList<ILocalStructuralProperty>();
                List<OrderColumn> orderColumns = new ArrayList<OrderColumn>();
                for (LogicalVariable orderVar : searchKeyVars) {
                    orderColumns.add(new OrderColumn(orderVar, OrderKind.ASC));
                }
                propsLocal.add(new LocalOrderProperty(orderColumns));
                pv[0] = new StructuralPropertiesVector(new UnorderedPartitionedProperty(searchKeyVars, null),
                        propsLocal);
                return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
            } else {
                StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
                pv[0] = new StructuralPropertiesVector(new BroadcastPartitioningProperty(null), null);
                return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
            }
        } else {
            return super.getRequiredPropertiesForChildren(op, reqdByParent);
        }
    }
}