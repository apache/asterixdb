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

package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator.Kind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;

public class IndexBulkloadPOperator extends AbstractPhysicalOperator {

    private final List<LogicalVariable> primaryKeys;
    private final List<LogicalVariable> secondaryKeys;
    private final List<LogicalVariable> additionalFilteringKeys;
    private final ILogicalExpression filterExpr;
    private final IDataSourceIndex<?, ?> dataSourceIndex;

    public IndexBulkloadPOperator(List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalFilteringKeys, Mutable<ILogicalExpression> filterExpr,
            IDataSourceIndex<?, ?> dataSourceIndex) {
        this.primaryKeys = primaryKeys;
        this.secondaryKeys = secondaryKeys;
        this.additionalFilteringKeys = additionalFilteringKeys;
        if (filterExpr != null) {
            this.filterExpr = filterExpr.getValue();
        } else {
            this.filterExpr = null;
        }
        this.dataSourceIndex = dataSourceIndex;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.INDEX_BULKLOAD;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        //skVarMap is used to remove duplicated variable references for order operator
        Map<Integer, Object> skVarMap = new HashMap<Integer, Object>();
        List<LogicalVariable> scanVariables = new ArrayList<>();
        scanVariables.addAll(primaryKeys);
        scanVariables.add(new LogicalVariable(-1));
        IPhysicalPropertiesVector physicalProps =
                dataSourceIndex.getDataSource().getPropertiesProvider().computePropertiesVector(scanVariables);
        List<ILocalStructuralProperty> localProperties = new ArrayList<>();
        List<OrderColumn> orderColumns = new ArrayList<OrderColumn>();
        // Data needs to be sorted based on the [token, number of token, PK]
        // OR [token, PK] if the index is not partitioned
        for (LogicalVariable skVar : secondaryKeys) {
            if (!skVarMap.containsKey(skVar.getId())) {
                orderColumns.add(new OrderColumn(skVar, OrderKind.ASC));
                skVarMap.put(skVar.getId(), null);
            }
        }
        for (LogicalVariable pkVar : primaryKeys) {
            orderColumns.add(new OrderColumn(pkVar, OrderKind.ASC));
        }
        localProperties.add(new LocalOrderProperty(orderColumns));
        StructuralPropertiesVector spv =
                new StructuralPropertiesVector(physicalProps.getPartitioningProperty(), localProperties);
        return new PhysicalRequirements(new IPhysicalPropertiesVector[] { spv },
                IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        IndexInsertDeleteUpsertOperator indexInsertDeleteOp = (IndexInsertDeleteUpsertOperator) op;
        assert indexInsertDeleteOp.getOperation() == Kind.INSERT;
        assert indexInsertDeleteOp.isBulkload();

        IMetadataProvider mp = context.getMetadataProvider();
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(op);
        JobSpecification spec = builder.getJobSpec();
        RecordDescriptor inputDesc = JobGenHelper.mkRecordDescriptor(
                context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas[0], context);
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> runtimeAndConstraints =
                mp.getIndexInsertRuntime(dataSourceIndex, propagatedSchema, inputSchemas, typeEnv, primaryKeys,
                        secondaryKeys, additionalFilteringKeys, filterExpr, inputDesc, context, spec, true);
        IOperatorDescriptor opDesc = runtimeAndConstraints.first;
        opDesc.setSourceLocation(indexInsertDeleteOp.getSourceLocation());
        builder.contributeHyracksOperator(indexInsertDeleteOp, opDesc);
        builder.contributeAlgebricksPartitionConstraint(opDesc, runtimeAndConstraints.second);
        ILogicalOperator src = indexInsertDeleteOp.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, indexInsertDeleteOp, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }

}
