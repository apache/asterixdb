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
import java.util.List;

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
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;

public class IndexInsertDeleteUpsertPOperator extends AbstractPhysicalOperator {

    private final List<LogicalVariable> primaryKeys;
    private final List<LogicalVariable> secondaryKeys;
    private final ILogicalExpression filterExpr;
    private final IDataSourceIndex<?, ?> dataSourceIndex;
    private final List<LogicalVariable> additionalFilteringKeys;
    private final LogicalVariable upsertIndicatorVar;
    private final List<LogicalVariable> prevSecondaryKeys;
    private final LogicalVariable prevAdditionalFilteringKey;
    private final int numOfAdditionalNonFilteringFields;

    public IndexInsertDeleteUpsertPOperator(List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalFilteringKeys, Mutable<ILogicalExpression> filterExpr,
            IDataSourceIndex<?, ?> dataSourceIndex, LogicalVariable upsertIndicatorVar,
            List<LogicalVariable> prevSecondaryKeys, LogicalVariable prevAdditionalFilteringKey,
            int numOfAdditionalNonFilteringFields) {
        this.primaryKeys = primaryKeys;
        this.secondaryKeys = secondaryKeys;
        if (filterExpr != null) {
            this.filterExpr = filterExpr.getValue();
        } else {
            this.filterExpr = null;
        }
        this.dataSourceIndex = dataSourceIndex;
        this.additionalFilteringKeys = additionalFilteringKeys;
        this.upsertIndicatorVar = upsertIndicatorVar;
        this.prevSecondaryKeys = prevSecondaryKeys;
        this.prevAdditionalFilteringKey = prevAdditionalFilteringKey;
        this.numOfAdditionalNonFilteringFields = numOfAdditionalNonFilteringFields;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.INDEX_INSERT_DELETE;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        List<LogicalVariable> scanVariables = new ArrayList<LogicalVariable>();
        scanVariables.addAll(primaryKeys);
        scanVariables.add(new LogicalVariable(-1));
        for (int i = 0; i < numOfAdditionalNonFilteringFields; i++) {
            scanVariables.add(new LogicalVariable(-1));
        }
        IPhysicalPropertiesVector r =
                dataSourceIndex.getDataSource().getPropertiesProvider().computePropertiesVector(scanVariables);
        r.getLocalProperties().clear();
        IPhysicalPropertiesVector[] requirements = new IPhysicalPropertiesVector[1];
        requirements[0] = r;
        return new PhysicalRequirements(requirements, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        IndexInsertDeleteUpsertOperator insertDeleteUpsertOp = (IndexInsertDeleteUpsertOperator) op;
        IMetadataProvider mp = context.getMetadataProvider();

        JobSpecification spec = builder.getJobSpec();
        RecordDescriptor inputDesc = JobGenHelper.mkRecordDescriptor(
                context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas[0], context);

        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> runtimeAndConstraints = null;
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(insertDeleteUpsertOp);
        Kind operation = insertDeleteUpsertOp.getOperation();
        switch (operation) {
            case INSERT:
                runtimeAndConstraints =
                        mp.getIndexInsertRuntime(dataSourceIndex, propagatedSchema, inputSchemas, typeEnv, primaryKeys,
                                secondaryKeys, additionalFilteringKeys, filterExpr, inputDesc, context, spec, false);
                break;
            case DELETE:
                runtimeAndConstraints =
                        mp.getIndexDeleteRuntime(dataSourceIndex, propagatedSchema, inputSchemas, typeEnv, primaryKeys,
                                secondaryKeys, additionalFilteringKeys, filterExpr, inputDesc, context, spec);
                break;
            case UPSERT:
                runtimeAndConstraints = mp.getIndexUpsertRuntime(dataSourceIndex, propagatedSchema, inputSchemas,
                        typeEnv, primaryKeys, secondaryKeys, additionalFilteringKeys, filterExpr, upsertIndicatorVar,
                        prevSecondaryKeys, prevAdditionalFilteringKey, inputDesc, context, spec);
                break;
            default:
                throw new AlgebricksException("Unsupported Operation " + operation);
        }
        IOperatorDescriptor opDesc = runtimeAndConstraints.first;
        opDesc.setSourceLocation(insertDeleteUpsertOp.getSourceLocation());
        builder.contributeHyracksOperator(insertDeleteUpsertOp, opDesc);
        builder.contributeAlgebricksPartitionConstraint(opDesc, runtimeAndConstraints.second);
        ILogicalOperator src = insertDeleteUpsertOp.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, insertDeleteUpsertOp, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }

    public List<LogicalVariable> getPrevSecondaryKeys() {
        return prevSecondaryKeys;
    }

    public LogicalVariable getPrevFilteringKeys() {
        return prevAdditionalFilteringKey;
    }
}
