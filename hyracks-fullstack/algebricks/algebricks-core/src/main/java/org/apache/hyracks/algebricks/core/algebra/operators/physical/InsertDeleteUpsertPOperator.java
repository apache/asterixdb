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

import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator.Kind;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;

@SuppressWarnings("rawtypes")
public class InsertDeleteUpsertPOperator extends AbstractPhysicalOperator {

    private final LogicalVariable payload;
    private final List<LogicalVariable> keys;
    private final IDataSource<?> dataSource;
    private final List<LogicalVariable> additionalFilteringKeys;
    private final List<LogicalVariable> additionalNonFilteringFields;
    private final Kind operation;

    public InsertDeleteUpsertPOperator(LogicalVariable payload, List<LogicalVariable> keys,
            List<LogicalVariable> additionalFilteringKeys, IDataSource dataSource, Kind operation,
            List<LogicalVariable> additionalNonFilteringFields) {
        this.payload = payload;
        this.keys = keys;
        this.dataSource = dataSource;
        this.additionalFilteringKeys = additionalFilteringKeys;
        this.operation = operation;
        this.additionalNonFilteringFields = additionalNonFilteringFields;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.INSERT_DELETE;
    }

    // Delivered Properties of this will be (Sorted on PK, Partitioned on PK)
    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        List<LogicalVariable> scanVariables = new ArrayList<LogicalVariable>();
        scanVariables.addAll(keys);
        scanVariables.add(payload);
        if (additionalNonFilteringFields != null) {
            scanVariables.addAll(additionalNonFilteringFields);
        }
        IPhysicalPropertiesVector r = dataSource.getPropertiesProvider().computePropertiesVector(scanVariables);
        r.getLocalProperties().clear();
        IPhysicalPropertiesVector[] requirements = new IPhysicalPropertiesVector[1];
        requirements[0] = r;
        return new PhysicalRequirements(requirements, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        InsertDeleteUpsertOperator insertDeleteOp = (InsertDeleteUpsertOperator) op;
        IMetadataProvider mp = context.getMetadataProvider();
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(op);
        JobSpecification spec = builder.getJobSpec();
        RecordDescriptor inputDesc = JobGenHelper.mkRecordDescriptor(
                context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas[0], context);

        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> runtimeAndConstraints;
        switch (operation) {
            case INSERT:
                runtimeAndConstraints = mp.getInsertRuntime(dataSource, propagatedSchema, typeEnv, keys, payload,
                        additionalFilteringKeys, additionalNonFilteringFields, inputDesc, context, spec, false);
                break;
            case DELETE:
                runtimeAndConstraints = mp.getDeleteRuntime(dataSource, propagatedSchema, typeEnv, keys, payload,
                        additionalFilteringKeys, inputDesc, context, spec);
                break;
            case UPSERT:
                runtimeAndConstraints = mp.getUpsertRuntime(dataSource, inputSchemas[0], typeEnv, keys, payload,
                        additionalFilteringKeys, additionalNonFilteringFields, inputDesc, context, spec);
                break;
            default:
                throw new AlgebricksException("Unsupported Operation " + operation);
        }

        IOperatorDescriptor opDesc = runtimeAndConstraints.first;
        opDesc.setSourceLocation(insertDeleteOp.getSourceLocation());
        builder.contributeHyracksOperator(insertDeleteOp, opDesc);
        builder.contributeAlgebricksPartitionConstraint(opDesc, runtimeAndConstraints.second);
        ILogicalOperator src = insertDeleteOp.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, insertDeleteOp, 0);
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
