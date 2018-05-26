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

import java.util.List;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;

@SuppressWarnings("rawtypes")
public class DataSourceScanPOperator extends AbstractScanPOperator {

    private final IDataSource<?> dataSource;
    private Object implConfig;

    public DataSourceScanPOperator(IDataSource<?> dataSource) {
        this.dataSource = dataSource;
    }

    public void setImplConfig(Object implConfig) {
        this.implConfig = implConfig;
    }

    public Object getImplConfig() {
        return implConfig;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.DATASOURCE_SCAN;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        // partitioning properties
        DataSourceScanOperator dssOp = (DataSourceScanOperator) op;
        IDataSourcePropertiesProvider dspp = dataSource.getPropertiesProvider();
        deliveredProperties = dspp.computePropertiesVector(dssOp.getVariables());
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        if (op.getInputs().isEmpty()) {
            return emptyUnaryRequirements();
        }
        ILogicalOperator childOp = op.getInputs().get(0).getValue();
        // Empty tuple source is a special case that can be partitioned in the same way as the data scan.
        if (childOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
            return emptyUnaryRequirements();
        }
        INodeDomain domain = dataSource.getDomain();
        return new PhysicalRequirements(
                new StructuralPropertiesVector[] {
                        new StructuralPropertiesVector(new BroadcastPartitioningProperty(domain), null) },
                IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        DataSourceScanOperator scan = (DataSourceScanOperator) op;
        IMetadataProvider mp = context.getMetadataProvider();
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(op);
        List<LogicalVariable> vars = scan.getVariables();
        List<LogicalVariable> projectVars = scan.getProjectVariables();

        ITupleFilterFactory tupleFilterFactory = null;
        if (scan.getSelectCondition() != null) {
            tupleFilterFactory = context.getMetadataProvider().createTupleFilterFactory(
                    new IOperatorSchema[] { opSchema }, typeEnv, scan.getSelectCondition().getValue(), context);
        }

        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = mp.getScannerRuntime(dataSource, vars, projectVars,
                scan.isProjectPushed(), scan.getMinFilterVars(), scan.getMaxFilterVars(), tupleFilterFactory,
                scan.getOutputLimit(), opSchema, typeEnv, context, builder.getJobSpec(), implConfig);
        IOperatorDescriptor opDesc = p.first;
        opDesc.setSourceLocation(scan.getSourceLocation());
        builder.contributeHyracksOperator(scan, opDesc);
        if (p.second != null) {
            builder.contributeAlgebricksPartitionConstraint(opDesc, p.second);
        }

        ILogicalOperator srcExchange = scan.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, scan, 0);
    }
}
