/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;

@SuppressWarnings("rawtypes")
public class DataSourceScanPOperator extends AbstractScanPOperator {

    private IDataSource<?> dataSource;
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
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = mp.getScannerRuntime(dataSource, vars,
                projectVars, scan.isProjectPushed(), opSchema, typeEnv, context, builder.getJobSpec(), implConfig);
        builder.contributeHyracksOperator(scan, p.first);
        if (p.second != null) {
            builder.contributeAlgebricksPartitionConstraint(p.first, p.second);
        }

        ILogicalOperator srcExchange = scan.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, scan, 0);
    }

}
