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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.DataSourceIndex;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractScanPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Class that embodies the commonalities between access method physical operators.
 */
public abstract class IndexSearchPOperator extends AbstractScanPOperator {

    protected final IDataSourceIndex<String, DataSourceId> idx;
    protected final boolean requiresBroadcast;
    protected final INodeDomain domain;

    public IndexSearchPOperator(IDataSourceIndex<String, DataSourceId> idx, INodeDomain domain,
            boolean requiresBroadcast) {
        this.idx = idx;
        this.requiresBroadcast = requiresBroadcast;
        this.domain = domain;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

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
                    // Getting the primary keys vars from the Unnest Map or Left Outer Unnest Map operator.
                    // Primary key Vars are always located at the end of the unnest variables list.
                    // This check is unnecessary as the operator will always be
                    // AbstractUnnestMapOperator in secondary index search cases.
                    List<LogicalVariable> opVars = ((AbstractUnnestMapOperator) op).getScanVariables();
                    int varsSize = opVars.size();
                    scanVariables.addAll(opVars.subList(varsSize - numOfPrimaryKeys, varsSize));
                    scanVariables.add(new LogicalVariable(-1));
                    if (dataset.hasMetaPart()) {
                        //if the dataset has a meta part, we add an additional variable for the meta item
                        //By doing so we ensure that DataSource.getPrimaryKeyVariables will always pick
                        //the correct pks.
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

    protected int[] getKeyIndexes(List<LogicalVariable> keyVarList, IOperatorSchema[] inputSchemas) {
        if (keyVarList == null) {
            return null;
        }
        int[] keyIndexes = new int[keyVarList.size()];
        for (int i = 0; i < keyVarList.size(); i++) {
            keyIndexes[i] = inputSchemas[0].findVariable(keyVarList.get(i));
        }
        return keyIndexes;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) throws AlgebricksException {
        if (requiresBroadcast) {
            StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
            pv[0] = new StructuralPropertiesVector(new BroadcastPartitioningProperty(domain), null);
            return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
        } else {
            return super.getRequiredPropertiesForChildren(op, reqdByParent, context);
        }
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }

    @Override
    public String toString(boolean verbose) {
        String ss = super.toString();
        if (verbose) {
            ss += " (" + idx.getDataSource().getId() + '.' + idx.getId() + ')';
        }
        return ss;
    }

    protected static IMissingWriterFactory getNonMatchWriterFactory(IAlgebricksConstantValue missingValue,
            JobGenContext context, SourceLocation sourceLoc) throws CompilationException {
        IMissingWriterFactory nonMatchWriterFactory;
        if (missingValue.isMissing()) {
            nonMatchWriterFactory = context.getMissingWriterFactory();
        } else if (missingValue.isNull()) {
            nonMatchWriterFactory = context.getNullWriterFactory();
        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc, missingValue.toString());
        }
        return nonMatchWriterFactory;
    }

    protected static IMissingWriterFactory getNonFilterWriterFactory(boolean propagateFilter, JobGenContext context) {
        return propagateFilter ? context.getMissingWriterFactory() : null;
    }
}
