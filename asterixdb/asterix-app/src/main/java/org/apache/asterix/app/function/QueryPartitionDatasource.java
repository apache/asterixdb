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
package org.apache.asterix.app.function;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.metadata.api.IDatasourceFunction;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;

public class QueryPartitionDatasource extends FunctionDataSource {

    private final Dataset ds;
    private final AlgebricksAbsolutePartitionConstraint storageLocations;
    private final int partitionNum;

    public QueryPartitionDatasource(Dataset ds, INodeDomain domain,
            AlgebricksAbsolutePartitionConstraint storageLocations, ARecordType recType, int partitionNum)
            throws AlgebricksException {
        super(createQueryPartitionDataSourceId(ds), QueryPartitionRewriter.QUERY_PARTITION, domain, recType);
        if (partitionNum < 0) {
            throw new IllegalArgumentException("partition must be >= 0");
        }
        this.partitionNum = partitionNum;
        this.ds = ds;
        this.storageLocations = storageLocations;
    }

    public Dataset getDatasource() {
        return ds;
    }

    public int getPartitionNumber() {
        return partitionNum;
    }

    @Override
    protected void initSchemaType(IAType iType) {
        ARecordType type = (ARecordType) iType;
        IAType[] fieldTypes = type.getFieldTypes();
        schemaTypes = new IAType[fieldTypes.length];
        System.arraycopy(fieldTypes, 0, schemaTypes, 0, schemaTypes.length);
    }

    @Override
    protected AlgebricksAbsolutePartitionConstraint getLocations(IClusterStateManager csm, MetadataProvider md) {
        return storageLocations;
    }

    @Override
    public boolean isScanAccessPathALeaf() {
        // the index scan op is not a leaf op. the ETS op will start the scan of the index. we need the ETS op below
        // the index scan to be still generated
        return false;
    }

    @Override
    protected IDatasourceFunction createFunction(MetadataProvider metadataProvider,
            AlgebricksAbsolutePartitionConstraint locations) {
        throw new UnsupportedOperationException("query-partition() does not use record reader adapter");
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDatasourceScanRuntime(
            MetadataProvider metadataProvider, IDataSource<DataSourceId> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig,
            IProjectionFiltrationInfo projectionInfo) throws AlgebricksException {
        return metadataProvider.getBtreePartitionSearchRuntime(jobSpec, opSchema, typeEnv, context, ds,
                tupleFilterFactory, outputLimit, partitionNum);
    }

    @Override
    public IDataSourcePropertiesProvider getPropertiesProvider() {
        return new IDataSourcePropertiesProvider() {
            @Override
            public IPhysicalPropertiesVector computeRequiredProperties(List<LogicalVariable> scanVariables,
                    IOptimizationContext ctx) {
                return StructuralPropertiesVector.EMPTY_PROPERTIES_VECTOR;
            }

            @Override
            public IPhysicalPropertiesVector computeDeliveredProperties(List<LogicalVariable> scanVariables,
                    IOptimizationContext ctx) {
                List<ILocalStructuralProperty> propsLocal = new ArrayList<>(1);
                return new StructuralPropertiesVector(new RandomPartitioningProperty(domain), propsLocal);
            }
        };
    }

    private static DataSourceId createQueryPartitionDataSourceId(Dataset dataset) {
        return new DataSourceId(dataset.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName(),
                new String[] { dataset.getDatasetName(), QueryPartitionRewriter.QUERY_PARTITION.getName() });
    }

    @Override
    public boolean sameFunctionDatasource(FunctionDataSource other) {
        if (!Objects.equals(this.functionId, other.getFunctionId())) {
            return false;
        }
        QueryPartitionDatasource that = (QueryPartitionDatasource) other;
        return Objects.equals(this.ds, that.getDatasource())
                && Objects.equals(this.partitionNum, that.getPartitionNumber());
    }
}
