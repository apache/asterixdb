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
package org.apache.asterix.metadata.declared;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.external.adapter.factory.GenericAdapterFactory;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.parser.factory.ADMDataParserFactory;
import org.apache.asterix.metadata.api.IDatasourceFunction;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;

public abstract class FunctionDataSource extends DataSource {

    protected final FunctionIdentifier functionId;

    public FunctionDataSource(DataSourceId id, FunctionIdentifier functionId, INodeDomain domain, IAType itemType)
            throws AlgebricksException {
        super(id, itemType, null, DataSource.Type.FUNCTION, domain);
        this.functionId = functionId;
        initSchemaType(itemType);
    }

    public FunctionDataSource(DataSourceId id, FunctionIdentifier functionId, INodeDomain domain)
            throws AlgebricksException {
        this(id, functionId, domain, RecordUtil.FULLY_OPEN_RECORD_TYPE);
    }

    protected void initSchemaType(IAType itemType) {
        schemaTypes = new IAType[] { itemType };
    }

    public FunctionIdentifier getFunctionId() {
        return functionId;
    }

    @Override
    public boolean isScanAccessPathALeaf() {
        return true;
    }

    @Override
    public IDataSourcePropertiesProvider getPropertiesProvider() {
        // Unordered Random partitioning on all nodes
        return new IDataSourcePropertiesProvider() {
            @Override
            public IPhysicalPropertiesVector computeRequiredProperties(List<LogicalVariable> scanVariables,
                    IOptimizationContext ctx) {
                return StructuralPropertiesVector.EMPTY_PROPERTIES_VECTOR;
            }

            @Override
            public IPhysicalPropertiesVector computeDeliveredProperties(List<LogicalVariable> scanVariables,
                    IOptimizationContext ctx) {
                return new StructuralPropertiesVector(new RandomPartitioningProperty(domain), Collections.emptyList());
            }
        };
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDatasourceScanRuntime(
            MetadataProvider metadataProvider, IDataSource<DataSourceId> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig,
            IProjectionFiltrationInfo projectionFiltrationInfo) throws AlgebricksException {
        GenericAdapterFactory adapterFactory = new GenericAdapterFactory();
        adapterFactory.setOutputType(RecordUtil.FULLY_OPEN_RECORD_TYPE);
        IClusterStateManager csm = metadataProvider.getApplicationContext().getClusterStateManager();
        FunctionDataSourceFactory factory =
                new FunctionDataSourceFactory(createFunction(metadataProvider, getLocations(csm)));
        IDataParserFactory dataParserFactory = createDataParserFactory();
        dataParserFactory.setRecordType(RecordUtil.FULLY_OPEN_RECORD_TYPE);
        dataParserFactory.configure(Collections.emptyMap());
        adapterFactory.configure(factory, dataParserFactory);
        return metadataProvider.getExternalDatasetScanRuntime(jobSpec, itemType, adapterFactory, tupleFilterFactory,
                outputLimit);
    }

    public boolean skipJobCapacityAssignment() {
        return false;
    }

    protected abstract IDatasourceFunction createFunction(MetadataProvider metadataProvider,
            AlgebricksAbsolutePartitionConstraint locations);

    protected AlgebricksAbsolutePartitionConstraint getLocations(IClusterStateManager csm) {
        String[] sortedLocations = csm.getSortedClusterLocations().getLocations();
        return new AlgebricksAbsolutePartitionConstraint(
                Arrays.stream(sortedLocations).distinct().toArray(String[]::new));
    }

    protected IDataParserFactory createDataParserFactory() {
        return new ADMDataParserFactory();
    }

    protected static DataSourceId createDataSourceId(FunctionIdentifier fid, String... parameters) {
        return new DataSourceId(fid.getDatabase(), FunctionSignature.getDataverseName(fid), fid.getName(), parameters);
    }
}
