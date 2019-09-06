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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.adapter.factory.GenericAdapterFactory;
import org.apache.asterix.metadata.api.IDatasourceFunction;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;

public abstract class FunctionDataSource extends DataSource {

    public FunctionDataSource(DataSourceId id, INodeDomain domain) throws AlgebricksException {
        super(id, RecordUtil.FULLY_OPEN_RECORD_TYPE, null, DataSource.Type.FUNCTION, domain);
        schemaTypes = new IAType[] { itemType };
    }

    @Override
    public boolean isScanAccessPathALeaf() {
        return true;
    }

    @Override
    public IDataSourcePropertiesProvider getPropertiesProvider() {
        // Unordered Random partitioning on all nodes
        return scanVariables -> new StructuralPropertiesVector(new RandomPartitioningProperty(domain),
                Collections.emptyList());
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDatasourceScanRuntime(
            MetadataProvider metadataProvider, IDataSource<DataSourceId> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig)
            throws AlgebricksException {
        if (tupleFilterFactory != null || outputLimit >= 0) {
            throw CompilationException.create(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "tuple filter and limit are not supported by FunctionDataSource");
        }
        GenericAdapterFactory adapterFactory = new GenericAdapterFactory();
        adapterFactory.setOutputType(RecordUtil.FULLY_OPEN_RECORD_TYPE);
        IClusterStateManager csm = metadataProvider.getApplicationContext().getClusterStateManager();
        FunctionDataSourceFactory factory =
                new FunctionDataSourceFactory(createFunction(metadataProvider, getLocations(csm)));
        adapterFactory.configure(factory);
        return metadataProvider.buildExternalDatasetDataScannerRuntime(jobSpec, itemType, adapterFactory);
    }

    protected abstract IDatasourceFunction createFunction(MetadataProvider metadataProvider,
            AlgebricksAbsolutePartitionConstraint locations);

    protected AlgebricksAbsolutePartitionConstraint getLocations(IClusterStateManager csm) {
        String[] allPartitions = csm.getClusterLocations().getLocations();
        Set<String> ncs = new HashSet<>(Arrays.asList(allPartitions));
        return new AlgebricksAbsolutePartitionConstraint(ncs.toArray(new String[ncs.size()]));
    }

    protected static DataSourceId createDataSourceId(FunctionIdentifier fid, String... parameters) {
        int paramCount = parameters != null ? parameters.length : 0;
        String[] components = new String[paramCount + 2];
        components[0] = fid.getNamespace();
        components[1] = fid.getName();
        if (paramCount > 0) {
            System.arraycopy(parameters, 0, components, 2, paramCount);
        }
        return new DataSourceId(components);
    }
}
